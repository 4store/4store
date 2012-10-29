/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__OpenBSD__)
#define _XOPEN_SOURCE 600
#endif
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "backend.h"
#include "rhash.h"
#include "list.h"
#include "prefix-trie.h"
#include "../common/4s-hash.h"
#include "../common/params.h"
#include "../common/error.h"

#define FS_RHASH_DEFAULT_LENGTH        65536
#define FS_RHASH_DEFAULT_SEARCH_DIST      32
#define FS_RHASH_DEFAULT_BUCKET_SIZE      16
#define FS_MAX_PREFIXES                  256

#define FS_RHASH_ID 0x4a585230

/* maximum distance that we wil allow the resource to be from its hash value */

#define FS_PACKED __attribute__((__packed__))

#define FS_RHASH_ENTRY(rh, rid) (((uint64_t)(rid >> 10) & ((uint64_t)(rh->size - 1)))*rh->bucket_size)

#define DISP_I_UTF8         'i'
#define DISP_I_NUMBER       'N'
#define DISP_I_DATE         'D'
#define DISP_I_PREFIX       'p'
#define DISP_F_UTF8         'f'
#define DISP_F_PREFIX       'P'
#define DISP_F_ZCOMP        'Z'

struct rhash_header {
    int32_t id;             // "JXR0"
    uint32_t size;          // size of hashtable in buckets,
                            //    must be power of two
    uint32_t count;         // number of resources in hashtable
    uint32_t search_dist;   // offset to scan to in table for match
    uint32_t bucket_size;   // number of entries per bucket
    uint32_t revision;      // revision of the strucure
                            // rev=1: 32 byte, packed entries
    char padding[488];      // allign to a block
} FS_PACKED;
 
#define INLINE_STR_LEN 15

typedef struct _fs_rhash_entry {
    fs_rid rid;
    union {
        fs_rid attr;    // attribute value, lang tag or datatype
        unsigned char pstr[8]; // prefix code + first 7 chars
    } FS_PACKED aval;
    union {
        int64_t offset; // offset in lex file
        char str[INLINE_STR_LEN];   // inline string data
    } FS_PACKED val;
    char disp;          // disposition of data - lex file or inline
} FS_PACKED fs_rhash_entry;

struct _fs_rhash {
    uint32_t size;
    uint32_t count;
    uint32_t search_dist;
    uint32_t bucket_size;
    uint32_t revision;
    int fd;
    fs_rhash_entry *entries;
    char *filename;
    FILE *lex_f;
    char *lex_filename;
    int flags;
    int locked;
    fs_prefix_trie *ptrie;
    fs_prefix_trie *prefixes;
    int prefix_count;
    char *prefix_strings[FS_MAX_PREFIXES];
    fs_list *prefix_file;
    char *z_buffer;
    int z_buffer_size;
};

/* this is much wider than it needs to be to match fs_list requirements */
struct prefix_file_line {
    uint32_t code;
    char     prefix[512-4];
};

static fs_rhash *global_sort_rh = NULL;

static int double_size(fs_rhash *rh);
int fs_rhash_write_header(fs_rhash *rh);

static int compress_bcd(const char *in, char *out);
static char *uncompress_bcd(unsigned char *bcd);

static int compress_bcdate(const char *in, char *out);
static char *uncompress_bcdate(unsigned char *bcd);

fs_rhash *fs_rhash_open(fs_backend *be, const char *label, int flags)
{
    char *filename = g_strdup_printf(FS_RHASH, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), label);
    fs_rhash *rh = fs_rhash_open_filename(filename, flags);
    g_free(filename);

    return rh;
}

void fs_rhash_ensure_size(fs_rhash *rh)
{
    /* skip if we're read-only */
    if (!(rh->flags & (O_WRONLY | O_RDWR))) return;

    const off_t len = sizeof(struct rhash_header) + ((off_t) rh->size) * ((off_t) rh->bucket_size) * sizeof(fs_rhash_entry);

    /* FIXME should use fallocate where it has decent performance,
       in order to avoid fragmentation */

    unsigned char byte = 0;
    /* write one past the end to avoid possibility of overwriting the last RID */
    if (pwrite(rh->fd, &byte, sizeof(byte), len) == -1) {
        fs_error(LOG_ERR, "couldn't pre-allocate for '%s': %s", rh->filename, strerror(errno));
    }
}

fs_rhash *fs_rhash_open_filename(const char *filename, int flags)
{
    struct rhash_header header;
    if (sizeof(struct rhash_header) != 512) {
        fs_error(LOG_CRIT, "incorrect rhash header size %zd, should be 512",
                 sizeof(header));

        return NULL;
    }
    if (sizeof(fs_rhash_entry) != 32) {
        fs_error(LOG_CRIT, "incorrect entry size %zd, should be 32",
                 sizeof(fs_rhash_entry));

        return NULL;
    }

    fs_rhash *rh = calloc(1, sizeof(fs_rhash));
    rh->fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    rh->flags = flags;
    if (rh->fd == -1) {
        fs_error(LOG_ERR, "cannot open rhash file '%s': %s", filename, strerror(errno));

        return NULL;
    }
    rh->z_buffer_size = 1024;
    rh->z_buffer = malloc(rh->z_buffer_size);
    rh->filename = g_strdup(filename);
    rh->size = FS_RHASH_DEFAULT_LENGTH;
    rh->search_dist = FS_RHASH_DEFAULT_SEARCH_DIST;
    rh->bucket_size = FS_RHASH_DEFAULT_BUCKET_SIZE;
    rh->revision = 1;
    rh->lex_filename = g_strdup_printf("%s.lex", filename);
    char *mode;
    if (flags & (O_WRONLY | O_RDWR)) {
        mode = "a+";
        rh->locked = 1;
        flock(rh->fd, LOCK_EX);
    } else {
        mode = "r";
    }
    const off_t file_length = lseek(rh->fd, 0, SEEK_END);
#ifndef FS_DISABLE_PREFIXES
    rh->prefixes = fs_prefix_trie_new();
    rh->ptrie = fs_prefix_trie_new();
    char *prefix_filename = g_strdup_printf("%s.prefixes", rh->filename);
    rh->prefix_file = fs_list_open_filename(prefix_filename,
                                            sizeof(struct prefix_file_line), flags);
    g_free(prefix_filename);
    struct prefix_file_line pre;
    fs_list_rewind(rh->prefix_file);
    while (fs_list_next_value(rh->prefix_file, &pre)) {
        fs_prefix_trie_add_code(rh->prefixes, pre.prefix, pre.code);
        rh->prefix_strings[pre.code] = g_strdup(pre.prefix);
        (rh->prefix_count)++;
    }
#endif
    if ((flags & O_TRUNC) || file_length == 0) {
        fs_rhash_write_header(rh);
    } else {
        pread(rh->fd, &header, sizeof(header), 0);
        if (header.id != FS_RHASH_ID) {
            fs_error(LOG_ERR, "%s does not appear to be a rhash file", rh->filename);

            return NULL;
        }
        rh->size = header.size;
        rh->count = header.count;
        rh->search_dist = header.search_dist;
        rh->bucket_size = header.bucket_size;
        if (rh->bucket_size == 0) {
            rh->bucket_size = 1;
        }
        rh->revision = header.revision;
    }
    fs_rhash_ensure_size(rh);
    const size_t len = sizeof(header) + ((size_t) rh->size) * ((size_t) rh->bucket_size) * sizeof(fs_rhash_entry);
    rh->entries = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, rh->fd, 0)
                + sizeof(header);
    if (rh->entries == MAP_FAILED) {
        fs_error(LOG_ERR, "failed to mmap rhash file “%s”: %s", rh->filename, strerror(errno));
        return NULL;
    }
    rh->lex_f = fopen(rh->lex_filename, mode);
    if (!rh->lex_f) {
        fs_error(LOG_ERR, "failed to open rhash lex file “%s”: %s",
                 rh->lex_filename, strerror(errno));

        return NULL;
    }

    return rh;
}

int fs_rhash_write_header(fs_rhash *rh)
{
    struct rhash_header header;

    header.id = FS_RHASH_ID;
    header.size = rh->size;
    header.count = rh->count;
    header.search_dist = rh->search_dist;
    header.bucket_size = rh->bucket_size;
    header.revision = rh->revision;
    memset(&header.padding, 0, sizeof(header.padding));
    if (pwrite(rh->fd, &header, sizeof(header), 0) == -1) {
        fs_error(LOG_CRIT, "failed to write header on %s: %s",
                 rh->filename, strerror(errno));

        return 1;
    }

    return 0;
}

int fs_rhash_flush(fs_rhash *rh)
{
    if (rh->flags & (O_WRONLY | O_RDWR)) {
        fs_rhash_write_header(rh);
        fflush(rh->lex_f);
    }

    return 0;
}

int fs_rhash_close(fs_rhash *rh)
{
    if (rh->flags & (O_WRONLY | O_RDWR)) {
        fs_rhash_write_header(rh);
    }

    fclose(rh->lex_f);
    if (rh->prefix_file) {
        fs_list_close(rh->prefix_file);
    }
    if (rh->locked) flock(rh->fd, LOCK_UN);
    const size_t len = sizeof(struct rhash_header) + ((size_t) rh->size) * ((size_t) rh->bucket_size) * sizeof(fs_rhash_entry);
    munmap((char *)rh->entries - sizeof(struct rhash_header), len);
    close(rh->fd);
    g_free(rh->filename);
    free(rh);

    return 0;
}

int fs_rhash_put(fs_rhash *rh, fs_resource *res)
{
    int entry = FS_RHASH_ENTRY(rh, res->rid);
    if (entry >= rh->size * rh->bucket_size) {
        fs_error(LOG_CRIT, "tried to write into rhash '%s' with bad entry number %d", rh->filename, entry);
        return 1;
    }
    fs_rhash_entry *buffer = rh->entries + entry;
    int new = -1;
    for (int i= 0; i < rh->search_dist && entry + i < rh->size * rh->bucket_size; i++) {
        if (buffer[i].rid == res->rid) {
            /* resource is already there, we're done */
            // TODO could check for collision

            return 0;
        } else if (buffer[i].rid == 0 && new == -1) {
            new = entry + i;
        }
    }
    if (new == -1) {
        /* hash overfull, grow */
        if (double_size(rh)) {
            fs_error(LOG_CRIT, "failed to correctly double size of rhash");
            return 1;
        }

        return fs_rhash_put(rh, res);
    }
    if (new >= rh->size * rh->bucket_size) {
        fs_error(LOG_CRIT, "writing RID %016llx past end of rhash '%s'", res->rid, rh->filename);
    }

    fs_rhash_entry e;
    e.rid = res->rid;
    e.aval.attr = res->attr;
    memset(&e.val.str, 0, INLINE_STR_LEN);
    if (strlen(res->lex) <= INLINE_STR_LEN) {
        strncpy(e.val.str, res->lex, INLINE_STR_LEN);
        e.disp = DISP_I_UTF8;
    } else if (compress_bcd(res->lex, NULL) == 0) {
        if (compress_bcd(res->lex, e.val.str)) {
            fs_error(LOG_ERR, "failed to compress '%s' as BCD", res->lex);
        }
        e.disp = DISP_I_NUMBER;
    } else if (compress_bcdate(res->lex, NULL) == 0) {
        if (compress_bcdate(res->lex, e.val.str)) {
            fs_error(LOG_ERR, "failed to compress '%s' as BCDate", res->lex);
        }
        e.disp = DISP_I_DATE;
    } else if (rh->prefixes && FS_IS_URI(res->rid) &&
               fs_prefix_trie_get_code(rh->prefixes, res->lex, NULL)) {
        int length = 0;
        int code = fs_prefix_trie_get_code(rh->prefixes, res->lex, &length);
        char *suffix = (res->lex)+length;
        const int32_t suffix_len = strlen(suffix);
        e.aval.pstr[0] = (char)code;
        if (suffix_len > 22) {
            /* even with prefix, won't fit inline */
            if (fseek(rh->lex_f, 0, SEEK_END) == -1) {
                fs_error(LOG_CRIT, "failed to fseek to end of '%s': %s",
                    rh->filename, strerror(errno));
                return 1;
            }
            long pos = ftell(rh->lex_f);
            if (fwrite(&suffix_len, sizeof(suffix_len), 1, rh->lex_f) != 1) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);

                return 1;
            }
            if (fputs(suffix, rh->lex_f) == EOF || fputc('\0', rh->lex_f) == EOF) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);
            }
            e.val.offset = pos;
            e.disp = DISP_F_PREFIX;
        } else {
            strncpy((char *)(e.aval.pstr)+1, suffix, 7);
            if (suffix_len > 7) {
                strncpy((char *)e.val.str, suffix+7, INLINE_STR_LEN);
            }
            e.disp = DISP_I_PREFIX;
        }
    } else {
        /* needs to go into external file */
        if (rh->ptrie && FS_IS_URI(res->rid)) {
            if (fs_prefix_trie_add_string(rh->ptrie, res->lex)) {
                /* add_string failed, prefix trie is probably full */
                fs_prefix *pre = fs_prefix_trie_get_prefixes(rh->ptrie, 32);
                int num = 0;
                struct prefix_file_line pfl;
                memset(&pfl, 0, sizeof(struct prefix_file_line));
                for (int i=0; i<32; i++) {
                    if (pre[i].score == 0 || rh->prefix_count == FS_MAX_PREFIXES) {
                        break;
                    }
                    num++;
                    rh->prefix_strings[rh->prefix_count] = strdup(pre[i].prefix);
                    fs_prefix_trie_add_code(rh->prefixes, pre[i].prefix,
                                            rh->prefix_count);
                    fs_error(LOG_INFO, "adding prefix %d <%s>", rh->prefix_count, pre[i].prefix);
                    pfl.code = rh->prefix_count;
                    strcpy(pfl.prefix, pre[i].prefix);
                    fs_list_add(rh->prefix_file, &pfl);
                    (rh->prefix_count)++;
                }
                fs_list_flush(rh->prefix_file);
                free(pre);
                fs_prefix_trie_free(rh->ptrie);
                rh->ptrie = fs_prefix_trie_new();
            }
        }

        /* check to see if there's any milage in compressing */
        int32_t lex_len = strlen(res->lex);
        /* grow z buffer if neccesary */
        if (rh->z_buffer_size < lex_len * 1.01 + 12) {
            while (rh->z_buffer_size < (lex_len * 1.01 + 12)) {
                rh->z_buffer_size *= 2;
            }
            free(rh->z_buffer);
            rh->z_buffer = malloc(rh->z_buffer_size);
            if (!rh->z_buffer) {
                fs_error(LOG_CRIT, "failed to allocate z buffer (%d bytes)", rh->z_buffer_size);
            }
        }
        unsigned long compsize = rh->z_buffer_size;
        char *data = res->lex;
        int32_t data_len = lex_len;
        char disp = DISP_F_UTF8;
        /* if the lex string is more than 100 chars long, try compressing it */
        if (lex_len > 100) {
            int ret = compress((Bytef *)rh->z_buffer, &compsize, (Bytef *)res->lex, (unsigned long)lex_len);
            if (ret == Z_OK) {
                if (compsize && compsize < lex_len - 4) {
                    data = rh->z_buffer;
                    data_len = compsize;
                    disp = DISP_F_ZCOMP;
                }
            } else {
                if (ret == Z_MEM_ERROR) {
                    fs_error(LOG_ERR, "zlib error: out of memory");
                } else if (ret == Z_BUF_ERROR) {
                    fs_error(LOG_ERR, "zlib error: buffer error");
                } else {
                    fs_error(LOG_ERR, "zlib error %d", ret);
                }
            }
        }
        if (fseek(rh->lex_f, 0, SEEK_END) == -1) {
            fs_error(LOG_CRIT, "failed to fseek to end of '%s': %s",
                rh->filename, strerror(errno));
                return 1;
        }
        long pos = ftell(rh->lex_f);
        e.disp = disp;
        if (fwrite(&data_len, sizeof(data_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);

            return 1;
        }
        if (disp == DISP_F_ZCOMP) {
            /* write the length of the uncompressed string too */
            if (fwrite(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
                fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                         rh->lex_filename);

                return 1;
            }
        }
        if (fwrite(data, data_len, 1, rh->lex_f) == EOF || fputc('\0', rh->lex_f) == EOF) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);
        }
        e.val.offset = pos;
    }
    rh->entries[new] = e;
    rh->count++;

    return 0;
}

static int sort_by_hash(const void *va, const void *vb)
{
    const fs_resource *a = va;
    const fs_resource *b = vb;
    int ea = FS_RHASH_ENTRY(global_sort_rh, a->rid);
    int eb = FS_RHASH_ENTRY(global_sort_rh, b->rid);

    if (ea != eb) return ea - eb;
    if (a->rid < b->rid) return -1;
    if (a->rid > b->rid) return 1;

    return 0;
}

static int double_size(fs_rhash *rh)
{
    long int oldsize = rh->size;
    long int errs = 0;

    fs_error(LOG_INFO, "doubling rhash (%s)", rh->filename);

    rh->size *= 2;
    fs_rhash_ensure_size(rh);
    const size_t oldlen = sizeof(struct rhash_header) + ((size_t) oldsize) * ((size_t) rh->bucket_size) * sizeof(fs_rhash_entry);
    const size_t newlen = sizeof(struct rhash_header) + ((size_t) rh->size) * ((size_t) rh->bucket_size) * sizeof(fs_rhash_entry);
    munmap((char *)rh->entries - sizeof(struct rhash_header), oldlen);
    rh->entries = mmap(NULL, newlen, PROT_READ | PROT_WRITE, MAP_SHARED, rh->fd, 0) + sizeof(struct rhash_header);
    if (rh->entries == MAP_FAILED) {
        fs_error(LOG_ERR, "failed to re-mmap rhash file “%s”: %s", rh->filename, strerror(errno));
        return -1;
    }

    fs_rhash_entry blank;
    memset(&blank, 0, sizeof(blank));
    fs_rhash_entry buffer_hi[rh->bucket_size];

    for (long int i=0; i<oldsize * rh->bucket_size; i += rh->bucket_size) {
        memset(buffer_hi, 0, sizeof(buffer_hi));
        fs_rhash_entry * const from = rh->entries + i;
        for (int j=0; j < rh->bucket_size; j++) {
            if (from[j].rid == 0) continue;

            long int entry = FS_RHASH_ENTRY(rh, from[j].rid);
            if (entry >= oldsize * rh->bucket_size) {
                buffer_hi[j] = from[j];
                from[j] = blank;
            }
        }
        memcpy(from + (oldsize * rh->bucket_size), buffer_hi, sizeof(buffer_hi));
    }
    fs_rhash_write_header(rh);

    return errs;
}

int fs_rhash_put_multi(fs_rhash *rh, fs_resource *res, int count)
{
    global_sort_rh = rh;
    qsort(res, count, sizeof(fs_resource), sort_by_hash);
    fs_rid last = FS_RID_NULL;

    int ret = 0;
    for (int i=0; i<count; i++) {
        if (res[i].rid == FS_RID_NULL) continue;
        if (res[i].rid == last) continue;
        ret += fs_rhash_put(rh, res+i);
        last = res[i].rid;
    }
    fs_rhash_write_header(rh);

    return ret;
}

static inline int get_entry(fs_rhash *rh, fs_rhash_entry *e, fs_resource *res)
{
    /* default, some things want to override this */
    res->attr = e->aval.attr;
    if (e->disp == DISP_I_UTF8) {
        res->lex = malloc(INLINE_STR_LEN+1);
        res->lex[INLINE_STR_LEN] = '\0';
        res->lex = memcpy(res->lex, e->val.str, INLINE_STR_LEN);
    } else if (e->disp == DISP_I_NUMBER) {
        res->lex = uncompress_bcd((unsigned char *)e->val.str);
    } else if (e->disp == DISP_I_DATE) {
        res->lex = uncompress_bcdate((unsigned char *)e->val.str);
    } else if (e->disp == DISP_I_PREFIX) {
        if (e->aval.pstr[0] >= rh->prefix_count) {
            res->lex = malloc(128);
            sprintf(res->lex, "¡bad prefix %d (max %d)!", e->aval.pstr[0], rh->prefix_count - 1);
            fs_error(LOG_ERR, "prefix %d out of range, count=%d", e->aval.pstr[0], rh->prefix_count);
        } else {
            const int pnum = e->aval.pstr[0];
            int plen = strlen(rh->prefix_strings[pnum]);
            res->lex = calloc(23 + plen, sizeof(char));
            strcpy(res->lex, rh->prefix_strings[pnum]);
            strncpy((res->lex) + plen, (char *)(e->aval.pstr)+1, 7);
            strncat((res->lex) + plen, e->val.str, 15);
            /* URIs have RID NULL */
            res->attr = FS_RID_NULL;
        }
    } else if (e->disp == DISP_F_UTF8) {
        int32_t lex_len;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        res->lex = malloc(lex_len + 1);

        if (fread(res->lex, sizeof(char), lex_len, rh->lex_f) < lex_len) {
            fs_error(LOG_ERR, "partial read %s from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex[0] = '\0';

            return 1;
        }
        res->lex[lex_len] = '\0';
    } else if (e->disp == DISP_F_PREFIX) {
        if (e->aval.pstr[0] >= rh->prefix_count) {
            fs_error(LOG_ERR, "prefix %d out of range, count=%d", e->aval.pstr[0], rh->prefix_count);

            return 1;
        }
        char *prefix = rh->prefix_strings[e->aval.pstr[0]];
        int prefix_len = strlen(prefix);
        int32_t lex_len = 0;
        int32_t suffix_len = 0;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&suffix_len, sizeof(suffix_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        lex_len = suffix_len + prefix_len;
        res->lex = malloc(lex_len + 1);
        strcpy(res->lex, prefix);

        if (fread((res->lex) + prefix_len, sizeof(char), suffix_len, rh->lex_f) < suffix_len) {
            fs_error(LOG_ERR, "partial read %s, of %d bytes (%d+%d) for RID %016llx from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", suffix_len, prefix_len, suffix_len, (long long)e->rid, rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex[0] = '\0';

            return 1;
        }
        res->lex[lex_len] = '\0';
    } else if (e->disp == DISP_F_ZCOMP) {
        int32_t data_len;
        int32_t lex_len;
        if (fseek(rh->lex_f, e->val.offset, SEEK_SET) == -1) {
            fs_error(LOG_ERR, "seek error reading lexical store '%s': %s", rh->lex_filename, strerror(errno));

            return 1;
        }
        if (fread(&data_len, sizeof(data_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }
        if (fread(&lex_len, sizeof(lex_len), 1, rh->lex_f) == 0) {
            fs_error(LOG_ERR, "read error from lexical store '%s', offset %lld: %s", rh->lex_filename, (long long)e->val.offset, strerror(errno));

            return 1;
        }

        if (rh->z_buffer_size < data_len) {
            while (rh->z_buffer_size < data_len) {
                rh->z_buffer_size *= 2;
            }
            free(rh->z_buffer);
            rh->z_buffer = malloc(rh->z_buffer_size);
        }
        res->lex = malloc(lex_len + 1);
        if (fread(rh->z_buffer, sizeof(char), data_len, rh->lex_f) < data_len) {
            fs_error(LOG_ERR, "partial read %s from lexical store '%s'", ferror(rh->lex_f) ? "error" : "EOF", rh->lex_filename);
            clearerr(rh->lex_f);
            res->lex = strdup("¡read error!");

            return 1;
        }
        unsigned long uncomp_len = lex_len;
        unsigned long dlen = data_len;
        int ret;
        ret = uncompress((Bytef *)res->lex, &uncomp_len, (Bytef *)rh->z_buffer, dlen);
        if (ret == Z_OK) {
            if (uncomp_len != lex_len) {
                fs_error(LOG_ERR, "something went wrong in decompression");
            }
            res->lex[lex_len] = '\0';
        } else {
            if (ret == Z_MEM_ERROR) {
                fs_error(LOG_ERR, "zlib error: out of memory");
            } else if (ret == Z_BUF_ERROR) {
                fs_error(LOG_ERR, "zlib error: buffer error");
            } else {
                fs_error(LOG_ERR, "zlib error %d, uncomp_len = %d (%d), comp_len = %d", ret, (int)uncomp_len, (int)lex_len, (int)dlen);
            }
            res->lex[0] = '\0';

            return 1;
        }
    } else {
        res->lex = g_strdup_printf("error: unknown disposition: %c", e->disp);

        return 1;
    }

    return 0;
}

static int fs_rhash_get_intl(fs_rhash *rh, fs_resource *res)
{
    const int entry = FS_RHASH_ENTRY(rh, res->rid);
    fs_rhash_entry *buffer = rh->entries + entry;

    for (int k = 0; k < rh->search_dist; ++k) {
        if (buffer[k].rid == res->rid) {
            return get_entry(rh, &buffer[k], res);
        }
    }

    fs_error(LOG_WARNING, "resource %016llx not found in § 0x%x-0x%x of %s", res->rid, entry, entry + rh->search_dist - 1, rh->filename);
    res->lex = g_strdup_printf("¡resource %llx not found!", res->rid);
    res->attr = 0;

    return 1;
}

int fs_rhash_get(fs_rhash *rh, fs_resource *res)
{
    if (!rh->locked) flock(rh->fd, LOCK_SH);
    int ret = fs_rhash_get_intl(rh, res);
    if (!rh->locked) flock(rh->fd, LOCK_UN);

    return ret;
}

int fs_rhash_get_multi(fs_rhash *rh, fs_resource *res, int count)
{
    global_sort_rh = rh;
    qsort(res, count, sizeof(fs_resource), sort_by_hash);

    int ret = 0;
    if (!rh->locked) flock(rh->fd, LOCK_SH);
    for (int i=0; i<count; i++) {
        res[i].attr = FS_RID_NULL;
        res[i].lex = NULL;
        if (FS_IS_BNODE(res[i].rid)) {
            res[i].lex = g_strdup_printf("_:b%llx", res[i].rid);
            continue;
        }
        ret += fs_rhash_get_intl(rh, res+i);
    }
    if (!rh->locked) flock(rh->fd, LOCK_UN);

    return ret;
}

void fs_rhash_print(fs_rhash *rh, FILE *out, int verbosity)
{
    if (!rh) {
        fprintf(out, "ERROR: tried to print NULL mhash\n");

        return;
    }

    int disp_freq[128];
    memset(disp_freq, 0, 128);

    fprintf(out, "%s\n", rh->filename);
    fprintf(out, "size:     %d (buckets)\n", rh->size);
    fprintf(out, "bucket:   %d\n", rh->bucket_size);
    fprintf(out, "entries:  %d\n", rh->count);
    fprintf(out, "prefixes:  %d\n", rh->prefix_count);
    fprintf(out, "revision: %d\n", rh->revision);
    fprintf(out, "fill:     %.1f%%\n", 100.0 * (double)rh->count / (double)(rh->size * rh->bucket_size));

    if (verbosity < 1) {
        return;
    }

    for (int p=0; p<rh->prefix_count; p++) {
        fprintf(out, "prefix %d: %s\n", p, rh->prefix_strings[p]);
    }

    if (verbosity < 2) {
        return;
    }

    fs_rhash_entry e;
    int entry = 0, entries = 0, show_next = 0;

    lseek(rh->fd, sizeof(struct rhash_header), SEEK_SET);
    while (read(rh->fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.rid) {
            char *ent_str = g_strdup_printf("%08d.%02d", entry / rh->bucket_size, entry % rh->bucket_size);
            fs_resource res = { .lex = NULL };
            int ret = get_entry(rh, &e, &res);
            if (ret) {
                fprintf(out, "ERROR: failed to get entry for %016llx\n", e.rid);
                continue;
            }
            if (e.disp == DISP_F_UTF8 || e.disp == DISP_F_ZCOMP) {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %016llx %c %10lld %s\n", ent_str, e.rid, e.aval.attr, e.disp, (long long)e.val.offset, res.lex);
            } else if (e.disp == DISP_F_PREFIX) {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %16d %c %10lld %s\n", ent_str, e.rid, (unsigned char)e.aval.pstr[0], e.disp, (long long)e.val.offset, res.lex);
            } else {
                if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %016llx %c %s\n", ent_str, e.rid, e.aval.attr, e.disp, res.lex);
            }
            disp_freq[(int)e.disp]++;
            entries++;
            show_next = 0;
            free(res.lex);
            g_free(ent_str);
        }
        entry++;
    }
    fprintf(out, "STATS: length: %d, bsize: %d, entries: %d (%+d), %.1f%% full\n", rh->size, rh->bucket_size, entries, rh->count - entries, 100.0 * (double)entries / (double)(rh->size * rh->bucket_size));
    if (rh->count != entries) {
        fprintf(out, "ERROR: entry count in header %d != count from scan %d\n",
                rh->count, entries);
    }
    fprintf(out, "Disposition frequencies:\n");
    for (int d=0; d<128; d++) {
        if (disp_freq[d] > 0) {
            fprintf(out, "%c: %8d\n", d, disp_freq[d]);
        }
    }
}

int fs_rhash_count(fs_rhash *rh)
{
    return rh->count;
}

/* literal storage compression functions */

enum bcd {
    bcd_nul = 0,
    bcd_1,
    bcd_2,
    bcd_3,
    bcd_4,
    bcd_5,
    bcd_6,
    bcd_7,
    bcd_8,
    bcd_9,
    bcd_0,
    bcd_dot,
    bcd_plus,
    bcd_minus,
    bcd_e
};

static const char bcd_map[16] = {
    '\0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', '0', '.', '+', '-', 'e', '?'
};

enum bcdate {
    bcdate_nul = 0,
    bcdate_1,
    bcdate_2,
    bcdate_3,
    bcdate_4,
    bcdate_5,
    bcdate_6,
    bcdate_7,
    bcdate_8,
    bcdate_9,
    bcdate_0,
    bcdate_colon,
    bcdate_plus,
    bcdate_minus,
    bcdate_T,
    bcdate_Z
};

static const char bcdate_map[16] = {
    '\0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', '0', ':', '+', '-', 'T', 'Z'
};

static inline void write_bcd(char *out, int pos, int val)
{
    out += pos / 2;
    const int offset = (pos % 2) * 4;
    *out |= (val << offset);
}

static int compress_bcd(const char *in, char *out)
{
    if (strlen(in) > INLINE_STR_LEN * 2) {
        /* too long */
        return 1;
    }

    /* zero output buffer */
    if (out) {
        memset(out, 0, INLINE_STR_LEN);
    }
    int outpos = 0;
    for (const char *inp = in; *inp; inp++) {
        switch (*inp) {
        case '1':
            if (out) {
                write_bcd(out, outpos, bcd_1);
                outpos++;
            }
            break;
        case '2':
            if (out) {
                write_bcd(out, outpos, bcd_2);
                outpos++;
            }
            break;
        case '3':
            if (out) {
                write_bcd(out, outpos, bcd_3);
                outpos++;
            }
            break;
        case '4':
            if (out) {
                write_bcd(out, outpos, bcd_4);
                outpos++;
            }
            break;
        case '5':
            if (out) {
                write_bcd(out, outpos, bcd_5);
                outpos++;
            }
            break;
        case '6':
            if (out) {
                write_bcd(out, outpos, bcd_6);
                outpos++;
            }
            break;
        case '7':
            if (out) {
                write_bcd(out, outpos, bcd_7);
                outpos++;
            }
            break;
        case '8':
            if (out) {
                write_bcd(out, outpos, bcd_8);
                outpos++;
            }
            break;
        case '9':
            if (out) {
                write_bcd(out, outpos, bcd_9);
                outpos++;
            }
            break;
        case '0':
            if (out) {
                write_bcd(out, outpos, bcd_0);
                outpos++;
            }
            break;
        case '.':
            if (out) {
                write_bcd(out, outpos, bcd_dot);
                outpos++;
            }
            break;
        case '+':
            if (out) {
                write_bcd(out, outpos, bcd_plus);
                outpos++;
            }
            break;
        case '-':
            if (out) {
                write_bcd(out, outpos, bcd_minus);
                outpos++;
            }
            break;
        case 'e':
            if (out) {
                write_bcd(out, outpos, bcd_e);
                outpos++;
            }
            break;
        default:
            /* character we can't handle */
            return 1;
        }
    }

    /* worked OK */
    return 0;
}

static int compress_bcdate(const char *in, char *out)
{
    if (strlen(in) > INLINE_STR_LEN * 2) {
        /* too long */
        return 1;
    }

    /* zero output buffer */
    if (out) {
        memset(out, 0, INLINE_STR_LEN);
    }
    int outpos = 0;
    for (const char *inp = in; *inp; inp++) {
        switch (*inp) {
        case '1':
            if (out) {
                write_bcd(out, outpos, bcdate_1);
                outpos++;
            }
            break;
        case '2':
            if (out) {
                write_bcd(out, outpos, bcdate_2);
                outpos++;
            }
            break;
        case '3':
            if (out) {
                write_bcd(out, outpos, bcdate_3);
                outpos++;
            }
            break;
        case '4':
            if (out) {
                write_bcd(out, outpos, bcdate_4);
                outpos++;
            }
            break;
        case '5':
            if (out) {
                write_bcd(out, outpos, bcdate_5);
                outpos++;
            }
            break;
        case '6':
            if (out) {
                write_bcd(out, outpos, bcdate_6);
                outpos++;
            }
            break;
        case '7':
            if (out) {
                write_bcd(out, outpos, bcdate_7);
                outpos++;
            }
            break;
        case '8':
            if (out) {
                write_bcd(out, outpos, bcdate_8);
                outpos++;
            }
            break;
        case '9':
            if (out) {
                write_bcd(out, outpos, bcdate_9);
                outpos++;
            }
            break;
        case '0':
            if (out) {
                write_bcd(out, outpos, bcdate_0);
                outpos++;
            }
            break;
        case ':':
            if (out) {
                write_bcd(out, outpos, bcdate_colon);
                outpos++;
            }
            break;
        case '+':
            if (out) {
                write_bcd(out, outpos, bcdate_plus);
                outpos++;
            }
            break;
        case '-':
            if (out) {
                write_bcd(out, outpos, bcdate_minus);
                outpos++;
            }
            break;
        case 'T':
            if (out) {
                write_bcd(out, outpos, bcdate_T);
                outpos++;
            }
            break;
        case 'Z':
            if (out) {
                write_bcd(out, outpos, bcdate_Z);
                outpos++;
            }
            break;
        default:
            /* character we can't handle */
            return 1;
        }
    }

    /* worked OK */
    return 0;
}

static char *uncompress_bcd(unsigned char *bcd)
{
    char *out = calloc(INLINE_STR_LEN*2 + 1, sizeof(char));

    for (int inpos = 0; inpos < INLINE_STR_LEN*2; inpos++) {
        unsigned int code = bcd[inpos/2];
        if (inpos % 2 == 0) {
            code &= 15;
        } else {
            code >>= 4;
        }
        if (code == bcd_nul) {
            break;
        }
        out[inpos] = bcd_map[code];
    }

    return out;
}

static char *uncompress_bcdate(unsigned char *bcd)
{
    char *out = calloc(INLINE_STR_LEN*2 + 1, sizeof(char));

    for (int inpos = 0; inpos < INLINE_STR_LEN*2; inpos++) {
        unsigned int code = bcd[inpos/2];
        if (inpos % 2 == 0) {
            code &= 15;
        } else {
            code >>= 4;
        }
        if (code == bcdate_nul) {
            break;
        }
        out[inpos] = bcdate_map[code];
    }

    return out;
}

/* vi:set expandtab sts=4 sw=4: */
