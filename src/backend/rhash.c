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
#ifndef __APPLE__
#define _XOPEN_SOURCE 500
#endif
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "backend.h"
#include "rhash.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"

#define FS_RHASH_DEFAULT_LENGTH        65536
#define FS_RHASH_DEFAULT_SEARCH_DIST      32
#define FS_RHASH_DEFAULT_BUCKET_SIZE      16

#define FS_RHASH_ID 0x4a585230

/* maximum distance that we wil allow the resource to be from its hash value */

#define FS_PACKED __attribute__((__packed__))

#define FS_RHASH_ENTRY(rh, rid) (((uint64_t)(rid >> 10) & ((uint64_t)(rh->size - 1)))*rh->bucket_size)

struct rhash_header {
    int32_t id;             // "JXR0"
    uint32_t size;          // size of hashtable in buckets,
                            //    must be power of two
    uint32_t count;         // number of resources in hashtable
    uint32_t search_dist;   // offset to scant to in table for match
    uint32_t bucket_size;   // number of entries per bucket
    uint32_t revision;      // revision of the strucure
                            // rev=0: 33 byte, semi-packed entries
                            // rev=1: 32 byte, packed entries
    char padding[488];      // allign to a block
} FS_PACKED;
 
struct _fs_rhash {
    uint32_t size;
    uint32_t count;
    uint32_t search_dist;
    uint32_t bucket_size;
    uint32_t revision;
    int fd;
    char *filename;
    FILE *lex_f;
    char *lex_filename;
    int flags;
    int locked;
};

#define INLINE_STR_LEN 15

typedef struct _fs_rhash_entry {
    fs_rid rid;
    fs_rid attr;        // attribute value, lang tag or datatype
    union {
        int64_t offset; // offset in lex file
        char str[INLINE_STR_LEN];   // inline string data
    } FS_PACKED val;
    char disp;          // disposition of data - lex file or inline
} FS_PACKED fs_rhash_entry;

static fs_rhash *global_sort_rh = NULL;

static int double_size(fs_rhash *rh);
int fs_rhash_write_header(fs_rhash *rh);

static int compress_bcd(const char *in, char *out);
static char *uncompress_bcd(unsigned char *bcd);

fs_rhash *fs_rhash_open(fs_backend *be, const char *label, int flags)
{
    char *filename = g_strdup_printf(FS_RHASH, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), label);
    fs_rhash *rh = fs_rhash_open_filename(filename, flags);
    g_free(filename);

    return rh;
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
    off_t file_length = lseek(rh->fd, 0, SEEK_END);
    lseek(rh->fd, 0, SEEK_SET);
    if ((flags & O_TRUNC) || file_length == 0) {
        fs_rhash_write_header(rh);
    } else {
        read(rh->fd, &header, sizeof(header));
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
    if (rh->locked) flock(rh->fd, LOCK_UN);
    close(rh->fd);
    g_free(rh->filename);
    free(rh);

    return 0;
}

int fs_rhash_put(fs_rhash *rh, fs_resource *res)
{
    unsigned long long entry = FS_RHASH_ENTRY(rh, res->rid);
    if (entry >= rh->size * rh->bucket_size) {
        fs_error(LOG_CRIT, "tried to write into rhash '%s' with bad entry number %lld", rh->filename, entry);

        return 1;
    }
    fs_rhash_entry buffer[rh->search_dist];
    fs_rhash_entry e;
    memset(buffer, 0, sizeof(buffer));
    int read_len = pread(rh->fd, buffer, sizeof(buffer),
                         sizeof(struct rhash_header) + entry * sizeof(e));
    if (read_len == -1) {
        fs_error(LOG_CRIT, "read from %s failed: %s", rh->filename,
                 strerror(errno));

        return 1;
    }
    for (int i=0; 1; i++) {
        if (buffer[i].rid == res->rid) {
            /* resource is allready there, we're done */
            // TODO could check for collision

            return 0;
        } else if (buffer[i].rid == 0) {
            break;
        }
        if (i == rh->search_dist || entry == rh->size * rh->bucket_size - 1) {
	    /* hash overfull, grow */
            if (double_size(rh)) {
                fs_error(LOG_CRIT, "failed to correctly double size of rhash");
            }

            return fs_rhash_put(rh, res);
        }
        entry++;
    }
    if (entry >= rh->size * rh->bucket_size) {
        fs_error(LOG_CRIT, "writing RID %016llx past end of rhash '%s'", res->rid, rh->filename);
    }
    e.rid = res->rid;
    e.attr = res->attr;
    memset(&e.val.str, 0, INLINE_STR_LEN);
    if (strlen(res->lex) <= INLINE_STR_LEN) {
        strncpy(e.val.str, res->lex, INLINE_STR_LEN);
        e.disp = 'i';
    } else if (compress_bcd(res->lex, NULL) == 0) {
        if (compress_bcd(res->lex, e.val.str)) {
            fs_error(LOG_ERR, "failed to compress '%s' as BCD", res->lex);
        }
        e.disp = 'N';
    } else {
        if (fseek(rh->lex_f, 0, SEEK_END) == -1) {
            fs_error(LOG_CRIT, "failed to fseek to end of '%s': %s",
                rh->filename, strerror(errno));
        }
        long pos = ftell(rh->lex_f);
        int len = strlen(res->lex);
        e.disp = 'f';
        if (fwrite(&len, sizeof(len), 1, rh->lex_f) == 0) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);

            return 1;
        }
        if (fputs(res->lex, rh->lex_f) == EOF || fputc('\0', rh->lex_f) == EOF) {
            fs_error(LOG_CRIT, "failed writing to lexical file “%s”",
                     rh->lex_filename);
        }
        e.val.offset = pos;
    }
    if (pwrite(rh->fd, &e, sizeof(e), sizeof(struct rhash_header) +
               entry * sizeof(e)) == -1) {
        fs_error(LOG_CRIT, "write to %s failed: %s", rh->filename,
                 strerror(errno));

        return 1;
    }
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

    rh->size *= 2;
    fs_rhash_entry blank;
    memset(&blank, 0, sizeof(blank));
    fs_rhash_entry buffer_lo[rh->bucket_size];
    fs_rhash_entry buffer_hi[rh->bucket_size];

    for (long int i=0; i<oldsize * rh->bucket_size; i += rh->bucket_size) {
        memset(buffer_lo, 0, sizeof(buffer_lo));
        memset(buffer_hi, 0, sizeof(buffer_hi));
        if (pread(rh->fd, &buffer_lo, sizeof(buffer_lo),
                   sizeof(struct rhash_header) +
                   i * sizeof(fs_rhash_entry)) == -1) {
            fs_error(LOG_CRIT, "bucket read error in '%s': %s", rh->filename,
                     strerror(errno));
        }
        for (int j=0; j < rh->bucket_size; j++) {
            if (buffer_lo[j].rid == 0) continue;
            long int entry = FS_RHASH_ENTRY(rh, buffer_lo[j].rid);
            if (entry >= oldsize * rh->bucket_size) {
                buffer_hi[j] = buffer_lo[j];
                buffer_lo[j] = blank;
            }
        }
        if (pwrite(rh->fd, &buffer_lo, sizeof(buffer_lo),
                   sizeof(struct rhash_header) +
                   i * sizeof(fs_rhash_entry)) == -1) {
            fs_error(LOG_CRIT, "failed to write rhash '%s' bucket: %s",
                     rh->filename, strerror(errno));
            errs++;
        }
        if (pwrite(rh->fd, &buffer_hi, sizeof(buffer_hi),
                   sizeof(struct rhash_header) +
                   (oldsize * rh->bucket_size + i) *
                   sizeof(fs_rhash_entry)) == -1) {
            fs_error(LOG_CRIT, "failed to write rhash '%s' bucket: %s",
                     rh->filename, strerror(errno));
            errs++;
        }
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
    if (e->disp == 'i') {
        res->lex = malloc(INLINE_STR_LEN+1);
        res->lex[INLINE_STR_LEN] = '\0';
        res->lex = memcpy(res->lex, e->val.str, INLINE_STR_LEN);
    } else if (e->disp == 'N') {
        res->lex = uncompress_bcd((unsigned char *)e->val.str);
    } else if (e->disp == 'f') {
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
            fs_error(LOG_ERR, "partial read error from lexical store '%s'", rh->lex_filename);
            res->lex[0] = '\0';

            return 1;
        }
        res->lex[lex_len] = '\0';
    } else {
        res->lex = g_strdup_printf("error: unknown disposition: %c", e->disp);

        return 1;
    }
    res->attr = e->attr;

    return 0;
}

static int fs_rhash_get_intl(fs_rhash *rh, fs_resource *res)
{
    int entry = FS_RHASH_ENTRY(rh, res->rid);
    fs_rhash_entry e;

    /* special case for handling common small block read case - one medium
       read rather than N small ones */
    if (rh->search_dist == FS_RHASH_DEFAULT_SEARCH_DIST) {
        fs_rhash_entry buffer[FS_RHASH_DEFAULT_SEARCH_DIST];
        const int ret = pread(rh->fd, buffer, sizeof(buffer), sizeof(struct rhash_header) + entry * sizeof(fs_rhash_entry));
        if (ret == -1) {
            fs_error(LOG_CRIT, "pread from %s failed: %s", rh->filename, strerror(errno));
        }
        for (int i=0; i<ret/sizeof(fs_rhash_entry); i++) {
            if (buffer[i].rid == res->rid) {
                return get_entry(rh, &buffer[i], res);
            }
        }
        fs_error(LOG_WARNING, "resource %016llx not found in § 0x%x-0x%x of %s", res->rid, (int)FS_RHASH_ENTRY(rh, res->rid), entry-1, rh->filename);
        res->lex = g_strdup_printf("¡resource %llx not found!", res->rid);
        res->attr = 0;

        return 1;
    }

    for (int i=0; i<rh->search_dist; i++) {
        if (pread(rh->fd, &e, sizeof(e), sizeof(struct rhash_header) + entry * sizeof(e)) == -1) {
            fs_error(LOG_CRIT, "read from %s failed: %s", rh->filename, strerror(errno));

            return 1;
        }
        if (e.rid == res->rid) {
            return get_entry(rh, &e, res);
        }
        entry++;
        if (entry == rh->size * rh->bucket_size) break;
    }

    fs_error(LOG_WARNING, "resource %llx not found in § 0x%x-0x%x of %s", res->rid, (int)FS_RHASH_ENTRY(rh, res->rid), entry-1, rh->filename);
    res->lex = g_strdup_printf("resource %llx not found", res->rid);
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

    if (verbosity == 0) {
        fprintf(out, "%s\n", rh->filename);
        fprintf(out, "size:     %d (buckets)\n", rh->size);
        fprintf(out, "bucket:   %d\n", rh->bucket_size);
        fprintf(out, "entries:  %d\n", rh->count);
        fprintf(out, "revision: %d\n", rh->revision);
        fprintf(out, "fill:     %.1f%%\n", 100.0 * (double)rh->count / (double)(rh->size * rh->bucket_size));

        return;
    } 
    fs_rhash_entry e;
    int entry = 0, entries = 0, show_next = 0;

    lseek(rh->fd, sizeof(struct rhash_header), SEEK_SET);
    while (read(rh->fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.rid) {
            char *ent_str = g_strdup_printf("%08d.%02d", entry / rh->bucket_size, entry % rh->bucket_size);
            fs_resource res = { .lex = NULL };
            get_entry(rh, &e, &res);
            if (verbosity > 1 || show_next) fprintf(out, "%s %016llx %016llx %c %10lld %s\n", ent_str, e.rid, e.attr, e.disp, e.disp == 'f' ? (long long)e.val.offset : 0, res.lex);
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
else { printf("not writing...\n"); }
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

/* vi:set expandtab sts=4 sw=4: */
