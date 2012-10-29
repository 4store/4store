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
#include "mhash.h"
#include "tbchain.h"
#include "../common/params.h"
#include "../common/error.h"

#define FS_MHASH_DEFAULT_LENGTH         4096
#define FS_MHASH_DEFAULT_SEARCH_DIST      16

#define FS_MHASH_ID 0x4a584d30

/* maximum distance that we wil allow the resource to be from its hash value */

#define FS_PACKED __attribute__((__packed__))

#define FS_MHASH_ENTRY(mh, rid) ((rid >> 10) & (mh->size - 1))

struct mhash_header {
    int32_t id;             // "JXM0"
    int32_t size;           // size of hashtable, must be power of two
    int32_t count;          // number of models in hashtable
    int32_t search_dist;    // offset to scan up to in table for match
    char padding[496];      // allign to a block
} FS_PACKED;

struct _fs_mhash {
    int32_t size;
    int32_t count;
    int32_t search_dist;
    int fd;
    char *filename;
    int flags;
    int locked;
};

typedef struct _fs_mhash_entry {
    fs_rid rid;
    fs_index_node val;        // 0 = unused, 1 = in seperate file, 2+ = in list
} FS_PACKED fs_mhash_entry;

static int double_size(fs_mhash *mh);
static int fs_mhash_write_header(fs_mhash *mh);

fs_mhash *fs_mhash_open(fs_backend *be, const char *label, int flags)
{
    char *filename = g_strdup_printf(FS_MHASH, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), label);
    fs_mhash *mh = fs_mhash_open_filename(filename, flags);
    g_free(filename);

    return mh;
}

fs_mhash *fs_mhash_open_filename(const char *filename, int flags)
{
    struct mhash_header header;
    if (sizeof(header) != 512) {
        fs_error(LOG_CRIT, "incorrect mhash header size %zd, should be 512",
                 sizeof(header));
        return NULL;
    }
    if (sizeof(fs_mhash_entry) != 12) {
        fs_error(LOG_CRIT, "incorrect entry size %zd, should be 12",
                 sizeof(fs_mhash_entry));
        return NULL;
    }

    fs_mhash *mh = calloc(1, sizeof(fs_mhash));
    mh->fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    mh->flags = flags;
    if (mh->fd == -1) {
        fs_error(LOG_ERR, "cannot open mhash file '%s': %s", filename, strerror(errno));

        return NULL;
    }
    mh->filename = g_strdup(filename);
    mh->size = FS_MHASH_DEFAULT_LENGTH;
    mh->search_dist = FS_MHASH_DEFAULT_SEARCH_DIST;
    if (flags & (O_WRONLY | O_RDWR)) {
        mh->locked = 1;
        flock(mh->fd, LOCK_EX);
    }
    off_t file_length = lseek(mh->fd, 0, SEEK_END);
    lseek(mh->fd, 0, SEEK_SET);
    if ((flags & O_TRUNC) || file_length == 0) {
        fs_mhash_write_header(mh);
    } else {
        read(mh->fd, &header, sizeof(header));
        if (header.id != FS_MHASH_ID) {
            fs_error(LOG_ERR, "%s does not appear to be a mhash file", mh->filename);

            return NULL;
        }
        mh->size = header.size;
        mh->count = header.count;
        mh->search_dist = header.search_dist;
    }

    return mh;
}

static int fs_mhash_write_header(fs_mhash *mh)
{
    if (!mh) {
        fs_error(LOG_CRIT, "tried to write header of NULL mhash");

        return 1;
    }
    struct mhash_header header;

    header.id = FS_MHASH_ID;
    header.size = mh->size;
    header.count = mh->count;
    header.search_dist = mh->search_dist;
    memset(&header.padding, 0, sizeof(header.padding));
    if (pwrite(mh->fd, &header, sizeof(header), 0) == -1) {
        fs_error(LOG_CRIT, "failed to write header on %s: %s",
                 mh->filename, strerror(errno));

        return 1;
    }

    return 0;
}

int fs_mhash_flush(fs_mhash *mh)
{
    if (mh->flags & (O_WRONLY | O_RDWR)) {
        return fs_mhash_write_header(mh);
    }

    return 0;
}

int fs_mhash_close(fs_mhash *mh)
{
    if (mh->flags & (O_WRONLY | O_RDWR)) {
        fs_mhash_write_header(mh);
    }
    if (mh->locked) flock(mh->fd, LOCK_UN);
    close(mh->fd);
    mh->fd = -1;
    g_free(mh->filename);
    mh->filename = NULL;
    free(mh);

    return 0;
}

int fs_mhash_put(fs_mhash *mh, const fs_rid rid, fs_index_node val)
{
    int entry = FS_MHASH_ENTRY(mh, rid);
    fs_mhash_entry e;
    int candidate = -1;
    for (int i=0; 1; i++) {
        e.rid = 0;
        e.val = 0;
        if (pread(mh->fd, &e, sizeof(e), sizeof(struct mhash_header) +
                  entry * sizeof(e)) == -1) {
            fs_error(LOG_CRIT, "read from %s failed: %s", mh->filename,
                     strerror(errno));

            return 1;
        }
        if (e.rid == rid) {
            /* model is allready there, replace value */

            break;
        } else if (e.rid == 0 && candidate == -1) {
            /* we can't break here because there might be a mathcing entry
             * later in the hashtable */
            candidate = entry;
        }
        if ((i == mh->search_dist || entry == mh->size - 1) &&
            candidate != -1) {
            /* we can use the candidate we found earlier */
            entry = candidate;
            if (pread(mh->fd, &e, sizeof(e), sizeof(struct mhash_header) +
                      entry * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "read from %s failed: %s", mh->filename,
                         strerror(errno));

                return 1;
            }

            break;
        }
        if (i == mh->search_dist || entry == mh->size - 1) {
            /* model hash overful, grow */
            double_size(mh);

            return fs_mhash_put(mh, rid, val);
        }
        entry++;
    }

    /* if there's no changes to be made we don't want to write anything */
    if (e.rid == rid && e.val == val) return 0;

    fs_index_node oldval = e.val;

    e.rid = rid;
    e.val = val;
    if (pwrite(mh->fd, &e, sizeof(e), sizeof(struct mhash_header) +
               entry * sizeof(e)) == -1) {
        fs_error(LOG_CRIT, "write to %s failed: %s", mh->filename,
                 strerror(errno));

        return 1;
    }
    if (val) {
        if (!oldval) mh->count++;
    } else {
        if (oldval) mh->count--;
    }

    return 0;
}

static int double_size(fs_mhash *mh)
{
    int32_t oldsize = mh->size;
    int errs = 0;

    mh->size *= 2;
    mh->search_dist *= 2;
    mh->search_dist++;
    fs_mhash_entry blank;
    memset(&blank, 0, sizeof(blank));
    for (int i=0; i<oldsize; i++) {
        fs_mhash_entry e;
        pread(mh->fd, &e, sizeof(e), sizeof(struct mhash_header) + i * sizeof(e));
        if (e.rid == 0) continue;
        int entry = FS_MHASH_ENTRY(mh, e.rid);
        if (entry >= oldsize) {
            if (pwrite(mh->fd, &blank, sizeof(blank),
                       sizeof(struct mhash_header) + i * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "failed to write mhash '%s' entry: %s",
                         mh->filename, strerror(errno));
                errs++;
            }
            if (pwrite(mh->fd, &e, sizeof(e),
                       sizeof(struct mhash_header) +
                       (oldsize+i) * sizeof(e)) == -1) {
                fs_error(LOG_CRIT, "failed to write mhash '%s' entry: %s",
                         mh->filename, strerror(errno));
                errs++;
            }
        }
    }

    return errs;
}

static int fs_mhash_get_intl(fs_mhash *mh, const fs_rid rid, fs_index_node *val)
{
    int entry = FS_MHASH_ENTRY(mh, rid);
    fs_mhash_entry e;
    memset(&e, 0, sizeof(e));

    for (int i=0; i<mh->search_dist; i++) {
        if (pread(mh->fd, &e, sizeof(e), sizeof(struct mhash_header) + entry * sizeof(e)) == -1) {
            fs_error(LOG_CRIT, "read from %s failed: %s", mh->filename, strerror(errno));

            return 1;
        }
        if (e.rid == rid) {
            *val = e.val;

            return 0;
        }
        entry = (entry + 1) & (mh->size - 1);
        if (entry == 0) break;
    }

    *val = 0;

    return 0;
}

int fs_mhash_get(fs_mhash *mh, const fs_rid rid, fs_index_node *val)
{
    if (!mh->locked) flock(mh->fd, LOCK_SH);
    int ret = fs_mhash_get_intl(mh, rid, val);
    if (!mh->locked) flock(mh->fd, LOCK_UN);

    return ret;
}

fs_rid_vector *fs_mhash_get_keys(fs_mhash *mh)
{
    if (!mh) {
        fs_error(LOG_CRIT, "tried to get keys from NULL mhash");

        return NULL;
    }
    fs_rid_vector *v = fs_rid_vector_new(0);

    fs_mhash_entry e;

    if (!mh->locked) flock(mh->fd, LOCK_SH);
    if (lseek(mh->fd, sizeof(struct mhash_header), SEEK_SET) == -1) {
        fs_error(LOG_ERR, "seek error on mhash: %s", strerror(errno));
    }
    while (read(mh->fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.val) fs_rid_vector_append(v, e.rid);
    }
    if (!mh->locked) flock(mh->fd, LOCK_UN);

    return v;
}

void fs_mhash_check_chain(fs_mhash *mh, fs_tbchain *tbc, FILE *out, int verbosity)
{
    if (!mh) {
        fs_error(LOG_CRIT, "tried to print NULL mhash");

        return;
    }
    fs_mhash_entry e;
    int entry = 0;
    int count = 0;

    lseek(mh->fd, sizeof(struct mhash_header), SEEK_SET);
    while (read(mh->fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.rid && e.val) {
            count++;
            fprintf(out, "%016llx %8d:\n", e.rid, e.val);
            if (verbosity > 0) {
                fs_tbchain_get_stats(tbc, e.val, out);
            }
            if (fs_tbchain_check_consistency(tbc, e.rid, e.val, out)) {
                printf("check failed\n");
            }
        }
        entry++;
    }
    if (count && fs_tbchain_check_leaks(tbc, out)) {
        printf("check failed\n");
    }

    if (mh->count != count) {
        fprintf(out, "ERROR: %s header count %d != scanned count %d\n",
                mh->filename, mh->count, count);
    }
}

void fs_mhash_print(fs_mhash *mh, FILE *out, int verbosity)
{
    if (!mh) {
        fs_error(LOG_CRIT, "tried to print NULL mhash");

        return;
    }
    fs_mhash_entry e;
    fs_rid_vector *models = fs_rid_vector_new(0);
    fs_rid last_model = FS_RID_NULL;
    int entry = 0;
    int count = 0;

    fprintf(out, "mhash %s\n", mh->filename);
    fprintf(out, "  count: %d\n", mh->count);
    fprintf(out, "  size: %d\n", mh->size);
    fprintf(out, "\n");

    lseek(mh->fd, sizeof(struct mhash_header), SEEK_SET);
    while (read(mh->fd, &e, sizeof(e)) == sizeof(e)) {
        if (e.val) {
            count++;
            if (verbosity > 0) {
                fprintf(out, "%8d %016llx %8d\n", entry, e.rid, e.val);
            }
            fs_rid_vector_append(models, e.rid);
            if (e.rid == last_model) {
                fprintf(out, "ERROR: %s model %016llx appears multiple times\n",
                        mh->filename, e.rid);
            }
            last_model = e.rid;
        }
        entry++;
    }

    if (mh->count != count) {
        fprintf(out, "ERROR: %s header count %d != scanned count %d\n",
                mh->filename, mh->count, count);
    }

    int oldlength = models->length;
    fs_rid_vector_sort(models);
    fs_rid_vector_uniq(models, 0);
    if (models->length != oldlength) {
        fprintf(out, "ERROR: %s some models appear > 1 time\n",
                mh->filename);
    }
}

int fs_mhash_count(fs_mhash *mh)
{
    return mh->count;
}

/* vi:set expandtab sts=4 sw=4: */
