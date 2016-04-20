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
/*
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "list.h"
#include "lock.h"
#include "../common/error.h"
#include "../common/timing.h"
#include "../common/4s-store-root.h"

#define LIST_BUFFER_SIZE 256

/* number of rows in the chunk that will be sorted, CHUNK_SIZE has to
 * be a multiple of the page size */
#define CHUNK_SIZE (131072*4096)
/* use smaller size to test chunk sorting on small lists */
//#define CHUNK_SIZE (4096)
//#define CHUNK_SIZE (4096*1024)

enum sort_state { unsorted, chunk_sorted, sorted };

struct _fs_list {
    int fd;
    size_t width;
    off_t offset;
    char *filename;
    int buffer_pos;
    void *buffer;
    enum sort_state sort;
    int chunks;
    long long count;
    int (*sort_func)(const void *, const void *);
    off_t *chunk_pos;
    off_t *chunk_end;
    void *map;
    void *last;
};

fs_list *fs_list_open(fs_backend *be, const char *label, size_t width, int flags)
{
  char *filename = g_strdup_printf(fs_get_list_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), label);
    fs_list *l = fs_list_open_filename(filename, width, flags);
    g_free(filename);

    return l;
}

fs_list *fs_list_open_filename(const char *filename, size_t width, int flags)
{
    if (CHUNK_SIZE % width != 0) {
        fs_error(LOG_CRIT, "width of %s (%lld) does no go into %lld", filename,
                 (long long)width, (long long)CHUNK_SIZE);

        return NULL;
    }
    fs_list *l = calloc(1, sizeof(fs_list));
    l->filename = g_strdup(filename);
    l->sort = unsorted;
    l->fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (l->fd == -1) {
        fs_error(LOG_ERR, "failed to open list file '%s': %s", l->filename, strerror(errno));
        free(l);
        return NULL;
    }
    if ((flags & (O_WRONLY | O_RDWR)) && flock(l->fd, LOCK_EX) == -1) {
        fs_error(LOG_ERR, "failed to open list: %s, cannot get lock: %s", filename, strerror(errno));
        free(l);
        return NULL;
    }
    off_t end = lseek(l->fd, 0, SEEK_END);
    if (end == -1) {
        fs_error(LOG_CRIT, "failed to open list: %s, cannot seek to end", filename);
        free(l);
        return NULL;
    }
    if (end % width != 0) {
        fs_error(LOG_CRIT, "failed to open list: %s, length not multiple of data size", filename);
        free(l);
        return NULL;
    }
    l->offset = end / width;
    l->width = width;
    l->buffer_pos = 0;
    l->buffer = malloc(LIST_BUFFER_SIZE * width);
    if (l->fd == -1) {
        fs_error(LOG_CRIT, "failed to open list %s: %s", filename,
                strerror(errno));
        free(l);
        return NULL;
    }

    return l;
}

int fs_list_flush(fs_list *l)
{
    if (!l) {
        fs_error(LOG_WARNING, "tried to flush NULL list");

        return 1;
    }
    if (!l->filename) {
        fs_error(LOG_WARNING, "tried to flush list with NULL name");

        return 1;
    }
    
    off_t end = lseek(l->fd, l->offset * l->width, SEEK_SET);
    if (end == -1) {
        fs_error(LOG_ERR, "failed to seek to end of list %s: %s", l->filename,
                strerror(errno));

        return -1;
    }
    if (l->buffer_pos > 0) {
        int ret = write(l->fd, l->buffer, l->width * l->buffer_pos);
        if (ret != l->width * l->buffer_pos) {
            fs_error(LOG_ERR, "failed to write to list %s: %s", l->filename,
                    strerror(errno));

            return -1;
        }
    }

    l->buffer_pos = 0;
    l->offset = lseek(l->fd, 0, SEEK_END) / l->width;

    return 0;
}

int32_t fs_list_add(fs_list *l, const void *data)
{
    if (!l) {
        fs_error(LOG_CRIT, "tried to write to NULL list");
        return -1;
    }

    if (l->buffer_pos == LIST_BUFFER_SIZE) {
        int ret = fs_list_flush(l);
        if (ret != 0) return ret;
    }

    memcpy(l->buffer + l->buffer_pos * l->width, data, l->width);

    l->buffer_pos++;

    return l->offset + l->buffer_pos - 1;
}

int fs_list_get(fs_list *l, int32_t pos, void *data)
{
    if (!l) {
        fs_error(LOG_CRIT, "tried to read from NULL list");
        return 1;
    }

    if (pos >= l->offset) {
        /* fetch from buffer */
        if (pos >= (long int)l->offset + (long int)l->buffer_pos) {
            fs_error(LOG_CRIT, "tried to read past end of list %s, "
                     "postition %d/%d", l->filename, pos,
                     (int)l->offset + l->buffer_pos);

            return 1;
        }
        memcpy(data, l->buffer + (pos - l->offset) * l->width, l->width);

        return 0;
    }

    if (lseek(l->fd, pos * l->width, SEEK_SET) == -1) {
        fs_error(LOG_ERR, "failed to seek to position %zd in %s", pos * l->width, l->filename);

        return 1;
    }
    int ret = read(l->fd, data, l->width);
    if (ret != l->width) {
        if (ret == -1) {
            fs_error(LOG_CRIT, "failed to read %zd bytes from list %s, position %d, %s", l->width, l->filename, pos, strerror(errno));
        } else {
            fs_error(LOG_CRIT, "failed to read %zd bytes from list %s, position %d/%ld, got %d bytes", l->width, l->filename, pos, (long int)l->offset, ret);
        }

        return 1;
    }

    return 0;
}

int fs_list_length(fs_list *l)
{
    return l->offset + l->buffer_pos;
}

void fs_list_rewind(fs_list *l)
{
    lseek(l->fd, 0, SEEK_SET);
}

/* return the next item from a sorted list, uniqs as well */
int fs_list_next_sort_uniqed(fs_list *l, void *out)
{
    if (!l) {
        fprintf(out, "NULL list\n");

        return 0;
    }

    switch (l->sort) {
    case unsorted:
        fs_error(LOG_WARNING, "tried to call %s on unsorted list", __func__);
        return fs_list_next_value(l, out);

    case sorted:
        /* could use fs_list_next_value(l, out) but it will cause duplicates */
    case chunk_sorted:
        break;
    }

    /* initialise if this is the first time were called */
    if (!l->chunk_pos) {
        l->count = 0;
        l->chunks = (l->offset * l->width) / CHUNK_SIZE + 1;
        l->chunk_pos = calloc(l->chunks, sizeof(off_t));
        l->chunk_end = calloc(l->chunks, sizeof(off_t));
        for (int c=0; c<l->chunks; c++) {
            l->chunk_pos[c] = c * CHUNK_SIZE;
            l->chunk_end[c] = (c+1) * CHUNK_SIZE;
        }
        l->chunk_end[l->chunks - 1] = l->offset * l->width;
        long long int chunk_length = 0;
        for (int c=0; c<l->chunks; c++) {
            chunk_length += (l->chunk_end[c] - l->chunk_pos[c]) / l->width;
        }
        if (chunk_length != l->offset) {
            fs_error(LOG_ERR, "length(chunks) = %lld, length(list) = %lld, not sorting", chunk_length, (long long)l->offset);
            free(l->chunk_pos);
            l->chunk_pos = NULL;
            free(l->chunk_end);
            l->chunk_end = NULL;

            return 1;
        }
        l->last = calloc(1, l->width);
        l->map = mmap(NULL, l->offset * l->width, PROT_READ,
                     MAP_FILE | MAP_SHARED, l->fd, 0);
    }

again:;
    int best_c = -1;
    for (int c=0; c < l->chunks; c++) {
        if (l->chunk_pos[c] >= l->chunk_end[c]) {
            continue;
        }
        if (best_c == -1 || l->sort_func(l->map + l->chunk_pos[c],
            l->map + l->chunk_pos[best_c]) < 0) {
            best_c = c;
        }
    }
    if (best_c == -1) {
        for (int c=0; c<l->chunks; c++) {
            if (l->chunk_pos[c] != l->chunk_end[c]) {
                fs_error(LOG_ERR, "chunk %d was not sorted to end", c);
            }
        }
        if (l->count != l->offset) {
            fs_error(LOG_ERR, "failed to find low row after %lld/%lld rows", (long long)l->count, (long long)l->offset);
        }

        free(l->chunk_pos);
        l->chunk_pos = NULL;
        free(l->chunk_end);
        l->chunk_end = NULL;
        free(l->last);
        l->last = NULL;
        munmap(l->map, l->offset * l->width);
        l->map = NULL;

        return 0;
    }

    if (bcmp(l->last, l->map + l->chunk_pos[best_c], l->width) == 0) {
        /* it's a duplicate */
        l->chunk_pos[best_c] += l->width;
        (l->count)++;

        goto again;
    } else {
        memcpy(out, l->map + l->chunk_pos[best_c], l->width);
        memcpy(l->last, l->map + l->chunk_pos[best_c], l->width);
        l->chunk_pos[best_c] += l->width;
        (l->count)++;

        return 1;
    }
}

int fs_list_next_value(fs_list *l, void *out)
{
    if (!l) {
        fprintf(out, "NULL list\n");

        return 0;
    }

    int ret = read(l->fd, out, l->width);
    if (ret == -1) {
        fs_error(LOG_ERR, "error reading entry from list: %s\n", strerror(errno));
        return 0;
    } else if (ret == 0) {
        return 0;
    } else if (ret != l->width) {
        fs_error(LOG_ERR, "error reading entry from list, got %d bytes instead of %zd\n", ret, l->width);

        return 0;
    }

    return 1;
}

void fs_list_print(fs_list *l, FILE *out, int verbosity)
{
    if (!l) {
        fprintf(out, "NULL list\n");

        return;
    }

    fprintf(out, "list of %ld entries\n", (long int)(l->offset + l->buffer_pos));
    if (l->buffer_pos) {
        fprintf(out, "   (%d buffered)\n", l->buffer_pos);
    }
    fprintf(out, "  width %zd bytes\n", l->width);
    fprintf(out, "  sort state: ");
    switch (l->sort) {
    case unsorted:
        fprintf(out, "unsorted\n");
        break;
    case chunk_sorted:
        fprintf(out, "chunk sorted (%lld chunks)\n",
                (long long)(l->offset * l->width) / CHUNK_SIZE + 1);
        break;
    case sorted:
        fprintf(out, "sorted\n");
        break;
    }
    if (verbosity > 0) {
        char buffer[l->width];
        lseek(l->fd, 0, SEEK_SET);
        for (int i=0; i<l->offset; i++) {
            if (l->sort == chunk_sorted && i>0 && i % (CHUNK_SIZE/l->width) == 0) {
                fprintf(out, "--- sort chunk boundary ----\n");
            }
            memset(buffer, 0, l->width);
            int ret = read(l->fd, buffer, l->width);
            if (ret == -1) {
                fs_error(LOG_ERR, "error reading entry %d from list: %s\n", i, strerror(errno));
            } else if (ret != l->width) {
                fs_error(LOG_ERR, "error reading entry %d from list, got %d bytes instead of %zd\n", i, ret, l->width);
            }
            if (l->width % sizeof(fs_rid) == 0) {
                volatile fs_rid *row = (fs_rid *)buffer;
                fprintf(out, "%08x", i);
                for (int j=0; j<l->width / sizeof(fs_rid); j++) {
                    fprintf(out, " %016llx", row[j]);
                }
                fprintf(out, "\n");
            }
        }
    }
}

int fs_list_truncate(fs_list *l)
{
    if (ftruncate(l->fd, 0) == -1) {
        fs_error(LOG_CRIT, "failed to truncate '%s': %s", l->filename, strerror(errno));

        return 1;
    }
    l->offset = 0;
    l->buffer_pos = 0;

    return 0;
}

static int fs_list_sort_chunk(fs_list *l, off_t start, off_t length, int (*comp)(const void *, const void *))
{
    /* map the file so we can access it efficiently */
    void *map = mmap(NULL, length * l->width, PROT_READ | PROT_WRITE,
                     MAP_FILE | MAP_SHARED, l->fd, start * l->width);
    if (map == (void *)-1) {
        fs_error(LOG_ERR, "failed to map '%s', %lld+%lld for sort: %s",
                 l->filename, (long long)start * l->width,
                 (long long)length * l->width, strerror(errno));

        return 1;
    }

    qsort(map, length, l->width, comp);

    munmap(map, length * l->width);

    return 0;
}

int fs_list_sort(fs_list *l, int (*comp)(const void *, const void *))
{
    /* make sure it's flushed to disk */
    fs_list_flush(l);
    l->sort_func = comp;

    if (fs_list_sort_chunk(l, 0, l->offset, comp)) {
        return 1;
    }
    l->sort = sorted;

    return 0;
}

int fs_list_sort_chunked(fs_list *l, int (*comp)(const void *, const void *))
{
    /* make sure it's flushed to disk */
    fs_list_flush(l);
    l->sort_func = comp;

    for (int c=0; c < l->offset; c += CHUNK_SIZE/l->width) {
        off_t length = l->offset - c;
        if (length > CHUNK_SIZE/l->width) length = CHUNK_SIZE/l->width;
        int ret = fs_list_sort_chunk(l, c, length, comp);
        if (ret) {
            fs_error(LOG_ERR, "chunked sort failed at chunk %ld", c / (CHUNK_SIZE/l->width));

            return ret;
        }
    }
    if (l->offset <= CHUNK_SIZE/l->width) {
        l->sort = sorted;
    } else {
        l->sort = chunk_sorted;
    }

    return 0;
}

int fs_list_lock(fs_list *l, int action)
{
    return flock(l->fd, action);
}

int fs_list_unlink(fs_list *l)
{
    return unlink(l->filename);
}

int fs_list_close(fs_list *l)
{
    if (l->fd == -1) {
        fs_error(LOG_WARNING, "tried to close already closed list");

        return 1;
    }
    fs_list_flush(l);
    int fd = l->fd;
    l->fd = -1;
    g_free(l->filename);
    l->filename = NULL;
    free(l->buffer);
    free(l);
    flock(fd, LOCK_UN);

    return close(fd);
}

/* vi:set expandtab sts=4 sw=4: */
