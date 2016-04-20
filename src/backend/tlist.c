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
#include <sys/file.h>
#include <sys/stat.h>

#include "tlist.h"
#include "lock.h"
#include "../common/error.h"
#include "../common/timing.h"
#include "../common/4s-store-root.h"

#define LIST_BUFFER_SIZE 256

/* the width of a single row (in bytes) */
#define WIDTH     (sizeof(fs_rid) * 3)

#define HEADER    512

#define TLIST_ID  0x4a585530    /* JXT0 */

struct tlist_header {
    int32_t id;
    int32_t superset;
    int64_t length;
    char padding[496];
};

struct _fs_tlist {
    int flags;
    int fd;
    int64_t offset;
    char *filename;
    int buffer_pos;
    int superset;
    void *buffer;
};

static int write_header(fs_tlist *l)
{
    struct tlist_header header;
    memset(&header, 0, sizeof(header));
    header.id = TLIST_ID;
    header.length = l->offset;
    lseek(l->fd, 0, SEEK_SET);
    write(l->fd, &header, HEADER);

    return 0;
}

fs_tlist *fs_tlist_open(fs_backend *be, fs_rid model, int flags)
{
    fs_tlist *l;

    if (fs_backend_model_dirs(be)) {
        char hash[17];
        sprintf(hash, "%016llx", model);
        hash[16] = '\0';
        char *dirname = g_strdup_printf(fs_get_tlist_dird_format(), fs_backend_get_kb(be),
                                        fs_backend_get_segment(be), hash[0],
                                        hash[1], hash[2], hash[3]);
        struct stat buf;
        int ret = stat(dirname, &buf);
        if (ret) mkdir(dirname, FS_FILE_MODE + 0100);
        char *filename = g_strdup_printf(fs_get_tlist_dir_format(), fs_backend_get_kb(be),
                                         fs_backend_get_segment(be),
                                         hash[0], hash[1],
                                         hash[2], hash[3], hash+4);
        l = fs_tlist_open_filename(filename, flags);
        g_free(filename);
    } else {
	char *filename = g_strdup_printf(fs_get_tlist_format(), fs_backend_get_kb(be),
                                         fs_backend_get_segment(be), model);
        l = fs_tlist_open_filename(filename, flags);
        g_free(filename);
    }

    return l;
}

fs_tlist *fs_tlist_open_filename(const char *filename, int flags)
{
    if (sizeof(struct tlist_header) != HEADER) {
        fs_error(LOG_CRIT, "tlist header size != 512");
    
        return NULL;
    }

    struct tlist_header header;
    memset(&header, 0, sizeof(header));
    fs_tlist *l = calloc(1, sizeof(fs_tlist));
    l->fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (l->fd == -1) {
        free(l);
        if (flags != O_RDONLY || errno != ENOENT) {
            fs_error(LOG_ERR, "failed to open list file '%s': %s", filename, strerror(errno));
        }

        return NULL;
    }
    if (flags & (O_TRUNC)) {
        l->offset = 0;
    } else {
        lseek(l->fd, 0, SEEK_SET);
        int length = read(l->fd, &header, HEADER);
        if (length == HEADER && header.id != TLIST_ID) {
            fs_error(LOG_ERR, "“%s” does not appear to be a tlist", filename);
            close(l->fd);
            free(l);
            return NULL;
        }
        l->offset = header.length;
        l->superset = header.superset;
    }
    l->filename = g_strdup(filename);
    l->flags = flags;
    if ((flags & (O_WRONLY | O_RDWR)) && flock(l->fd, LOCK_EX) == -1) {
        fs_error(LOG_ERR, "failed to open list: %s, cannot get lock: %s", filename, strerror(errno));
        close(l->fd);
        free(l);
        return NULL;
    }
    write_header(l);
    l->buffer_pos = 0;
    l->buffer = malloc(LIST_BUFFER_SIZE * WIDTH);
    if (l->fd == -1) {
        fs_error(LOG_CRIT, "failed to open list %s: %s", filename,
                strerror(errno));
        free(l);
        return NULL;
    }

    return l;
}

int fs_tlist_flush(fs_tlist *l)
{
    if (!l) {
        fs_error(LOG_ERR, "tried to flush NULL tlist");

        return 1;
    }

    off_t end = lseek(l->fd, 0, SEEK_END);
    if (end == -1) {
        fs_error(LOG_ERR, "failed to seek to end of list %s: %s", l->filename,
                strerror(errno));

        return -1;
    }
    if (l->buffer_pos > 0) {
        int ret = write(l->fd, l->buffer, WIDTH * l->buffer_pos);
        if (ret != WIDTH * l->buffer_pos) {
            fs_error(LOG_ERR, "failed to write to list %s: %s", l->filename,
                    strerror(errno));

            return -1;
        }
    }

    l->offset += l->buffer_pos;
    l->buffer_pos = 0;
    write_header(l);

    return 0;
}

int64_t fs_tlist_add(fs_tlist *l, fs_rid data[4])
{
    if (!l) {
        fs_error(LOG_CRIT, "tried to write to NULL list");
        return -1;
    }

    if (l->buffer_pos == LIST_BUFFER_SIZE) {
        int ret = fs_tlist_flush(l);
        if (ret != 0) return ret;
    }

    memcpy(l->buffer + l->buffer_pos * WIDTH, data, WIDTH);

    l->buffer_pos++;

    return l->offset + l->buffer_pos - 1;
}

int fs_tlist_length(fs_tlist *l)
{
    return l->offset + l->buffer_pos;
}

void fs_tlist_rewind(fs_tlist *l)
{
    lseek(l->fd, sizeof(struct tlist_header), SEEK_SET);
}

int fs_tlist_next_value(fs_tlist *l, void *out)
{
    if (!l) {
        fs_error(LOG_ERR, "cannot read from null list");

        return 0;
    }

    int ret = read(l->fd, out, WIDTH);
    if (ret == -1) {
        fs_error(LOG_ERR, "error reading entry from list: %s\n", strerror(errno));
        return 0;
    } else if (ret == 0) {
        return 0;
    } else if (ret != WIDTH) {
        fs_error(LOG_ERR, "error reading entry from list, got %d bytes instead of %zd\n", ret, WIDTH);

        return 0;
    }

    return 1;
}

void fs_tlist_print(fs_tlist *l, FILE *out, int verbosity)
{
    if (!l) {
        fprintf(out, "NULL list\n");

        return;
    }

    fprintf(out, "list of %ld entries\n", (long int)(l->offset + l->buffer_pos));
    if (l->buffer_pos) {
        fprintf(out, "   (%d buffered)\n", l->buffer_pos);
    }
    if (verbosity > 0) {
        char buffer[WIDTH];
        lseek(l->fd, sizeof(struct tlist_header), SEEK_SET);
        for (int i=0; i<l->offset; i++) {
            memset(buffer, 0, WIDTH);
            int ret = read(l->fd, buffer, WIDTH);
            if (ret == -1) {
                fs_error(LOG_ERR, "error reading entry %d from list: %s\n", i, strerror(errno));
            } else if (ret != WIDTH) {
                fs_error(LOG_ERR, "error reading entry %d from list, got %d bytes instead of %zd\n", i, ret, WIDTH);
            }
            volatile fs_rid *row = (fs_rid *)buffer;
            printf("%08x", i);
            for (int j=0; j<WIDTH / sizeof(fs_rid); j++) {
                printf(" %016llx", row[j]);
            }
            printf("\n");
        }
    }
}

int fs_tlist_truncate(fs_tlist *l)
{
    if (ftruncate(l->fd, sizeof(struct tlist_header)) == -1) {
        fs_error(LOG_CRIT, "failed to truncate '%s': %s", l->filename, strerror(errno));

        return 1;
    }
    l->offset = 0;
    l->buffer_pos = 0;

    return 0;
}

int fs_tlist_lock(fs_tlist *l, int action)
{
    return flock(l->fd, action);
}

int fs_tlist_unlink(fs_tlist *l)
{
    return unlink(l->filename);
}

int fs_tlist_close(fs_tlist *l)
{
    if (!l) {
        fs_error(LOG_ERR, "tried to close NULL tlist");

        return 1;
    }
    fs_tlist_flush(l);
    
    int fd = l->fd;
    g_free(l->filename);
    free(l->buffer);
    free(l);
    flock(fd, LOCK_UN);

    return close(fd);
}

/* vi:set expandtab sts=4 sw=4: */
