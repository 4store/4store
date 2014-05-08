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

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <glib.h>
#include <sys/stat.h>
#include <sys/file.h>

#include "lock.h"
#include "../common/error.h"
#include "../common/4s-store-root.h"

int fs_lock_kb(const char *kb)
{
    char *fn = g_strdup_printf(fs_get_md_file_format(), kb);
    int fd = open(fn, FS_O_NOATIME | O_RDONLY | O_CREAT, 0600);
    if (fd == -1) {
        fs_error(LOG_CRIT, "failed to open metadata file %s for locking: %s",
                 fn, strerror(errno));

        return 1;
    }
    g_free(fn);
    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
	    fs_error(LOG_ERR, "cannot get lock for kb “%s”", kb);

            return 1;
        }
        fs_error(LOG_ERR, "failed to get lock: %s", strerror(errno));

        return 1;
    }

    return 0;
}           

int fs_lock(fs_backend *be, const char *name, fs_lock_action action, int block)
{
    char *fn = g_strdup_printf(fs_get_file_lock_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), name);
    int ret = -1;
    int fd;

    switch (action) {
    case FS_LOCK_SHARED:
        fs_error(LOG_CRIT, "shared locks not implemented");
        break;

    case FS_LOCK_EXCLUSIVE:
        fd = open(fn, FS_O_NOATIME | O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd == -1 && errno == EEXIST) {
            if (block) {
                fs_error(LOG_CRIT, "blocking locks not supported");
                close(fd);

                break;
            }
            ret = 1;
            close(fd);

            break;
        }
        ret = 0;
        close(fd);

        break;

    case FS_LOCK_RELEASE:
        unlink(fn);
        ret = 0;

        break;
    }

    g_free(fn);

    return ret;
}

int fs_lock_taken(fs_backend *be, const char *name)
{
    struct stat junk;
    char *fn = g_strdup_printf(fs_get_file_lock_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), name);
    int ret = stat(fn, &junk);
    g_free(fn);
    if (ret == -1 && errno == ENOENT) {
        return 0;
    }

    return 1;
}

int fs_flock_logged(int fd, int op, const char *file, int line)
{
    char opstr[8] = { 0, 0, 0, 0, 0 };
    if (op & LOCK_SH) strcat(opstr, "s");
    else strcat(opstr, "-");
    if (op & LOCK_EX) strcat(opstr, "e");
    else strcat(opstr, "-");
    if (op & LOCK_NB) strcat(opstr, "n");
    else strcat(opstr, "-");
    if (op & LOCK_UN) strcat(opstr, "u");
    else strcat(opstr, "-");
    
    printf("@@L %s %d:%d\t%s:%d\n", opstr, getpid(), fd, file, line);

    return flock(fd, op);
}

/* vi:set expandtab sts=4 sw=4: */
