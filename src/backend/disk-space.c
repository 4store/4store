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
 *  Copyright (C) 2007 Steve Harris for Garlik
 */

#include <4store-config.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#include "../common/error.h"
#include "../common/params.h"
#include "../common/4s-store-root.h"

/* returns the free disk space (in GB) remaining in the filesystem used by the
 * storage system */
float fs_free_disk_gb(const char *kb)
{
    struct statfs buf;
    char *mdfn = g_strdup_printf(fs_get_md_file_format(), kb);

    if (statfs(mdfn, &buf) == -1) {
        fs_error(LOG_ERR, "cannot statfs('%s'): %s", mdfn, strerror(errno));
        g_free(mdfn);

        return 50.0;
    }
    g_free(mdfn);

    return (double)(buf.f_bavail * buf.f_bsize) / (1024.0*1024.0*1024.0);
}

/* returns the percentage of disk space remaining in the filesystem used by the
 * storage system */
float fs_free_disk(const char *kb)
{
    struct statfs buf;
    char *mdfn = g_strdup_printf(fs_get_md_file_format(), kb);

    if (statfs(mdfn, &buf) == -1) {
        fs_error(LOG_ERR, "cannot statfs('%s'): %s", mdfn, strerror(errno));
        g_free(mdfn);

        return 50.0;
    }

    g_free(mdfn);
    float used_pc = (buf.f_blocks - buf.f_bavail) * 100.0 / buf.f_blocks;

    return 100.0 - used_pc;
}

/* vi:set expandtab sts=4 sw=4: */
