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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <glib.h>
#include <libgen.h>

#include "../backend/backend.h"
#include "../backend/backend-intl.h"
#include "../common/gnu-options.h"

int main(int argc, char *argv[])
{
    fs_gnu_options(argc, argv, "<kbname>\n");

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <kbname>\n", basename(argv[0]));

        return 1;
    }

    const char *kbname = argv[1];

    fs_backend *be = fs_backend_init(kbname, 0);
    if (!be) {
        return 1;
    }

    int segments[FS_MAX_SEGMENTS];
    int num_segments;
    num_segments = fs_segments(be, segments);
    char *tmp = g_strdup_printf("du -hs "FS_KB_DIR" | sed 's/.\\/var.*/ disk usage/; s/^/  /'", kbname);
    system(tmp);
    g_free(tmp);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
