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
 *  Copyright (C) 2006 Nick Lamb and Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <rasqal.h>
#include <math.h>

#include "query-datatypes.h"
#include "../common/4store.h"
#include "../common/params.h"
#include "../common/error.h"
#include "../common/gnu-options.h"

#define CONST_2to63 9223372036854775808.0

int main(int argc, char *argv[])
{
    fs_gnu_options(argc, argv, "<kbname>\n");

    char *password = fsp_argv_password(&argc, argv);

    if (argc != 2) {
      fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
      fprintf(stderr, "Usage: %s <kbname>\n", argv[0]);
      return 1;
    }

    fsp_syslog_enable();

    fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RO);

    if (!link) {

      fs_error(LOG_ERR, "couldn't connect to “%s”", argv[1]);
      return 2;
    }

    if (fsp_no_op(link, 0)) {
      fs_error(LOG_ERR, "NO-OP failed\n");
      return 3;
    }

    fs_data_size total = {0, 0, 0, 0, 0, 0};
    fs_data_size sz;
    if (fsp_get_data_size(link, 0, &sz)) {
	fs_error(LOG_ERR, "cannot get size information");

	exit(2);
    }

    printf("%5s%12s%12s%12s%12s\n", "seg","quads (s)","quads (sr)",
           "models", "resources");

    const int segments = fsp_link_segments(link);
    for (fs_segment seg = 0; seg < segments; ++seg) {
        fs_data_size sz;
	int ret = fsp_get_data_size(link, seg, &sz);
	if (ret) {
           printf("%5d -- problem obtaining size information --\n", seg);
	} else {
           printf("%5d%12lld%+12lld%12lld%12lld\n", seg,
                  sz.quads_s, sz.quads_sr - sz.quads_s,
                  sz.models_s, sz.resources);
	   total.quads_s += sz.quads_s;
	   total.quads_sr += sz.quads_sr;
           if (sz.models_s > total.models_s) total.models_s = sz.models_s;
	   total.resources += sz.resources;
	}
   }
    printf("\n");
    printf("%5s%12lld%+12lld%12lld%12lld\n",
           "TOTAL", total.quads_s, (total.quads_sr - total.quads_s),
           total.models_s, total.resources);
    printf("\ncollision probability ≅ %.2Lf%%\n", 100.0 * (1.0 - expl(-(total.resources * (total.resources-1.0) / (2.0 * CONST_2to63)))));

    fsp_close_link(link);
}

/* vi:set expandtab sts=4 sw=4: */
