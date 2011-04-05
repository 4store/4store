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
#include <libgen.h>

#include "query.h"
#include "query-datatypes.h"
#include "optimiser.h"
#include "../common/4store.h"
#include "../common/params.h"
#include "../common/error.h"
#include "../common/rdf-constants.h"
#include "../common/gnu-options.h"

int main(int argc, char *argv[])
{
    fs_gnu_options(argc, argv, "<kbname> <noop|freq>\n");

    char *password = fsp_argv_password(&argc, argv);

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <kbname> <noop|freq>\n", basename(argv[0]));
        return 1;
    }

    fsp_syslog_enable();

    fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RO);

    if (!link) {
        fs_error(LOG_ERR, "couldn't connect to “%s”", argv[1]);
        return 2;
    }

    double then = fs_time();
    if (fsp_no_op(link, 0)) {
      fs_error(LOG_ERR, "NO-OP failed\n");
      return 3;
    }
    double now = fs_time();

    if (!strcmp(argv[2], "noop")) {
        printf("NO-OP took %fs\n", now-then);

        return 0;
    } else if (!strcmp(argv[2], "freq")) {
        fs_query_state *qs = fs_query_init(link, NULL, NULL);
        fs_optimiser_freq_print(qs);
    }

    fsp_close_link(link);
}

/* vi:set expandtab sts=4 sw=4: */
