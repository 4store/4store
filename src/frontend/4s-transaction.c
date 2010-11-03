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
#include <stdarg.h>
#include <stdlib.h>
#include <libgen.h>
#include <string.h>
#include <glib.h>
#include <rasqal.h>

#include "query-datatypes.h"
#include "../common/4store.h"
#include "../common/datatypes.h"
#include "../common/params.h"
#include "../common/hash.h"
#include "../common/error.h"

int main(int argc, char *argv[])
{
    char *password = fsp_argv_password(&argc, argv);

    if (argc != 3 || !strcmp(argv[2], "-h")) {
      fprintf(stderr, "Usage: %s kbname <begin|rollback|commit>\n", basename(argv[0]));

      return 1;
    }

    fsp_syslog_enable();
    fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RW);

    if (!link) {
      fprintf(stderr, "couldn't connect to “%s”\n", argv[1]);

      return 2;
    }

    if (fsp_no_op(link, 0)) {
      fprintf(stderr, "NO-OP failed\n");

      return 1;
    }

    int ret = 0;

    if (!strcmp(argv[2], "begin")) {
	fsp_transaction_begin_all(link);
    } else if (!strcmp(argv[2], "rollback")) {
	fsp_transaction_rollback_all(link);
    } else if (!strcmp(argv[2], "precommit")) {
	/* this is an undocumented "feature", don't use it unless you know
	 * what it means, it's only really for benchmark timing */
	ret = fsp_lock(link);
	if (ret) {
	    fs_error(LOG_CRIT, "failed to get commit lock for '%s'", argv[1]);
	}
	if (!ret) ret = fsp_transaction_pre_commit_all(link);
    } else if (!strcmp(argv[2], "commit")) {
	ret = fsp_lock(link);
	if (ret) {
	    fs_error(LOG_CRIT, "failed to get commit lock for '%s'", argv[1]);
	}
	if (!ret) ret = fsp_transaction_pre_commit_all(link);
	if (!ret) ret = fsp_transaction_commit_all(link);
    } else {
	fprintf(stderr, "bad argument, expected “begin”, “rollback” or “commit”\n");

	return 3;
    }

    fsp_close_link(link);

    return ret;
}
