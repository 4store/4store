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

    Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <rasqal.h>

#include "query-datatypes.h"
#include "common/4store.h"
#include "common/datatypes.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"

int main(int argc, char *argv[])
{
    char *password = fsp_argv_password(&argc, argv);

    if (argc < 3) {
      fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
      fprintf(stderr, "Usage: %s <kbname> <--all|model-uri ...>\n", argv[0]);

      return 1;
    }

    fsp_syslog_enable();

    fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RW);

    if (!link) {
      fs_error(LOG_ERR, "couldn't connect to “%s”", argv[1]);

      return 2;
    }

    fs_hash_init(fsp_hash_type(link));

    if (fsp_no_op(link, 0)) {
      fprintf(stderr, "NO-OP failed\n");

      return 1;
    }

    fs_rid model;
    fs_rid_vector *mvec = fs_rid_vector_new(0);

    for (int i=2; i<argc; i++) {
	if (i == 2 && !strcmp(argv[i], "--all")) {
	    //printf("deleting all models\n");
	    fs_rid_vector_append(mvec, FS_RID_NULL);
	    break;
	} else {
	    model = fs_hash_uri(argv[i]);
	    fs_rid_vector_append(mvec, model);
	}
    }
    fsp_delete_model_all(link, mvec);

    fsp_close_link(link);

    return 0;
}
