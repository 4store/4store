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
 *  Copyright 2006 Nick Lamb for Garlik.com
 */

#include <stdio.h>
#include <stdlib.h>

#include "../common/4store.h"
#include "../common/server.h"
#include "../common/error.h"

int main(int argc, char *argv[])
{
  char *password = fsp_argv_password(&argc, argv);

  if (argc != 3) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
    fprintf(stderr, "Usage: %s <kbname> <TriX file>\n", argv[0]);
    exit(1);
  }

  fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RW);

  if (!link) {
    fs_error (LOG_ERR, "couldn't connect to “%s”", argv[1]);
    exit(2);
  }

  fs_hash_init(fsp_hash_type(link));
  const int segments = fsp_link_segments(link);
  for (fs_segment s = 0; s < segments; ++s) {
    fs_data_size sz;
    if (fsp_get_data_size(link, s, &sz)) {
      fs_error (LOG_ERR, "problem with size information for segment %d", s);
    } else if (sz.quads_s || sz.quads_o || sz.resources) {
      fs_error (LOG_ERR, "segment %d is not empty, restore aborted", s);
      exit(3);
    } 
  }

  fsp_start_import_all(link);
  // restore_file(link, argv[2]); - removed by swh on 2007-04-03 to get rid of anoying warning
  fsp_stop_import_all(link);

  fsp_close_link(link);
}

