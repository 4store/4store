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

#include "4store.h"

int main(int argc, char *argv[])
{
  if (argc == 1) {
    fsp_backend *backend = calloc(1, sizeof (fsp_backend));
    /* server */
    fsp_serve("nulltest", backend); /* only NO-OP is implemented */
  } else {
    fsp_link *link = fsp_open_link(argv[1]);

    if (!link) {
      fprintf(stderr, "Couldn't connect to “%s”\n", argv[1]);
      return 0;
    }

    int k;

    for (k = 0; k < 3; ++k) {
      if (fsp_no_op(link, 0)) {
        fprintf(stderr, "NO-OP failed\n");
        break;
      }
    }
    if (k == 3) fprintf(stderr, "test NO-OP completed OK\n");
    fsp_close_link(link);
  }
  return 0;
}
