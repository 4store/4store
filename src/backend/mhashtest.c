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
#include <stdio.h>
#include <fcntl.h>
#include <glib.h>

#include "mhash.h"

#define ITS 22000

int main(int argc, char *argv[])
{
	fs_mhash *rh = fs_mhash_open_filename("/tmp/test.mhash", O_RDWR | O_CREAT | O_TRUNC);
	fs_mhash_put(rh, 1, 23);
	fs_mhash_put(rh, 2, 93);
	fs_mhash_put(rh, 23, 101);
	fs_index_node val;
	fs_mhash_get(rh, 1, &val);
	printf("GOT 23 -> %d\n", val);
	double then = fs_time();
	for (int i=0; i<ITS; i++) {
		fs_rid rid = i * 6556708946546543 + 23;
		if (fs_mhash_put(rh, rid, i)) {
			printf("error @ %d\n", i);

			return 1;
		}
	}
	double now = fs_time();
	printf("wrote model entries, %f models/s\n", (double)ITS/(now-then));
	then = fs_time();
	for (int i=0; i<ITS; i++) {
		fs_rid rid = i * 6556708946546543 + 23;
		if (fs_mhash_get(rh, rid, &val)) {
			printf("error @ %d\n", i);

			return 1;
		}
		if (val != i) {
			printf("error @ %d (%llx), got %d\n", i, rid, val);

			return 2;
		}
	}
	now = fs_time();
	printf("read resources, %f res/s\n", (double)ITS/(now-then));
	fs_mhash_close(rh);

	return 0;
}
