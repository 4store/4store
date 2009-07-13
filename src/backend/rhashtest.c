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
#include <unistd.h>
#include <glib.h>
#include <sys/types.h>

#include "rhash.h"

#define ITS 1200000

#define BIG_PRIME 275604541


int main(int argc, char *argv[])
{
	char *file = g_strdup_printf("/tmp/test-%d.rhash", getpid());
	fs_rhash *rh = fs_rhash_open_filename(file, O_RDWR | O_CREAT | O_TRUNC);
	fs_resource res;
	char *strings[] = { "foo", "bar", "qwertyuiopasdf",
			    "http://example.org/foo/bar",
			    "http://example.org/foo#bar" };
	fs_resource r1 = { 23, "foo", 0 };
	fs_resource r2 = { 23+262144, "bar", 0 };
	fs_rhash_put(rh, &r1);
	fs_rhash_put(rh, &r2);
	res.rid = 23;
	fs_rhash_get(rh, &res);
	printf("GOT %llx -> %s\n", res.rid, res.lex);
	double then = fs_time();
	for (int i=0; i<ITS; i++) {
		r1.rid = i * BIG_PRIME + 23;
		r1.lex = strings[i % 5];
		if (fs_rhash_put(rh, &r1)) {
			printf("error @ %d\n", i);

			return 1;
		}
	}
	double now = fs_time();
	printf("wrote resources, %f res/s\n", (double)ITS/(now-then));
	then = fs_time();
	int errors = 0;
	for (int i=0; i<ITS; i++) {
		r1.rid = i * BIG_PRIME + 23;
		if (fs_rhash_get(rh, &r1)) {
			printf("error @ %d\n", i);
			errors++;
		}
		g_free(r1.lex);
		if (errors == 10) {
			printf("skipping further reads...\n");
			fs_rhash_print(rh, stdout, 0);
			fs_rhash_close(rh);

			return 1;
		}
	}
	now = fs_time();
	printf("read resources, %f res/s\n", (double)ITS/(now-then));
	if (errors) {
		printf("%d/%d read errors\n", errors, ITS);
	}
	fs_rhash_print(rh, stdout, 0);
	fs_rhash_close(rh);
	unlink(file);

	return 0;
}
