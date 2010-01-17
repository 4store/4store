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

#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "mhash.h"
#include "tbchain.h"

int main(int argc, char *argv[])
{
    int verbosity = 0;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s [--tbchain chainfile] [--dump model-rid] [-v] <mhash-file>\n", argv[0]);
        fprintf(stderr, "example: %s --tbchain /var/lib/4store/query_test_swh/0000/{mlist.tbchain,models.mhash}\n", argv[0]);

        return 1;
    }

    fs_tbchain *tbc = NULL;
    fs_rid dump = 0;
    for (int i=1; i<argc; i++) {
        if (!strcmp(argv[i], "-v")) {
            verbosity++;
            continue;
        } else if (!strcmp(argv[i], "--tbchain")) {
            tbc = fs_tbchain_open_filename(argv[i+1], O_RDONLY);
            i++;
            continue;
        } else if (!strcmp(argv[i], "--dump")) {
            dump = strtoull(argv[i+1], NULL, 16);
            i++;
            continue;
        }
        fs_mhash *mh = fs_mhash_open_filename(argv[i], 0);
        if (!mh) {
            printf("couldn't open hash\n");

            return 1;
        }
        if (dump) {
            if (!tbc) {
                printf("dump requires a tbchain\n");
                continue;
            }
	    fs_index_node mnode;
	    fs_mhash_get(mh, dump, &mnode);
	    /* that model is not in the store */
	    if (mnode == 0) {
                printf("model %016llx not found\n", dump);
	    /* it's stored in a model index file */
	    } else if (mnode == 1) {
                printf("model is in a model file, can't dump here\n");
	    /* it's stored in the model table */
	    } else {
		fs_tbchain_it *it =
		    fs_tbchain_new_iterator(tbc, FS_RID_NULL, mnode);
		fs_rid triple[3];
		while (fs_tbchain_it_next(it, triple)) {
		    printf("%016llx %016llx %016llx\n", triple[0], triple[1], triple[2]);
		}
		fs_tbchain_it_free(it);
            }
        } else {
            fs_mhash_print(mh, stdout, verbosity);
            if (tbc) {
                fs_mhash_check_chain(mh, tbc, stdout, verbosity);
                fs_tbchain_close(tbc);
            }
        }
        fs_mhash_close(mh);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
