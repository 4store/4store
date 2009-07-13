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
 *  Copyright (C) 2007 Steve Harris for Garlik
 */

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "ptree.h"
#include "chain.h"

int main(int argc, char *argv[])
{
    if (argc < 4) {
        printf("Usage: %s </path/to/ptable> </path/to/ptree> <PK>\n", argv[0]);

        return 1;
    }

    fs_ptable *table = NULL;
    table = fs_ptable_open_filename(argv[1], O_RDWR);
    if (!table) {
        printf("failed to open ptable file\n");

        return 2;
    }
    fs_ptable_print(table, stdout, 0);
    printf("\n");
    fs_ptree *tree = fs_ptree_open_filename(argv[2], O_RDWR, table);
    if (!tree) {
        printf("failed to open ptree file\n");

        return 2;
    }
    errno = 0;
    fs_rid pk = strtoull(argv[3], NULL, 16);
    if (errno) {
        printf("error %d\n", errno);
        perror("stroll");

        return 1;
    }
    printf("PK %016llx\n", pk);
    fs_rid pair[2] = {FS_RID_NULL, FS_RID_NULL};
    fs_ptree_it *it = fs_ptree_search(tree, pk, pair);
    if (!it) {
        printf("no match\n");
    }
    while (it && fs_ptree_it_next(it, pair)) {
        printf("%016llx %016llx\n", pair[0], pair[2]);
    }
    fs_ptree_close(tree);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
