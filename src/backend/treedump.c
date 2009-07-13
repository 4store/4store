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

#include "tree.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <tree-file>\n", argv[0]);

        return 1;
    }

    for (int i=1; i<argc; i++) {
        fs_tree *tree = fs_tree_open_filename(NULL, NULL, argv[i], 0);
        if (!tree) {
            printf("couldn't open tree\n");

            return 1;
        }
        fs_tree_print(tree, stdout, 2);
        fs_tree_close(tree);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
