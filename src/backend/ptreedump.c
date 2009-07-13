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

#include "ptree.h"
#include "chain.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf("Usage: %s [--check-leaks] [--table /path/to/ptable] <ptree> ...\n", argv[0]);
        printf("       to use --check-leaks you need to pass all the trees that refer to the table\n       as arguments\n");

        return 1;
    }

    fs_ptable *table = NULL;
    int check_leaks = 0;
    for (int i=1; i<argc; i++) {
        if (!strcmp(argv[i], "--check-leaks")) {
            check_leaks = 1;

            continue;
        } else if (!strcmp(argv[i], "--table")) {
            if (i >= argc-1) {
                fprintf(stderr, "--table requires argument\n");

                return 1;
            }
            i++;
            table = fs_ptable_open_filename(argv[i], O_RDWR);
            fs_ptable_print(table, stdout, 0);
            printf("\n");

            continue;
        }
        fs_ptree *tree = fs_ptree_open_filename(argv[i], O_RDWR, table);
        if (!tree) {
            printf("failed to open ptree file\n");

            return 2;
        }
        fs_ptree_print(tree, stdout, 0);
        printf("\n");
        fs_ptree_close(tree);
    }
    if (table && check_leaks) {
        fs_ptable_check_leaks(table, stdout);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
