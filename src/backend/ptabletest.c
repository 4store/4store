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

#include "ptable.h"

int main(int argc, char *argv[])
{
    fs_ptable *pt = fs_ptable_open_filename("/tmp/test.ptable",
                                            O_RDWR | O_CREAT | O_TRUNC);
    printf("length = %d\n", fs_ptable_length(pt));
    fs_rid pair[2] = { 1, 23 };
    fs_row_id add;

    add = fs_ptable_add_pair(pt, 0, pair);
    printf("add = %d\n", add);
    add = fs_ptable_add_pair(pt, 0, pair);
    printf("add = %d\n", add);
    printf("length = %d\n", fs_ptable_length(pt));
    for (int i=0; i<10; i++) {
        pair[1] = i;
        add = fs_ptable_add_pair(pt, add, pair);
    }
    pair[0] = 2;
    pair[2] = 24;
    fs_ptable_add_pair(pt, 0, pair);
    fs_ptable_add_pair(pt, 0, pair);
    fs_ptable_add_pair(pt, 0, pair);
    fs_ptable_add_pair(pt, 0, pair);
    fs_ptable_add_pair(pt, 0, pair);
    for (int i=10; i<20; i++) {
        pair[1] = i;
        add = fs_ptable_add_pair(pt, add, pair);
    }
    printf("length = %d\n", fs_ptable_length(pt));
    fs_ptable_print(pt, stdout, 1);
    printf("remove %d\n", add);
    fs_ptable_remove_chain(pt, add);
    fs_ptable_print(pt, stdout, 1);
    add = fs_ptable_add_pair(pt, 0, pair);
    for (int i=0; i<10; i++) {
        pair[1] = i;
        add = fs_ptable_add_pair(pt, add, pair);
    }
    fs_ptable_print(pt, stdout, 1);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
