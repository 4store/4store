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
#include <glib.h>

#include "ptree.h"
#include "ptable.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);

        return 1;
    }

    char *tbl = g_strdup_printf("%s.tbl", argv[1]);

    fs_ptable *ptbl = fs_ptable_open_filename(tbl, O_CREAT | O_TRUNC | O_RDWR);
    if (!ptbl) {
        printf("failed to create ptable file\n");

        return 1;
    }
    fs_ptree *pt = fs_ptree_open_filename(argv[1], O_CREAT | O_TRUNC | O_RDWR, ptbl);
    if (!pt) {
        printf("failed to create ptree file\n");

        return 1;
    }
    fs_ptree_add(pt, 0x223456789abcdef0LL, NULL, 0);
    fs_ptable_close(ptbl);
    fs_ptree_close(pt);

    ptbl = fs_ptable_open_filename(tbl, O_RDWR);
    pt = fs_ptree_open_filename(argv[1], O_RDWR, ptbl);
    if (!pt) {
        printf("failed to reopen ptree file\n");

        return 1;
    }
    fs_ptree_add(pt, 0x123456789abcdef0LL, NULL, 0);
    fs_ptree_add(pt, 0x123456789abcdef0LL, NULL, 0);
    fs_ptree_add(pt, 0x1234567a9abcdef0LL, NULL, 0);
    fs_ptree_add(pt, 0x123456789abcdef0LL, NULL, 0);
    fs_ptree_add(pt, 0x1234567a9abcdef1LL, NULL, 0);
    fs_ptree_add(pt, 0x3234567a9abcdef1LL, NULL, 0);

    fs_rid s = 0x5555555555555555LL;
    fs_rid g = 0x0123456789abcdefLL;

    #define ITS 10000000

    double then = fs_time();
    for (int i=0; i<ITS; i++) {
        if (i % 10 == 0) s += 984354325432534976LL;
        fs_rid pair[2] = { g, s+i };
        fs_ptree_add(pt, s, pair, 0);
    }
    double now = fs_time();
    printf("imported %d rows/s\n", (int)(ITS/(now-then)));
#if 0
    fs_ptree_print(pt, stdout);
    printf("\n");
    fs_chain_print(ch, stdout, 2);
#endif
    fs_ptree_close(pt);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
