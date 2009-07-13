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
#include <errno.h>

#include "tree.h"
#include "tree-intl.h"
#include "chain.h"

#define NAME "test"

#define CACHE_SIZE 256

#define GUARD_SIZE 12

int main()
{
    fs_backend *be = fs_backend_init("test", 0);
    fs_tree *tree_sp = fs_tree_open(be, "treesp", O_CREAT | O_TRUNC);
    fs_tree *tree_op = fs_tree_open(be, "treeop", O_CREAT | O_TRUNC);

    printf("sizeof(s th) = %zd\n", sizeof(struct tree_header));
    printf("...\n");
    double then = fs_time();
    int count = 0;
    fs_rid quad[4];
    FILE *data = fopen("test/tiger.dump", "r");
    if (!data) {
        printf("cannot open data file: %s\n", strerror(errno));
    }
    int ret;
    while ((ret = fscanf(data, "%llx %llx %llx %llx\n", quad, quad+1, quad+2, quad+3)) == 4) {
        fs_index_node n;
        n = fs_tree_add_quad(tree_sp, quad[1], quad[2], quad);
        if (n == 0) { printf("got node 0\n"); exit(1); }
        n = fs_tree_add_quad(tree_op, quad[3], quad[2], quad);
        if (n == 0) exit(1);
        if (++count % 10000 == 0) {
            printf("quad %d\r", count);
            fflush(stdout);
        }
    }
    fclose(data);
    
    printf("quad %d\n", count);
    printf("syncing\n");
    fs_tree_sync(tree_sp);
    fs_tree_sync(tree_op);
    double now = fs_time();
    printf("indexed %d quads at %d triples/sec\n", count, (int)((double)count/(now-then)));
    fs_tree_print(tree_sp, stdout, 0);
    printf("\n");
    fs_tree_print(tree_op, stdout, 0);
    fs_tree_close(tree_sp);
    fs_tree_close(tree_op);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
