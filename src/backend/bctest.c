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

#include "tree.h"
#include "chain.h"

#define NAME "test"

int main()
{
    fs_rid a = 0x0123456789abcdefLL;
    fs_rid b = 0xfedcba9876543210LL;
    printf("a = %016llx\n", a);
    printf("b = %016llx\n", b);
    printf("root() = %04x\n", FS_TREE_OFFSET(fs_tree_root_from_hash(a)));
    for (int i=0; i<9; i++) {
        printf("branch(%d) = %01x (%d bits)\n", i, fs_tree_branch_from_hash(a, b, i), FS_INDEX_ROOT_BITS + FS_INDEX_BRANCH_BITS*i);
    }

    printf("sizeof(fs_bucket) = %zd\n", sizeof(fs_bucket));
    printf("sizeof(fs_rid_bucket) = %zd\n", sizeof(fs_rid_bucket));
    printf("sizeof(fs_i32_bucket) = %zd\n", sizeof(fs_i32_bucket));
return 0;

    fs_backend *be = fs_backend_init("test", 0);
    fs_chain *bc = fs_chain_open(be, NAME, O_TRUNC);
    if (!bc) {
        printf("couldn't open BC\b");

        return 1;
    }
    fs_index_node node = fs_chain_new_bucket(bc);
    if (node == 0) {
        printf("couldn't get new node\n");

        return 2;
    }
    printf("using node %d\n", node);
    for (long long int i=0; i<200; i++) {
        fs_rid quad[] = {i, i+1, i+2, i+3};
        if (!(node = fs_chain_add_quad(bc, node, quad))) {
            printf("add failed at %lld\n", i);
            break;
        }
    }
    fs_chain_close(bc);
    bc = fs_chain_open(be, NAME, 0);
    node = fs_chain_new_bucket(bc);
    printf("using node %d\n", node);
    for (long long int i=0; i<24; i++) {
        fs_rid quad[] = {i+0x1000000, i+0x1000001, i+0x1000002, i+0x1000003};
        if (!(node = fs_chain_add_quad(bc, node, quad))) {
            printf("add failed at %lld\n", i);
            break;
        }
    }
    fs_chain_close(bc);
    bc = fs_chain_open(be, NAME, 0);

    fs_tree_index *trees = calloc(FS_INDEX_ROOTS, sizeof(fs_tree_index));
    printf("allocated %zdMB of tree roots\n", (FS_INDEX_ROOTS * sizeof(fs_tree_index)) / (1024*1024));

    double then = fs_time();
    int count = 0;
    static const int iters = 100000000;
    for (int loop=0; loop<2; loop++) {
        fs_rid sbase = 123456789+loop;
        for (int i=0; i<iters/2; i++) {
            if ((i % 13) == 0 || (i % 23) == 0 || (i % 100) == 0) sbase++;
            fs_rid quad[] = {0x23, sbase*5754325356371893 + 5435342532, i%(3+((i*31) % 7)) + 0x1234567100, i+0x1000000000000};
            const int tree = fs_tree_root_from_hash(quad[1]);
            const int branch = fs_tree_branch_from_hash(quad[1], quad[2], 0);
            fs_index_node tnode = trees[tree].branch[branch];
            if (fs_is_tree_ref(tnode)) {
                printf("OMGWTFBBQ\n");
                return 1;
            }
            if (tnode == 0) {
                tnode = fs_chain_new_bucket(bc);
                trees[tree].branch[branch] = tnode;
            }
            if (!(tnode = fs_chain_add_quad(bc, tnode, quad))) {
                printf("add failed at quad import %d\n", i);
                return 1;
            }
    if (++count % 10000 == 0) { printf("quad %d\r", count); fflush(stdout); }
        }
    }
    printf("quad %d\n", count);
    printf("syncing\n");
    fs_chain_sync(bc);
    double now = fs_time();
    printf("indexed %d quads at %d triples/sec\n", iters, (int)((double)iters/(now-then)));

    int freq[1000];
    memset(freq, 0, sizeof(freq));

    int total = 0;
    static const int sample_size = 5000;
    for (int i=0; i<sample_size; i++) {
        for (int j=0; j<FS_INDEX_BRANCHES; j++) {
            if (trees[i].branch[j]) {
                int size = fs_chain_length(bc, trees[i].branch[j]);
                total += size;
                if (size < 1000) {
                    freq[size]++;
                }
            } else {
                freq[0]++;
            }
        }
    }
    printf("from sample of %d quads / %d roots:\n", sample_size, total);
    double total_pc = 0.0;
    printf("%d/%d empty leaves (%d%%)\n", freq[0], FS_INDEX_ROOTS*FS_INDEX_BRANCHES, 100*freq[0]/(FS_INDEX_ROOTS*FS_INDEX_BRANCHES));
    for (int i=1; i<100; i++) {
        if (freq[i] > 0) {
            double pc = 100.0*(double)i*(double)freq[i]/(double)total;
            printf("freq(size %d) = %d (%.1f%%)\n", i, freq[i], pc);
            total_pc += pc;
        }
    }
    printf("BC has %d buckets (%zdMB)\n", bc->header->size, bc->len / (1024*1024));
    printf("%.1f%% space efficient (%.1f%% including sparse)\n", (100.0*iters*4*sizeof(fs_rid))/(bc->header->length * sizeof(fs_bucket)), (100.0*iters*4*sizeof(fs_rid))/bc->len);
    fs_chain_close(bc);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
