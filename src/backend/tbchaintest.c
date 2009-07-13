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
 *  Copyright (C) 2008 Steve Harris for Garlik
 */

#include <fcntl.h>
#include <stdio.h>
#include <glib.h>
#include <time.h>

#include "tbchain.h"

#define INCT triple[0]++; triple[1]++; triple[2]++
#define REPS 2700000

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);

        return 1;
    }

    fs_tbchain *tbc = fs_tbchain_open_filename(argv[1], O_CREAT | O_TRUNC | O_RDWR);
    fs_rid triple[3] = { 1, 1, 1 };
    fs_index_node c1 = fs_tbchain_new_chain(tbc);
    fs_index_node c2 = fs_tbchain_new_chain(tbc);
    for (int i=0; i<44; i++) {
        c1 = fs_tbchain_add_triple(tbc, c1, triple); INCT;
    }
    c2 = fs_tbchain_add_triple(tbc, c2, triple); INCT;
    c2 = fs_tbchain_add_triple(tbc, c2, triple); INCT;
    c2 = fs_tbchain_add_triple(tbc, c2, triple); INCT;
    c2 = fs_tbchain_add_triple(tbc, c2, triple); INCT;
    c2 = fs_tbchain_add_triple(tbc, c2, triple); INCT;
    for (int i=0; i<44; i++) {
        c1 = fs_tbchain_add_triple(tbc, c1, triple); INCT;
    }
    fs_tbchain_print(tbc, stdout, 1);
    printf("length(c1) = %d\n", fs_tbchain_length(tbc, c1));
    printf("length(c2) = %u\n", fs_tbchain_length(tbc, c2));
    uint64_t chains[16] = { c1, c2 };
    for (int i=2; i<16; i++) {
        chains[i] = fs_tbchain_new_chain(tbc);
    }
    double then = fs_time();
    long long int sum = 0;
    for (int i=0; i<REPS; i++) {
        if (i % 129 == 128) {
            sum += fs_tbchain_length(tbc, chains[2]);
            chains[2] = fs_tbchain_new_chain(tbc);
        }
        if (i % 139 == 138) {
            sum += fs_tbchain_length(tbc, chains[3]);
            chains[3] = fs_tbchain_new_chain(tbc);
        }
        if (i % 1319 == 1318) {
            sum += fs_tbchain_length(tbc, chains[4]);
            chains[4] = fs_tbchain_new_chain(tbc);
        }
        if (i % 2319 == 2318) {
            sum += fs_tbchain_length(tbc, chains[5]);
            chains[5] = fs_tbchain_new_chain(tbc);
        }
        if (i % 23 == 22) {
            sum += fs_tbchain_length(tbc, chains[6]);
            chains[6] = fs_tbchain_new_chain(tbc);
        }
        if (i % 5 == 4) {
            sum += fs_tbchain_length(tbc, chains[7]);
            chains[7] = fs_tbchain_new_chain(tbc);
        }
        int dontcare = fs_tbchain_add_triple(tbc, chains[(i * 37) & 15], triple); INCT;
	dontcare++; // please be quiet mr. compiler
    }
    double now = fs_time();
    printf("took %f seconds to add %d triples, %f T/s\n", now-then, REPS, REPS/(now-then));
    for (int i=0; i<16; i++) {
        printf("length(c[%d]) = %u\n", i, fs_tbchain_length(tbc, chains[i]));
        sum += fs_tbchain_length(tbc, chains[i]);
    }
    printf("length(all) = %lld\n", sum);
    printf("sum should be %d\n", REPS+88+5);
    printf("allocated blocks = %u\n", fs_tbchain_allocated_blocks(tbc));
    printf("bytes / triple = %f\n", (512.0+fs_tbchain_allocated_blocks(tbc)*128.0) / (double)sum);
    fs_tbchain_close(tbc);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
