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

#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include <stdlib.h>
#include <stdio.h>

#define HIST_SIZE 8192
#define EX_SIZE   4

int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <lex-file>\n", basename(argv[0]));

        return 1;
    }

    int size;
    char term;
    int histogram[HIST_SIZE];
    char *examples[EX_SIZE][HIST_SIZE];

    for (int i=0; i<HIST_SIZE; i++) {
        histogram[i] = 0;
    }

    int count = 0;
    for (int i=1; i<argc; i++) {
        int fd = open(argv[i], O_RDONLY, 0);
        off_t pos = 0;
        off_t length = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        while (pos < length) {
            read(fd, &size, sizeof(size));
            if (size < 0) {
                fprintf(stderr, "error at byte %ld, found negative length\n", (long int)pos);

                return 2;
            }
            if (size > 1000) {
                fprintf(stderr, "warning at byte %ld, found length of %d\n", (long int)pos, size);
            }
            pos += sizeof(size);
            if (size < HIST_SIZE) {
                histogram[size]++;
                for (int j=0; j<EX_SIZE; j++) {
                    if (!examples[j][size]) {
                        examples[j][size] = calloc(1, size+1);
                        read(fd, examples[j][size], size);
                        lseek(fd, pos, SEEK_SET);
                        break;
                    }
                }
            }
            pos = lseek(fd, size, SEEK_CUR);
            read(fd, &term, sizeof(term));
            if (term != '\0') {
                fprintf(stderr, "error at byte %ld, missing NUL, found 0x%02x instead\n", (long int)pos, term);

                return 2;
            }
            pos++;
            count++;
        }
    }

    printf("# found %d lexical entries\n", count);
    printf("# len\tfreq\n");
    for (int i=0; i<HIST_SIZE; i++) {
        if (histogram[i] > 0) printf("%d\t%d\t\n", i, histogram[i]);
        //if (histogram[i] > 0) printf("%d\t%d\t# %s\n", i, histogram[i], examples[1][i] ? examples[1][i] : examples[0][i]);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
