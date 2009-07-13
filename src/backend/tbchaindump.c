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

#include "tbchain.h"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        printf("Usage: %s <tbchain> ...\n", argv[0]);

        return 1;
    }

    for (int i=1; i<argc; i++) {
        fs_tbchain *tbc = fs_tbchain_open_filename(argv[i], O_RDONLY);
        if (!tbc) {
            printf("failed to open tbchain file\n");

            return 2;
        }
        fs_tbchain_print(tbc, stdout, 1);
        printf("\n");
        fs_tbchain_close(tbc);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
