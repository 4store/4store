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

    Copyright (C) 2012 Manuel Salvadores
*/

#include <stdio.h>
#include <stdlib.h>

#include "4s-hash.h"
#include "4s-datatypes.h"

int main()
{
	fs_hash_init(FS_HASH_UMAC);

	double then = fs_time();
    fs_rid_set *set = fs_rid_set_new();
   // int entries = 2048 * 10;
    int entries = 10;
    for (int i=1; i <= entries; i++) {
        fs_rid_set_add(set, i);
          if ((i % 2 == 0) || (i % 5 == 0))
          fs_rid_set_add(set, i);
    }
/*
    fs_rid_set_add(set, 4095);
    fs_rid_set_add(set, 4094);
    printf("contains(%d) -> %d\n",4095,fs_rid_set_contains(set, 4095));
    printf("contains(%d) -> %d\n",4094,fs_rid_set_contains(set, 4094));

    return 0;
    for (int i=0; i < entries; i++) {
        if (!fs_rid_set_contains(set, i))
            printf("%d not there!\n",i);
    }
    for (int i=entries; i < 100 * entries; i++) {
        if (fs_rid_set_contains(set, i))
            printf("%d there!\n",i);
    }
*/
	double now = fs_time();

    fs_rid d = FS_RID_NULL;
    printf("scanning\n");
    fs_rid_set_rewind(set);
    int count =0;
    while ((d = fs_rid_set_next(set)) != FS_RID_NULL) {
        count = count + 1;
        printf("%d value %d \n",count,(int)d);
    }
    printf("Count match %d\n",entries == count);
    fs_rid_set_free(set);
    printf("elapse time %.3f sec. \n", now - then);


	return 0;
}

