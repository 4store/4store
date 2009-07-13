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

#include "backend.h"
#include "query-backend.h"
#include "common/hash.h"

#define NAME "test"

int main()
{
    fs_backend *be = fs_backend_init("test", 0);
    printf("be = %p\n", be);
    fs_rid_vector *mv = fs_rid_vector_new(0);
    fs_rid_vector *sv = fs_rid_vector_new(0);
    fs_rid_vector *pv = ts_rid_vector_new(0);
    fs_rid_vector *ov = ts_rid_vector_new_from_args(2, ts_hash_literal("Stephen", 0), ts_hash_literal("Harris", 0));
    fs_rid_vector **res = fs_bind(be, 0, TS_BIND_REVERSE | TS_BIND_SUBJECT | TS_BIND_MODEL | TS_BIND_BY_SUBJECT, mv, sv, pv, ov, 0, -1);
    printf("res = %p -> %d\n", res, res[0]->length);
    for (int r=0; r<res[0]->length; r++) {
        printf("%d %llx %llx\n", r, res[0]->data[r], res[1]->data[r]);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
