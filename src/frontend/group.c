/*
 *  Copyright (C) 2010 Steve Harris
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  $Id: group.c $
 */

#include <stdio.h>

#include "query.h"
#include "group.h"
#include "debug.h"

/* add metadata to block b of q to allow aggreagates to run over it */

int fs_query_group_block(fs_query *q, int b)
{
    /* if the query has an ORDER BY clause */
    if (rasqal_query_get_group_condition(q->rq, 0)) {
        for (int i=0; q->bb[b][i].name; i++) {
            q->bb[b][i].sort = 0;
        }
        fs_binding *gr = fs_binding_create(q->bb[b], "_group", FS_RID_NULL, 0);
        gr->bound = 1;
        gr->sort = 1;
        const long length = fs_binding_length(q->bb[b]);
        for (int i=0; rasqal_query_get_group_condition(q->rq, i); i++) {
            rasqal_expression *e = rasqal_query_get_group_condition(q->rq, i);

            for (long row = 0; row < length; row++) {
                fs_value v = fs_expression_eval(q, row, b, e);
                v = fs_value_fill_rid(q, v);
                //fs_value_print(v);
                //printf("\n");
                if (i == 0) {
                    fs_rid_vector_append(gr->vals, v.rid);
                } else {
                    /* note, this is a hack, no guarantee of non-collision */
                    gr->vals->data[row] ^= (v.rid << i);
                }
            }
        }
        fs_binding_sort(q->bb[b]);
#ifdef DEBUG_MERGE
        printf("Grouped:\n");
        fs_binding_print(q->bb[b], stdout);
#endif
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
