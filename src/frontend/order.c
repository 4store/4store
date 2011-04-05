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

    Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <string.h>

#include "order.h"
#include "filter.h"
#include "debug.h"
#include "results.h"
#include "query-intl.h"
#include "../common/4s-hash.h"
#include "../common/error.h"

struct order_row {
    int row;
    int width;
    fs_value *vals;
};

static int orow_compare_sub(const struct order_row *a,
                            const struct order_row *b)
{
    int mod = 0;

    for (int i=0; i<a->width; i++) {
        fs_value va = a->vals[i];
        fs_value vb = b->vals[i];
        if (va.valid & fs_valid_bit(FS_V_DESC)) {
            mod = -1;
        } else {
            mod = 1;
        }
#ifdef DEBUG_ORDER
fs_value_print(va);
printf(" <=> ");
fs_value_print(vb);
#endif
        int order = fs_order_by_cmp(va, vb);
        if (order == 0) {
            continue;
        }

        return order * mod;
    }

    return 0;
}

int fs_order_by_cmp(fs_value va, fs_value vb)
{
    if (va.valid & fs_valid_bit(FS_V_RID) && va.rid == FS_RID_NULL) {
        if (vb.valid & fs_valid_bit(FS_V_RID) && vb.rid == FS_RID_NULL) {
            return 0;
        }
        return -1;
    }
    if (vb.valid & fs_valid_bit(FS_V_RID) && vb.rid == FS_RID_NULL) {
        return 1;
    }
    if (va.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(va.rid)) {
        if (vb.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(vb.rid)) {
            if (va.rid > vb.rid) {
                return 1;
            } else if (va.rid < vb.rid) {
                return -1;
            }
            return 0;
        }
        return -1;
    }
    if (vb.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(vb.rid)) {
        return 1;
    }
    if (va.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI(va.rid)) {
        if (vb.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI(vb.rid)) {
            int cmp = strcmp(va.lex, vb.lex);
            if (cmp != 0) return cmp;
            return 0;
        }
        return -1;
    }
    if (vb.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI(vb.rid)) {
        return 1;
    }

    fs_value cmp = fn_equal(NULL, va, vb);
    if (!(cmp.valid & fs_valid_bit(FS_V_TYPE_ERROR)) && cmp.in) {
        return 0;
    }
    cmp = fn_less_than(NULL, va, vb);
    if (cmp.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
        if (va.lex && vb.lex) {
            int cmp = strcmp(va.lex, vb.lex);
            if (cmp != 0) {
                return cmp;
            }
        }

        /* TODO check for plain v's typed */
        
        return 0;
    }

    if (cmp.in) {
        return -1;
    } else {
        return 1;
    }
}

static int orow_compare(const void *ain, const void *bin)
{
    const struct order_row *a = ain;
    const struct order_row *b = bin;

    int cmp = orow_compare_sub(a, b);
#ifdef DEBUG_ORDER
    printf(" = %d\n", cmp);
#endif

    return cmp;
}

static void reverse_array(int *a, int length)
{
    int tmp;

    for (int i=0; i<length/2; i++) {
        tmp = a[i];
        a[i] = a[length-i-1];
        a[length-i-1] = tmp;
    }
}

void fs_query_order(fs_query *q)
{
    int conditions;

    for (conditions = 0; rasqal_query_get_order_condition(q->rq, conditions);
            conditions++);
    const int length = q->length;
#ifdef DEBUG_ORDER
printf("@@ ORDER (%d x %d)\n", conditions, length);
#endif

    /* spot the case where we have ORDER BY ?x, saves evaluating expressions */
    if (conditions == 1) {
        rasqal_expression *oe = rasqal_query_get_order_condition(q->rq, 0);
        if ((oe->op == RASQAL_EXPR_ORDER_COND_ASC ||
             oe->op == RASQAL_EXPR_ORDER_COND_DESC) &&
            oe->arg1->op == RASQAL_EXPR_LITERAL &&
            oe->arg1->literal->type == RASQAL_LITERAL_VARIABLE) {
            long int col = (long int)oe->arg1->literal->value.variable->user_data;
            if (col == 0) {
                fs_error(LOG_CRIT, "missing column");

                return;
            }
            int *ordering;
            if (!fs_sort_column(q, q->bt, col, &ordering)) {
                if (oe->op == RASQAL_EXPR_ORDER_COND_DESC) {
                    reverse_array(ordering, q->bt[col].vals->length);
                }
                q->ordering = ordering;

                return;
            }
        }
    }

    struct order_row *orows = malloc(sizeof(struct order_row) * length);
    fs_value *ordervals = malloc(length * conditions * sizeof(fs_value));
    for (int i=0; i<length; i++) {
	for (int j=0; j<conditions; j++) {
	    ordervals[i * conditions + j] = fs_expression_eval(q, i, 0,
				rasqal_query_get_order_condition(q->rq, j));

#ifdef DEBUG_ORDER
printf("@@ ORDER VAL (%d, %d) = ", i, j);
fs_value_print(ordervals[i * conditions + j]);
printf("\n");
#endif
	}
        orows[i].row = i;
        orows[i].width = conditions;
        orows[i].vals = ordervals + (i * conditions);
    }

    qsort(orows, length, sizeof(struct order_row), orow_compare);

    int *ordering = malloc(sizeof(int) * length);
    for (int i=0; i<length; i++) {
        ordering[i] = orows[i].row;
    }
#ifdef DEBUG_ORDER
printf("Output order:\n");
for (int i=0; i<length; i++) {
    printf("output row %d row %d\n", i, ordering[i]);
}
#endif

    q->ordering = ordering;
    free(ordervals);
    free(orows);
}

/* vi:set expandtab sw=4 sts=4: */
