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

#include <stdlib.h>
#include <glib.h>
#include <string.h>

#include "query-datatypes.h"
#include "query-intl.h"
#include "filter.h"
#include "debug.h"
#include "common/error.h"
#include "common/datatypes.h"
#include "common/sort.h"

/* struct to hold information useful for sorting binding tables */
struct sort_context {
    fs_binding *b;
};

/* lookup sorted table values given a column number and logcial (sorted) row
 * number, sorted order is represented in the 0th column (_ord) to make sorting
 * more efficient */

static fs_rid table_value(fs_binding *b, int col, int log)
{
    if (b[col].vals->length < log) {
        /* out of range */
        return FS_RID_NULL;
    }

    /* if the _ord column exists */
    if (b[0].vals->length > 0) {
        return b[col].vals->data[b[0].vals->data[log]];
    } else {
        return b[col].vals->data[log];
    }
}

fs_binding *fs_binding_new()
{
    fs_binding *b = calloc(FS_BINDING_MAX_VARS+1, sizeof(fs_binding));
    for (int i=0; i<FS_BINDING_MAX_VARS; i++) {
	b[i].appears = -1;
	b[i].depends = -1;
    }
    /* add column to denote join ordering */
    fs_binding_add(b, "_ord", FS_RID_NULL, 0);

    return b;
}

void fs_binding_free(fs_binding *b)
{
    if (!b) return;

    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	g_free(b[i].name);
        b[i].name = NULL;
	fs_rid_vector_free(b[i].vals);
        b[i].vals = NULL;
    }
    memset(b, 0, sizeof(fs_binding));
    free(b);
}

int fs_binding_set_expression(fs_binding *b, const char *name, rasqal_expression *ex)
{
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
            b[i].expression = ex;

            return 0;
        }
    }

    return 1;
}

int fs_binding_any_bound(fs_binding *b)
{
    for (int i=0; b[i].name; i++) {
	if (b[i].bound) {
	    return 1;
	}
    }

    return 0;
}

int fs_binding_bound_intersects(fs_query *q, int block, fs_binding *b, rasqal_literal *l[4])
{
    for (int i=0; b[i].name; i++) {
	if (b[i].bound) {
	    for (int j=0; j<4; j++) {
		if (l[j] && l[j]->type == RASQAL_LITERAL_VARIABLE &&
		    !strcmp((char *)l[j]->value.variable->name, b[i].name)) {
		    /* if this var is bound only in this union block, and its
		     * not yet be used in this branch, then we dont iterate */
		    if (q->union_group[block] > 0 &&
			q->union_group[block] == q->union_group[b[i].appears] &&
			b[i].bound_in_block[block] == 0) {
			continue;
		    }
		    return 1;
		}
	    }
	}
    }

    return 0;
}

int fs_binding_length(fs_binding *b)
{
    int length = 0;
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (b[i].vals && b[i].vals->length > length) {
	    length = b[i].vals->length;
	}
    }

    return length;
}

int fs_binding_width(fs_binding *b)
{
    int width;
    for (width=0; b[width].name; width++) ;

    return width;
}

fs_binding *fs_binding_add(fs_binding *b, const char *name, fs_rid val, int projected)
{
    int i;

#ifdef DEBUG_BINDING
    if (strcmp(DEBUG_BINDING, name)) printf("@@ add("DEBUG_BINDING", %016llx, %d)\n", val, projected);
#endif
    for (i=0; 1; i++) {
	if (b[i].name == NULL) {
	    if (i == FS_BINDING_MAX_VARS) {
		fs_error(LOG_ERR, "variable limit of %d exceeded",
			FS_BINDING_MAX_VARS);

		return NULL;
	    }
	    b[i].name = g_strdup(name);
	    if (val != FS_RID_NULL) {
                if (b[i].vals) {
                    fs_error(LOG_WARNING, "loosing pointer to rid_vector");
                }
		b[i].vals = fs_rid_vector_new_from_args(1, val);
		b[i].bound = 1;
	    } else {
                if (b[i].vals) {
                    fs_error(LOG_WARNING, "loosing pointer to rid_vector");
                }
		b[i].vals = fs_rid_vector_new(0);
	    }
	    b[i].proj = projected;
	    b[i].need_val = projected;

	    return b+i;
	}
	if (!strcmp(b[i].name, name)) {
	    fs_rid_vector_append(b[i].vals, val);
	    b[i].bound = 1;
	    b[i].proj |= projected;
	    b[i].need_val |= projected;

	    return b+i;
	}
    }

    return NULL;
}

void fs_binding_clear_vector(fs_binding *b, const char *name)
{
#ifdef DEBUG_BINDING
    if (strcmp(DEBUG_BINDING, name)) printf("@@ clear_vector("DEBUG_BINDING")\n");
#endif
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    b[i].vals->length = 0;
	    return;
	}
    }
}

fs_binding *fs_binding_copy(fs_binding *b)
{
    if (!b) {
        fs_error(LOG_CRIT, "tried to copy NULL binding table");

        return NULL;
    }
#ifdef DEBUG_BINDING
    printf("@@ copy()\n");
#endif
    fs_binding *b2 = fs_binding_new();

    memcpy(b2, b, sizeof(fs_binding) * FS_BINDING_MAX_VARS);
    for (int i=0; 1; i++) {
        if (!b[i].name) {
            break;
        }
        b[i].name = g_strdup(b2[i].name);
        b[i].vals = fs_rid_vector_copy(b2[i].vals);
    }

    return b2;
}

fs_binding *fs_binding_copy_and_clear(fs_binding *b)
{
#ifdef DEBUG_BINDING
    printf("@@ copy_and_clear()\n");
#endif
    fs_binding *b2 = fs_binding_new();

    memcpy(b2, b, sizeof(fs_binding) * FS_BINDING_MAX_VARS);
    for (int i=0; 1; i++) {
        if (!b[i].name) {
            break;
        }
        b[i].name = g_strdup(b2[i].name);
        b[i].vals = fs_rid_vector_new(b2[i].vals->size);
        b[i].vals->length = 0;

        /* at this point we can clear the bound flag as
           shortcuts on variables bound to the empty
           list are now handled by a wrapper round
           fsp_bind_*() and look to the parent code
           just like we had sent them to the backend */
        b[i].bound = 0;
    }

    return b2;
}

void fs_binding_clear(fs_binding *b)
{
#ifdef DEBUG_BINDING
    printf("@@ clear()\n");
#endif
    fs_binding *b2 = fs_binding_new();

    memcpy(b2, b, sizeof(fs_binding) * FS_BINDING_MAX_VARS);
    for (int i=0; 1; i++) {
        if (!b[i].name) {
            break;
        }
        b2[i].name = NULL;
        b[i].vals->length = 0;
        b[i].bound = 0;
    }
}

void fs_binding_add_vector(fs_binding *b, const char *name, fs_rid_vector *vals)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ add_vector("DEBUG_BINDING", %p)\n", vals);
#endif
    int i;
    for (i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    for (int j=0; vals && j<vals->length; j++) {
		fs_rid_vector_append(b[i].vals, vals->data[j]);
	    }
	    b[i].bound = 1;
	    return;
	}
    }

    if (i == FS_BINDING_MAX_VARS) {
	fs_error(LOG_ERR, "variable limit (%d) exceeded",
		FS_BINDING_MAX_VARS);

	return;
    }

    /* name wasn't found, add it */
    if (!b[i].name) {
	b[i].name = g_strdup(name);
	b[i].vals = fs_rid_vector_copy(vals);
	b[i].bound = 1;
    }
}

fs_binding *fs_binding_get(fs_binding *b, const char *name)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ get("DEBUG_BINDING")\n");
#endif
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    return b+i;
	}
    }

    return NULL;
}

fs_rid fs_binding_get_val(fs_binding *b, const char *name, int idx, int *bound)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ get_val("DEBUG_BINDING", %d)\n", idx);
#endif
    int i;

    for (i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
            if (!b[i].need_val) return FS_RID_GONE;
	    if (bound) *bound = b[i].bound;
	    if (!*bound) {
		return FS_RID_NULL;
	    }
	    if (idx >= 0 && idx < b[i].vals->length) {
		return b[i].vals->data[idx];
	    }
	    fs_error(LOG_ERR, "val request out of range for variable '%s'", name);

	    return FS_RID_NULL;
	}
    }

    return FS_RID_NULL;
}

fs_rid_vector *fs_binding_get_vals(fs_binding *b, const char *name, int *bound)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ get_vals("DEBUG_BINDING")\n");
#endif
    int i;

    for (i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    if (bound) *bound = b[i].bound;
	    return b[i].vals;
	}
    }

    /*
    fs_error(LOG_ERR, "binding lookup on unknown variable '%s'", name);
    */

    return NULL;
}

void fs_binding_clear_used_all(fs_binding *b)
{
#ifdef DEBUG_BINDING
    printf("@@ clear_used_all()\n");
#endif
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	b[i].used = 0;
    }
}

void fs_binding_set_used(fs_binding *b, const char *name)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ set_used("DEBUG_BINDING")\n");
#endif
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    b[i].used = 1;
	    break;
	}
    }
}

int fs_binding_get_projected(fs_binding *b, const char *name)
{
#ifdef DEBUG_BINDING
    if (!strcmp(DEBUG_BINDING, name)) printf("@@ get_projected("DEBUG_BINDING")\n");
#endif
    for (int i=0; 1; i++) {
	if (!b[i].name) break;
	if (!strcmp(b[i].name, name)) {
	    return b[i].proj;
	}
    }

    /*
    fs_error(LOG_ERR, "binding lookup on unknown variable '%s'", name);
    */

    return 0;
}

void fs_binding_copy_row_unused(fs_binding *from, int row, int count, fs_binding *to)
{
    for (int i=0; 1; i++) {
	if (!from[i].name) break;
	if (from[i].used) {
	    continue;
	}
	fs_rid val;
	if (row < from[i].vals->length) {
	    val = from[i].vals->data[row];
	} else {
	    val = FS_RID_NULL;
	}
	for (int j=0; j<count; j++) {
	    fs_rid_vector_append(to[i].vals, val);
	}
    }
}

void fs_binding_print(fs_binding *b, FILE *out)
{
    int length = fs_binding_length(b);

    fprintf(out, "    ");
    if (b[0].vals->length) {
        fprintf(out, "      ");
    }
    for (int c=1; b[c].name; c++) {
	if (b[c].bound) {
	    fprintf(out, " %16.16s", b[c].name);
	} else {
	    fprintf(out, " %12.12s", b[c].name);
	}
    }
    fprintf(out, "\n");
    fprintf(out, " row");
    if (b[0].vals->length) {
        fprintf(out, " order");
    }
    for (int c=1; b[c].name; c++) {
        if (b[c].bound) {
	    fprintf(out, "     %c%c%c%c A%02d D%02d",
		    b[c].proj ? 'p' : '-', b[c].used ? 'u' : '-',
		    b[c].need_val ? 'n' : '-', b[c].bound ? 'b' : '-',
		    b[c].appears, b[c].depends);
	} else {
	    fprintf(out, "  %c%c%c A%02d D%02d",
		    b[c].proj ? 'p' : '-', b[c].used ? 'u' : '-',
		    b[c].need_val ? 'n' : '-', 
		    b[c].appears, b[c].depends);
	}
    }
    fprintf(out, "\n");
    for (long int lr=0; lr<length; lr++) {
        long int r = lr;
        if (b[0].vals->length) {
            r = b[0].vals->data[lr];
        }
        fprintf(out, "%4ld", lr);
        if (b[0].vals->length) {
            fprintf(out, " %5ld", r);
        }
	for (int c=1; b[c].name; c++) {
            if (b[c].bound) {
		if (r < b[c].vals->length && b[c].vals->data[r] == FS_RID_NULL) {
                    fprintf(out, " %16s", "null");
		} else {
                    fprintf(out, " %016llx", r < b[c].vals->length ? b[c].vals->data[r] : -1);
		}
	    } else {
		fprintf(out, "%13s", "null");
	    }
	}
	fprintf(out, "\n");
#if !defined(DEBUG_MERGE) || DEBUG_MERGE < 3
	if (length > 25 && lr > 20 && (length - lr) > 2) {

	    fprintf(out, " ...\n");
	    lr = length - 3;
	}
#endif
    }
}

/* q should be set when joining */
static int binding_row_compare(fs_query *q, fs_binding *b1, fs_binding *b2, int p1, int p2, int length1, int length2)
{
    if (p1 >= length1 && p2 >= length2) {
        return 0;
    }
    if (p1 >= length1) {
#ifdef DEBUG_COMPARE
        printf("CMP from past end\n");
#endif
	return 1;
    } else if (p2 >= length2) {
#ifdef DEBUG_COMPARE
        printf("CMP to past end\n");
#endif
	return -1;
    }

    for (int i=1; b1[i].name; i++) {
	if (!b1[i].sort) continue;

        const fs_rid b1v = table_value(b1, i, p1);
        const fs_rid b2v = table_value(b2, i, p2);

        if (b1v == FS_RID_NULL || b2v == FS_RID_NULL) {
            continue;
        }

	if (b1v > b2v) {
#ifdef DEBUG_COMPARE
            printf("CMP %llx > %llx\n", b1v, b2v);
#endif
	    return 1;
	}
	if (b1v < b2v) {
#ifdef DEBUG_COMPARE
            printf("CMP %llx < %llx\n", b1v, b2v);
#endif
	    return -1;
	}
    }

    return 0;
}

static int qsort_r_cmp(const void *a, const void *b, void *ctxt)
{
    struct sort_context *c = ctxt;

    const long rowa = *(fs_rid *)a;
    const long rowb = *(fs_rid *)b;
    for (int i=1; c->b[i].name; i++) {
	if (!c->b[i].sort) continue;
        const fs_rid vala = rowa >= c->b[i].vals->length ?
            FS_RID_NULL : c->b[i].vals->data[rowa];
        const fs_rid valb = rowb >= c->b[i].vals->length ?
            FS_RID_NULL : c->b[i].vals->data[rowb];
	if (vala > valb) {
	    return 1;
	}
	if (vala < valb) {
	    return -1;
	}
    }

    return 0;
}

/* inplace quicksort on an array of rid_vectors */
void fs_binding_sort(fs_binding *b)
{
    int scount = 0;
    int length = fs_binding_length(b);

    for (int i=0; b[i].name; i++) {
	if (b[i].sort) scount++;
        if (b[i].vals->length < length) {
            for (int j=b[i].vals->length; j<length; j++) {
                fs_rid_vector_append(b[i].vals, FS_RID_NULL);
            }
        }
    }
    if (!scount) {
	fs_error(LOG_WARNING, "fs_binding_sort() called with no sort "
			      "columns set, ignoring");

	return;
    }

    /* fill out the _ord column with intergers in [0,n] */
    b[0].vals->length = 0;
    for (int row=0; row<length; row++) {
        fs_rid_vector_append(b[0].vals, row);
    }
#ifdef DEBUG_MERGE
    double then = fs_time();
#endif

    /* ctxt could include other stuff for optimisations */
    struct sort_context ctxt = { b };
    fs_qsort_r(b[0].vals->data, length, sizeof(fs_rid), qsort_r_cmp, &ctxt);

#ifdef DEBUG_MERGE
    double now = fs_time();
    printf("sort took %f seconds\n", now - then);
#endif
}

void fs_binding_uniq(fs_binding *bi)
{
    if (fs_binding_length(bi) == 0) {
        /* we don't need to do anything, code below assumes >= 1 row */
        return;
    }

    fs_binding *b = fs_binding_copy_and_clear(bi);

    bi[0].vals->length = 0;

#ifdef DEBUG_MERGE
    double then = fs_time();
#endif
    int length = fs_binding_length(b);

    int outrow = 1;
    for (int column = 1; b[column].name; column++) {
        fs_rid_vector_append(bi[column].vals, table_value(b, column, 0));
        bi[column].bound = b[column].bound;
        b[column].sort = b[column].bound;
    }
    for (int row = 1; row < length; row++) {
	if (binding_row_compare(NULL, b, b, row, row-1, length, length) == 0) {
	    continue;
	}
	for (int column = 1; b[column].name; column++) {
            fs_rid_vector_append(bi[column].vals, table_value(b, column, row));
	}
	outrow++;
    }

#ifdef DEBUG_MERGE
    double now = fs_time();
    printf("uniq took %fs (%d->%d rows)\n", now-then, length, outrow);
    fs_binding_print(bi, stdout);
#endif
    fs_binding_free(b);
}

/* truncate a binding to length entries long */
void fs_binding_truncate(fs_binding *b, int length)
{
    for (int i=0; b[i].name; i++) {
        fs_rid_vector_truncate(b[i].vals, length);
    }
}

/* UNION b onto a, returns a with b appended */
void fs_binding_union(fs_query *q, fs_binding *a, fs_binding *b)
{
    const int alen = fs_binding_length(a);
    const int blen = fs_binding_length(b);
    a[0].vals->length = 0;
    for (int c=1; a[c].name && b[c].name; c++) {
        if (!a[c].bound && b[c].bound) {
            a[c].bound = 1;
            while (a[c].vals->length < alen) {
                fs_rid_vector_append(a[c].vals, FS_RID_NULL);
            }
        } else if (a[c].bound && !b[c].bound) {
            b[c].bound = 1;
            while (b[c].vals->length < blen) {
                fs_rid_vector_append(b[c].vals, FS_RID_NULL);
            }
        }
        fs_rid_vector_append_vector(a[c].vals, b[c].vals);
    }
}

/* return to = from [X] to, this is used to perform joins inside blocks, it
 * saves allocations by doing most operations inplace, unlike fs_binding_join */
void fs_binding_merge(fs_query *q, int block, fs_binding *from, fs_binding *to)
{
    fs_binding *inter_f = NULL; /* the intersecting column */
    fs_binding *inter_t = NULL; /* the intersecting column */

    for (int i=0; from[i].name; i++) {
	from[i].sort = 0;
	to[i].sort = 0;
    }
    int used = 0;
    for (int i=1; from[i].name; i++) {
	if (!from[i].bound || !to[i].bound) continue;
        if (from[i].used) used++;

	if (from[i].bound && to[i].bound) {
	    inter_f = from+i;
	    inter_t = to+i;
	    from[i].sort = 1;
	    to[i].sort = 1;
#ifdef DEBUG_MERGE
    printf("@@ join on %s\n", to[i].name);
#endif
	}
    }

    /* from and to bound variables do not intersect, we can just dump results,
       under some circustances we need to do a combinatorial explosion */
    if (!inter_f && (fs_binding_length(from) == 0)) {
	const int length_f = fs_binding_length(from);
	const int length_t = fs_binding_length(to);
	for (int i=1; from[i].name; i++) {
	    if (to[i].bound && !from[i].bound) {
                if (from[i].vals) {
                    fs_rid_vector_free(from[i].vals);
                }
		from[i].vals = fs_rid_vector_new(length_f);
		for (int d=0; d<length_f; d++) {
		    from[i].vals->data[d] = FS_RID_NULL;
		}
		from[i].bound = 1;
	    }
	    if (!from[i].bound) continue;
	    if (!to[i].bound) {
                if (to[i].vals) {
                    fs_rid_vector_free(to[i].vals);
                }
		to[i].vals = fs_rid_vector_new(length_t);
		for (int d=0; d<length_t; d++) {
                    to[i].vals->data[d] = FS_RID_NULL;
                }
	    }
	    fs_rid_vector_append_vector(to[i].vals, from[i].vals);
	    to[i].bound = 1;
	}
#ifdef DEBUG_MERGE
        printf("append all, result:\n");
        fs_binding_print(to, stdout);
#endif

	return;
    }

    /* If were running in restricted mode, truncate the binding tables */
    if (q->flags & FS_QUERY_RESTRICTED) {
        fs_binding_truncate(from, q->soft_limit);
        fs_binding_truncate(to, q->soft_limit);
    }

    int length_t = fs_binding_length(to);
    int length_f = fs_binding_length(from);
    /* ms8: this list keeps track of the vars to replace */
    GList *rep_list = NULL;
    for (int i=1; to[i].name; i++) {
	if (to+i == inter_t || to[i].used || to[i].bound) {
	    /* do nothing */
#if DEBUG_MERGE > 1
    printf("@@ preserve %s\n", to[i].name);
#endif
	} else if (from[i].bound && !to[i].bound) {
#if DEBUG_MERGE > 1
    printf("@@ replace %s\n", from[i].name);
#endif
	    to[i].bound = 1;
            if (to[i].vals) {
                if (to[i].vals->length != length_t) {
                    fs_rid_vector_free(to[i].vals);
                    to[i].vals = fs_rid_vector_new(length_t);
                }
            } else {
                to[i].vals = fs_rid_vector_new(length_t);
            }
	    for (int d=0; d<length_t; d++) {
		to[i].vals->data[d] = FS_RID_NULL;
	    }
      if (q->opt_level == 0)
        rep_list = g_list_append(rep_list, GINT_TO_POINTER(i));
	}
    }

    /* sort the two sets of bindings so they can be merged linearly */
    if (inter_f) {
        fs_binding_sort(from);
        fs_binding_sort(to);
    } else {
        /* make sure the tables are not marked sorted */
        from[0].vals->length = 0;
        to[0].vals->length = 0;
    }

#ifdef DEBUG_MERGE
    printf("old: %d bindings\n", fs_binding_length(from));
    fs_binding_print(from, stdout);
    printf("new: %d bindings\n", fs_binding_length(to));
    fs_binding_print(to, stdout);
#endif

    int fpos = 0;
    int tpos = 0;
    while (fpos < length_f || tpos < length_t) {
        if (q->flags & FS_QUERY_RESTRICTED &&
            fs_binding_length(to) >= q->soft_limit) {
            char *msg = g_strdup("some results have been dropped to prevent overunning time allocation");
            q->warnings = g_slist_prepend(q->warnings, msg);
            break;
        }
	int cmp;
	cmp = binding_row_compare(q, from, to, fpos, tpos, length_f, length_t);
	if (cmp == 0) {
	    /* both rows match */
	    int fp, tp = tpos;
	    for (fp = fpos; binding_row_compare(q, from, to, fp, tpos, length_f, length_t) == 0; fp++) {
#if DEBUG_MERGE > 1
if (fp == 20) {
    printf("...\n");
}
#endif
		for (tp = tpos; 1; tp++) {
		    if (binding_row_compare(q, from, to, fp, tp, length_f, length_t) == 0) {
#if DEBUG_MERGE > 1
if (fp < 20) {
    printf("STEP %d, %d  ", fp-fpos, tp-tpos);
}
#endif
			if (fp == fpos) {
#if DEBUG_MERGE > 1
if (fp < 20) {
    if (inter_f) {
	printf("REPL %llx\n", inter_f->vals->data[fp]);
    } else {
	printf("REPL ???\n");
    }
}
#endif
			    for (int c=1; to[c].name; c++) {
				if (!from[c].bound && !to[c].bound) continue;
				if (from[c].bound && table_value(from, c, fp) == FS_RID_NULL) {
				    continue;
				}
				if (from[c].bound && fp < from[c].vals->length) {
                                    long wrow = to[0].vals->length ? to[0].vals->data[tp] : tp;
				    to[c].vals->data[wrow] = table_value(from, c, fp);
				    if (to[c].vals->length <= tp) {
					to[c].vals->length = tp+1;
				    }
				}
			    }
			} else {
#if DEBUG_MERGE > 1
if (fp < 20) {
    printf("ADD\n");
}
#endif
			    for (int c=1; to[c].name; c++) {
				if (!from[c].bound && !to[c].bound) continue;
				if (from[c].bound && fp < from[c].vals->length) {
				    fs_rid_vector_append(to[c].vals, table_value(from, c, fp));
				} else {
				    fs_rid_vector_append(to[c].vals, table_value(to, c, tp));
				}
			    }
			}
		    } else {
			break;
		    }
		}
	    }
	    tpos = tp;
	    fpos = fp;
	} else if (cmp == -1) {
	    fpos++;
	} else if (cmp == 1) {
	    tpos++;
	} else {
	    fs_error(LOG_CRIT, "unknown compare state %d in binding", cmp);
	}
    }

    /* clear the _ord columns */
    from[0].vals->length = 0;
    to[0].vals->length = 0;

    /* ms8: INIT code to clean up rows that where not replaced */
    if (q->opt_level == 0 && rep_list) {
        GList *del_list = NULL;
        while(rep_list) {
            int col_r = GPOINTER_TO_INT(rep_list->data);
             rep_list = g_list_next(rep_list);
             for (int d=0; d<length_t; d++) {
                if (to[col_r].vals->data[d] == FS_RID_NULL) {
                     del_list = g_list_append(del_list,GINT_TO_POINTER(d));
                }
             }
         }
         g_list_free(rep_list);
         if (del_list) {
             int vars = 0;
             for (int i=1; to[i].name; i++)
                vars++;
             fs_rid_vector **clean = calloc(vars, sizeof(fs_rid_vector *));
             for (int i=1;i<=vars;i++)
                clean[i] = fs_rid_vector_new(0);
             for (int d = 0;d<length_t;d++) {
                   if (!g_list_find(del_list,GINT_TO_POINTER(d))) {
                     for (int i=1;i<=vars;i++)
                        fs_rid_vector_append(clean[i],to[i].vals->data[d]);
                   }
              }

             for (int i=1;i<=vars;i++) {
                free(to[i].vals->data);
                to[i].vals->data = clean[i]->data;
                to[i].vals->length = clean[i]->length;
                to[i].vals->size = clean[i]->size;
             }
             for (int i=0;i<=vars;i++)
                 g_list_free(del_list);
         }
     }
    /* ms8: END code to clean up rows that where not replaced */

#ifdef DEBUG_MERGE
    printf("result: %d bindings\n", fs_binding_length(to));
    fs_binding_print(to, stdout);
#endif
}

/* return a [X] b, or a =X] b, depending on value of join */

fs_binding *fs_binding_join(fs_query *q, fs_binding *a, fs_binding *b, fs_join_type join)
{
    fs_binding *c = fs_binding_copy(a);
    int inter = 0;      /* do the tables intersect */

    for (int i=0; a[i].name; i++) {
	a[i].sort = 0;
	b[i].sort = 0;
	c[i].sort = 0;
        c[i].vals->length = 0;
    }
    int bound_a = 0;
    int bound_b = 0;
    for (int i=1; a[i].name; i++) {
        if (a[i].bound) bound_a++;
        if (b[i].bound) bound_b++;

        if (a[i].bound || b[i].bound) {
            c[i].bound = 1;
        }

	if (a[i].bound && b[i].bound) {
	    inter = 1;
	    a[i].sort = 1;
	    b[i].sort = 1;
#ifdef DEBUG_MERGE
            printf("joining on %s\n", a[i].name);
#endif
	}
    }

    /* a and b bound variables do not intersect, we can just dump results */
    if (!inter) {
        int length_a = fs_binding_length(a);
        int length_b = fs_binding_length(b);
	for (int i=1; a[i].name; i++) {
            if (!a[i].bound) {
                for (int j=0; j<length_a; j++) {
                    fs_rid_vector_append(c[i].vals, FS_RID_NULL);
                }
            } else {
                fs_rid_vector_append_vector(c[i].vals, a[i].vals);
            }
            if (!b[i].bound) {
                for (int j=0; j<length_b; j++) {
                    fs_rid_vector_append(c[i].vals, FS_RID_NULL);
                }
            } else {
                fs_rid_vector_append_vector(c[i].vals, b[i].vals);
            }
	}
#ifdef DEBUG_MERGE
        printf("append all, result:\n");
        fs_binding_print(c, stdout);
#endif
	return c;
    }

    int length_a = fs_binding_length(a);
    int length_b = fs_binding_length(b);

    /* sort the two sets of bindings so they can be merged linearly */
    fs_binding_sort(a);
    fs_binding_sort(b);

#ifdef DEBUG_MERGE
    printf("a: %d bindings\n", fs_binding_length(a));
    fs_binding_print(a, stdout);
    printf("b: %d bindings\n", fs_binding_length(b));
    fs_binding_print(b, stdout);
#endif

    /* If were running in restricted mode, truncate the binding tables */
    if (q->flags & FS_QUERY_RESTRICTED) {
        int restricted = 0;
        fs_binding_truncate(a, q->soft_limit);
        if (length_a > fs_binding_length(a)) {
            length_a = fs_binding_length(a);
            restricted = 1;
        }
        fs_binding_truncate(b, q->soft_limit);
        if (length_b > fs_binding_length(b)) {
            length_b = fs_binding_length(b);
            restricted = 1;
        }
        if (restricted) {
            char *msg = "some results have been dropped to prevent overunning effort allocation";
            q->warnings = g_slist_prepend(q->warnings, msg);
        }
    }

    int apos = 0;
    int bpos = 0;
    int cmp;
    while (apos < length_a) {
        if (join == FS_INNER && bpos >= length_b) break;
	cmp = binding_row_compare(q, a, b, apos, bpos, length_a, length_b);
        if (cmp == -1) {
            /* A and B aren't compatible, A sorts lower, skip A or left join */
#if DEBUG_MERGE > 1
printf("[L] Ar=%d, Br=%d", apos, bpos);
#endif
            if (join == FS_LEFT) {
                for (int col=0; a[col].name; col++) {
                    if (!c[col].need_val) {
                        continue;
                    } else if (a[col].bound) {
#if DEBUG_MERGE > 1
printf(" %s=%016llx", c[col].name, table_value(a, col, apos));
#endif
                        fs_rid_vector_append(c[col].vals, table_value(a, col, apos));
                    } else {
#if DEBUG_MERGE > 1
printf(" %s=null", c[col].name);
#endif
                        fs_rid_vector_append(c[col].vals, FS_RID_NULL);
                    }
                }
            }
            apos++;
        } else if (cmp == 0) {
	    /* both rows match, find out what combinations bind and produce them */
#if DEBUG_MERGE > 1
printf("[I] Ar=%d, Br=%d", apos, bpos);
#endif
            int range_a = apos+1;
            int range_b = bpos+1;
            while (binding_row_compare(q, a, a, apos, range_a, length_a, length_a) == 0) range_a++;
            while (binding_row_compare(q, b, b, bpos, range_b, length_b, length_b) == 0) range_b++;
            int start_a = apos;
            int start_b = bpos;
            for (apos = start_a; apos<range_a; apos++) {
                for (bpos = start_b; bpos<range_b; bpos++) {
                    for (int col=0; a[col].name; col++) {
                        if (!c[col].need_val) {
                            continue;
                        } else if (!a[col].bound && !b[col].bound) {
#if DEBUG_MERGE > 1
                            printf(" %s=null", c[col].name);
#endif
                            fs_rid_vector_append(c[col].vals, FS_RID_NULL);
                        } else if (a[col].bound) {
                            /* if were left joining and A is NULL, we want the
                             * value from B */
                            if (join == FS_LEFT && table_value(a, col, apos) == FS_RID_NULL && b[col].bound) {
#if DEBUG_MERGE > 1
                                printf(" %s=%016llx", c[col].name, table_value(b, col, bpos));
#endif
                                fs_rid_vector_append(c[col].vals, table_value(b, col, bpos));
                            } else {
#if DEBUG_MERGE > 1
                                printf(" %s=%016llx", c[col].name, table_value(a, col, apos));
#endif
                                fs_rid_vector_append(c[col].vals, table_value(a, col, apos));
                            }
                        } else {
#if DEBUG_MERGE > 1
                            printf(" %s=%016llx", c[col].name, table_value(b, col, bpos));
#endif
                            fs_rid_vector_append(c[col].vals, table_value(b, col, bpos));
                        }
                    }
                }
            }
            /* this is actually unneccesary because the for loop will do the
             * same thing, but it's clearer */
            apos = range_a;
            bpos = range_b;
	} else if (cmp == +1) {
            /* A and B aren't compatible, B sorts lower, skip B */
            bpos++;
	} else {
            fs_error(LOG_ERR, "cmp=%d, value out of range", cmp);
        }
#if DEBUG_MERGE > 1
printf("\n");
#endif
    }

    /* clear the _ord columns */
    a[0].vals->length = 0;
    b[0].vals->length = 0;

#ifdef DEBUG_MERGE
    printf("result: %d bindings\n", fs_binding_length(c));
    fs_binding_print(c, stdout);
#endif

    return c;
}

fs_binding *fs_binding_apply_filters(fs_query *q, int block, fs_binding *b, raptor_sequence *constr)
{
    fs_binding *ret = fs_binding_copy(b);
    if (!constr) {
        /* if there's no constriants then we don't need to do anything */

        return ret;
    }
    for (int col=0; b[col].name; col++) {
        ret[col].vals->length = 0;
    }
    int length = fs_binding_length(b);
    fs_binding *restore = q->bt;
    q->bt = b;
    /* TODO should prefetch lexical vals here */
    /* expressions that have been optimised out will be replaces with NULL,
     * so we have to be careful here */
    for (int row=0; row<length; row++) {
        for (int c=0; c<raptor_sequence_size(constr); c++) {
            rasqal_expression *e =
                raptor_sequence_get_at(constr, c);
            if (!e) continue;

            fs_value v = fs_expression_eval(q, row, block, e);
#ifdef DEBUG_FILTER
            rasqal_expression_print(e, stdout);
            printf(" -> ");
            fs_value_print(v);
            printf("\n");
#endif
            if (v.valid & fs_valid_bit(FS_V_TYPE_ERROR) && v.lex) {
                q->warnings = g_slist_prepend(q->warnings, v.lex);
            }
            fs_value result = fn_ebv(v);
            /* its EBV is not true, so we skip to the next one */
            if (result.valid & fs_valid_bit(FS_V_TYPE_ERROR) || !result.in) {
                continue;
            }
            for (int col=0; b[col].name; col++) {
                if (b[col].bound) {
                    fs_rid_vector_append(ret[col].vals, b[col].vals->data[row]);
                }
            }
        }
    }
    q->bt = restore;

    return ret;
}

/* vi:set expandtab sts=4 sw=4: */
