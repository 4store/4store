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

    Copyright (C) 2007 Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <glib.h>
#include <rasqal.h>

#include "optimiser.h"
#include "query.h"
#include "query-datatypes.h"
#include "query-intl.h"
#include "debug.h"
#include "../common/error.h"
#include "../common/hash.h"
#include "../common/rdf-constants.h"

/* returns the number of values for the expression, or INT_MAX if its unbound */
int fs_opt_num_vals(fs_binding *b, rasqal_literal *l)
{
    if (!l) return 0;

    switch (l->type) {
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
	case RASQAL_LITERAL_URI:
	case RASQAL_LITERAL_STRING:
	case RASQAL_LITERAL_BOOLEAN:
	case RASQAL_LITERAL_INTEGER:
	case RASQAL_LITERAL_INTEGER_SUBTYPE:
	case RASQAL_LITERAL_DOUBLE:
	case RASQAL_LITERAL_FLOAT:
	case RASQAL_LITERAL_DECIMAL:
	case RASQAL_LITERAL_DATETIME:
	    return 1;
	case RASQAL_LITERAL_VARIABLE: {
	    fs_binding *bv = fs_binding_get(b, l->value.variable);
	    if (bv && bv->bound == 1) {
		return bv->vals->length;
	    }
	    return INT_MAX;
	}

        /* we shouldn't find any of these */
	case RASQAL_LITERAL_UNKNOWN:
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
            return 0;
    }

    return INT_MAX;
}

/* returns true if the expression can be hashed */
int fs_opt_is_const(fs_binding *b, rasqal_literal *l)
{
    if (!l) return 0;

    switch (l->type) {
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
	case RASQAL_LITERAL_URI:
	case RASQAL_LITERAL_STRING:
	case RASQAL_LITERAL_BOOLEAN:
	case RASQAL_LITERAL_INTEGER:
	case RASQAL_LITERAL_INTEGER_SUBTYPE:
	case RASQAL_LITERAL_DOUBLE:
	case RASQAL_LITERAL_FLOAT:
	case RASQAL_LITERAL_DECIMAL:
	case RASQAL_LITERAL_DATETIME:
	    return 1;
	case RASQAL_LITERAL_VARIABLE: {
	    fs_binding *bv = fs_binding_get(b, l->value.variable);
	    if (bv && bv->bound == 1) {
		return 1;
	    }
	    return 0;

	}

	/* we shouldn't find any of these... */
	case RASQAL_LITERAL_UNKNOWN:
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
	    return 0;
    }

    return 0;
}

/* returns true if the expression has bound values, or nothing does */
int fs_opt_is_bound(fs_binding *b, rasqal_literal *l)
{
    if (!l) return 0;

    switch (l->type) {
	case RASQAL_LITERAL_VARIABLE: {
            if (fs_binding_length(b) == 0) {
                return 1;
            }
	    fs_binding *bv = fs_binding_get(b, l->value.variable);
	    if (bv && bv->bound == 1) {
		return 1;
	    }
	    return 0;

	}

        case RASQAL_LITERAL_INTEGER_SUBTYPE:
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
	case RASQAL_LITERAL_URI:
	case RASQAL_LITERAL_STRING:
	case RASQAL_LITERAL_BOOLEAN:
	case RASQAL_LITERAL_INTEGER:
	case RASQAL_LITERAL_DOUBLE:
	case RASQAL_LITERAL_FLOAT:
	case RASQAL_LITERAL_DECIMAL:
	case RASQAL_LITERAL_DATETIME:
	    return 0;

	/* we shouldn't find any of these... */
	case RASQAL_LITERAL_UNKNOWN:
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
	    return 0;
    }

    return 0;
}

/* returns name of bound variable, or NULL if its not a variable */
static char *var_name(rasqal_literal *l)
{
    if (!l) return NULL;

    switch (l->type) {
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
	case RASQAL_LITERAL_URI:
	case RASQAL_LITERAL_STRING:
	case RASQAL_LITERAL_BOOLEAN:
	case RASQAL_LITERAL_INTEGER:
	case RASQAL_LITERAL_INTEGER_SUBTYPE:
	case RASQAL_LITERAL_DOUBLE:
	case RASQAL_LITERAL_FLOAT:
	case RASQAL_LITERAL_DECIMAL:
	case RASQAL_LITERAL_DATETIME:
	    return NULL;

	case RASQAL_LITERAL_VARIABLE:
	    return (char *)l->value.variable->name;

	/* we shouldn't find any of these... */
	case RASQAL_LITERAL_UNKNOWN:
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
	    return NULL;
    }

    return NULL;
}

int fs_optimise_triple_pattern(fs_query_state *qs, fs_query *q, int block, rasqal_triple *patt[], int length, int start)
{
    if (length - start < 2 || q->opt_level < 1) {
	return 1;
    }

    rasqal_triple **pbuf = malloc(length * sizeof(rasqal_triple *));
    memcpy(pbuf, patt, sizeof(rasqal_triple *) * length);
    memset(patt, 0, length * sizeof(rasqal_triple *));
    int append_pos = start;
    for (int i=0; i<start; i++) {
	pbuf[i] = patt[i];
    }

    /* roughly sort into order:
     *   const subject and predicate
     *   const predicate and object
     *   const subject
     *   const object
     *   const graph
     *   const predicate
     *   all variable
     */

    for (int i=start; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_bind_freq(qs, q, block, pbuf[i]) == 1) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=start; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_const(q->bb[block], pbuf[i]->subject) && fs_opt_is_const(q->bb[block], pbuf[i]->predicate) && fs_opt_is_bound(q->bb[block], pbuf[i]->object)) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_bound(q->bb[block], pbuf[i]->subject) && fs_opt_is_const(q->bb[block], pbuf[i]->predicate) && fs_opt_is_const(q->bb[block], pbuf[i]->object)) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_const(q->bb[block], pbuf[i]->subject) && fs_opt_is_bound(q->bb[block], pbuf[i]->object)) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_const(q->bb[block], pbuf[i]->object) && fs_opt_is_bound(q->bb[block], pbuf[i]->subject)) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_const(q->bb[block], pbuf[i]->predicate) && (fs_opt_is_bound(q->bb[block], pbuf[i]->subject) || fs_opt_is_bound(q->bb[block], pbuf[i]->object))) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	if (fs_opt_is_const(q->bb[block], pbuf[i]->origin)) {
	    patt[append_pos++] = pbuf[i];
	    pbuf[i] = NULL;
	}
    }
    for (int i=0; i<length; i++) {
	if (!pbuf[i]) {
	    continue;
	}
	patt[append_pos++] = pbuf[i];
	pbuf[i] = NULL;
    }
    free(pbuf);

    if (append_pos != length) {
	fs_error(LOG_CRIT, "Optimser mismatch error");
    }

#ifdef DEBUG_OPTIMISER
    printf("optimiser choices look like:\n");
    for (int i=start; i<length; i++) {
        printf("%4d: ", i);
        rasqal_triple_print(patt[i], stdout);
        printf("\n");
    }
#endif

    /* If the next two or more pattern's subjects are both variables, we might be able
     * to multi reverse bind them */
    if (var_name(patt[start]->subject) && var_name(patt[start+1]->subject) &&
        !var_name(patt[start]->predicate) &&
        !var_name(patt[start]->object) &&
        fs_opt_num_vals(q->bb[block], patt[start]->predicate) == 1 &&
        fs_opt_num_vals(q->bb[block], patt[start]->origin) == 0 &&
        fs_opt_num_vals(q->bb[block], patt[start+1]->origin) == 0) {
        char *svname = var_name(patt[start]->subject);
        int count = 1;
        while (start+count < length &&
               !fs_opt_is_const(q->bb[block], patt[start+count]->subject) &&
               !strcmp(svname, var_name(patt[start+count]->subject)) &&
               !var_name(patt[start+count]->object) &&
               !var_name(patt[start+count]->predicate)) {
            count++;
        }

        /* if we found a reverse bind pair then we may as well use that, rather
         * than pressing on and using the freq data to pick an order, the
         * backend has more complete information */
        if (count > 1) return count;
    }

    if (length - start > 1) {
        int freq_a = fs_bind_freq(qs, q, block, patt[start]);
        int freq_b = fs_bind_freq(qs, q, block, patt[start+1]);
        /* the 2nd is cheaper than the 1st, then swap them */
        if (freq_b < freq_a) {
            rasqal_triple *tmp = patt[start];
            patt[start] = patt[start+1];
            patt[start+1] = tmp;
        }
    }

    return 1;
}

static int calc_freq(fs_query *q, int block, GHashTable *freq, rasqal_literal *pri, rasqal_literal *sec)
{
    int ret = 0;

    int junk;
    rasqal_variable *var;
    fs_rid_vector *pv = fs_rid_vector_new(1);
    fs_rid_vector *sv = fs_rid_vector_new(1);
    sv->length = 0;
    pv->length = 0;
    fs_bind_slot(q, -1, q->bb[block], pri, pv, &junk, &var, 1);
    if (sec) fs_bind_slot(q, -1, q->bb[block], sec, sv, &junk, &var, 1);
    fs_quad_freq fd;
    fd.pri = pv->data[0];
    if (sec) {
        fd.sec = sv->data[0];
    } else {
        fd.sec = FS_RID_NULL;
    }
    fs_quad_freq *res = g_hash_table_lookup(freq, &fd);
    if (res) ret = res->freq;

    if (ret == 0) {
        if (sec) {
            /* arbitrary choice - if primary and secondary keys are known, but
             * theres not enough data to register in the frequency data then we
             * guess there will be 1 answer */
            ret = 1;
        } else {
            /* arbitrary choice - if only the primary key is known, but
             * theres not enough data to register in the frequency data then we
             * guess there will be 10 answers */
            ret = 10;
        }
    }

    return ret;
}

int fs_bind_freq(fs_query_state *qs, fs_query *q, int block, rasqal_triple *t)
{
    int ret = 100;
    char dir = 'X';

    if (!fs_opt_is_const(q->bb[block], t->subject) && !fs_opt_is_const(q->bb[block], t->predicate) &&
        !fs_opt_is_const(q->bb[block], t->object) && !fs_opt_is_const(q->bb[block], t->origin)) {
        dir = '?';
        ret = INT_MAX;
    } else if (!fs_opt_is_const(q->bb[block], t->subject) &&
               !fs_opt_is_const(q->bb[block], t->object)) {
        dir = '?';
        ret = INT_MAX - 100;
    } else if (qs->freq_s && fs_opt_num_vals(q->bb[block], t->subject) == 1 &&
               fs_opt_num_vals(q->bb[block], t->predicate) == 1) {
        dir = 's';
        ret = calc_freq(q, block, qs->freq_s, t->subject, t->predicate);
    } else if (qs->freq_o && fs_opt_num_vals(q->bb[block], t->object) == 1 &&
               fs_opt_num_vals(q->bb[block], t->predicate) == 1) {
        dir = 'o';
        ret = calc_freq(q, block, qs->freq_o, t->object, t->predicate) +
                q->segments * 50;
    } else if (qs->freq_s && fs_opt_num_vals(q->bb[block], t->subject) == 1) {
        dir = 's';
        ret = calc_freq(q, block, qs->freq_s, t->subject, NULL);
    } else if (qs->freq_o && fs_opt_num_vals(q->bb[block], t->object) == 1) {
        dir = 'o';
        ret = calc_freq(q, block, qs->freq_s, t->object, NULL) +
                q->segments * 50;
    /* cluases for if we have no freq data */
    } else if (fs_opt_num_vals(q->bb[block], t->subject) < 1000000 &&
               fs_opt_num_vals(q->bb[block], t->predicate) < 100 &&
               fs_opt_num_vals(q->bb[block], t->object) == INT_MAX) {
        dir = 's';
        ret = fs_opt_num_vals(q->bb[block], t->subject) * fs_opt_num_vals(q->bb[block], t->predicate);
        if (!fs_opt_is_bound(q->bb[block], t->subject) &&
            !fs_opt_is_bound(q->bb[block], t->predicate) &&
            !fs_opt_is_bound(q->bb[block], t->object)) {
            ret *= (fs_binding_length(q->bb[block]) * 100);
        }
    } else if (fs_opt_num_vals(q->bb[block], t->object) < 1000000 &&
               fs_opt_num_vals(q->bb[block], t->predicate) < 100 &&
               fs_opt_num_vals(q->bb[block], t->subject) == INT_MAX) {
        dir = 'o';
        ret = fs_opt_num_vals(q->bb[block], t->predicate) * fs_opt_num_vals(q->bb[block], t->object);
        if (!fs_opt_is_bound(q->bb[block], t->subject) &&
            !fs_opt_is_bound(q->bb[block], t->predicate) &&
            !fs_opt_is_bound(q->bb[block], t->object)) {
            ret *= (fs_binding_length(q->bb[block]) * 100);
        }
    }

#if DEBUG_OPTIMISER
    if (q->flags & FS_QUERY_EXPLAIN) {
        printf("freq(%c, ", dir);
        rasqal_triple_print(t, stdout);
        printf(") = %d\n", ret);
    }
#endif

    return ret;
}

static char *get_lex(fsp_link *link, fs_rid rid)
{
    if (rid == FS_RID_NULL) return g_strdup("*");

    const int segments = fsp_link_segments(link);
    fs_resource res;
    fs_rid_vector *rv = fs_rid_vector_new(1);
    rv->data[0] = rid;
    fsp_resolve(link, FS_RID_SEGMENT(rid, segments), rv, &res);
    fs_rid_vector_free(rv);

    if (!strncmp(RDF_NAMESPACE, res.lex, strlen(RDF_NAMESPACE))) {
        char *new = g_strdup_printf("rdf:%s", res.lex + strlen(RDF_NAMESPACE));
        g_free(res.lex);

        return new;
    }

    if (FS_IS_URI(rid)) {
        char *new = g_strdup_printf("<%s>", res.lex);
        g_free(res.lex);

        return new;
    }
    if (FS_IS_LITERAL(rid)) {
        char *new = g_strdup_printf("'%s'", res.lex);
        g_free(res.lex);

        return new;
    }

    return res.lex;
}

void foreach_freq_both(gpointer key, gpointer value, gpointer user_data)
{
    fs_quad_freq *f = key;
    fsp_link *link = user_data;

    if (f->sec == FS_RID_NULL) return;

    char *p = get_lex(link, f->pri);
    char *s = get_lex(link, f->sec);
    printf("   (%s %s) -> %lld\n", p, s, f->freq);
}

void foreach_freq_one(gpointer key, gpointer value, gpointer user_data)
{
    fs_quad_freq *f = key;
    fsp_link *link = user_data;

    if (f->sec != FS_RID_NULL) return;

    char *p = get_lex(link, f->pri);
    char *s = get_lex(link, f->sec);
    printf("   (%s %s) -> %lld\n", p, s, f->freq);
}

void fs_optimiser_freq_print(fs_query_state *qs)
{
    printf("(subject predicate)\n");
    g_hash_table_foreach(qs->freq_s, foreach_freq_both, qs->link);
    g_hash_table_foreach(qs->freq_s, foreach_freq_one, qs->link);
    printf("(object predicate)\n");
    g_hash_table_foreach(qs->freq_o, foreach_freq_both, qs->link);
    g_hash_table_foreach(qs->freq_o, foreach_freq_one, qs->link);
}

/* vi:set expandtab sts=4 sw=4: */
