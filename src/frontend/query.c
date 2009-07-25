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

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <glib.h>
#include <rasqal.h>

#include "query.h"
#include "query-intl.h"
#include "query-datatypes.h"
#include "query-cache.h"
#include "optimiser.h"
#include "filter.h"
#include "filter-datatypes.h"
#include "order.h"
#include "import.h"
#include "common/4store.h"
#include "common/datatypes.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"
#include "common/rdf-constants.h"

//#define DEBUG_BIND

#define DESC_SIZE 1024

#define DEBUG_SIZE(n, thing) printf("@@ %d * sizeof(%s) = %zd\n", n, #thing, n * sizeof(thing))

static void graph_pattern_walk(fsp_link *link, rasqal_graph_pattern *p, fs_query *q, int optional, int pass, int uni);
static int fs_handle_query_triple(fs_query *q, int block, rasqal_triple *t);
static int fs_handle_query_triple_multi(fs_query *q, int block, int count, rasqal_triple *t[]);
static fs_rid const_literal_to_rid(fs_query *q, rasqal_literal *l, fs_rid *attr);
static void check_variables(fs_query *q, rasqal_expression *e);
static void filter_optimise_disjunct_equality(fs_query *q,
            rasqal_expression *e, int block, char **var, fs_rid_vector *res);

static void check_cons_slot(fs_query *q, raptor_sequence *vars, rasqal_literal *l)
{
    if (l->type == RASQAL_LITERAL_VARIABLE) {
        int dup = 0;
        for (int i=0; i<raptor_sequence_size(vars); i++) {
            if (raptor_sequence_get_at(vars, i) == l->value.variable) {
                dup = 1;
                break;
            }
        }
        if (!dup) {
            raptor_sequence_push(vars, l->value.variable);
        }
    }
}

#if 0
/* this is replaced by a cahcing version */
static int bind(fs_query *q, int all,
                int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit)
{
    /* check for no possible bindings */
    for (int s=0; s<4; s++) {
        if (rids[s]->length == 1 && rids[s]->data[0] == FS_RID_NULL) {
            int slots = 0;
            if (flags & FS_BIND_MODEL) slots++;
            if (flags & FS_BIND_SUBJECT) slots++;
            if (flags & FS_BIND_PREDICATE) slots++;
            if (flags & FS_BIND_OBJECT) slots++;
            *result = calloc(slots, sizeof(fs_rid_vector));
            for (int s=0; s<slots; s++) {
                (*result)[s] = fs_rid_vector_new(0);
            }

            return 0;
        }
    }

    int ret;

    if (all) {
        ret = fsp_bind_limit_all(q->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    } else {
        ret = fsp_bind_limit_many(q->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    }
    if (ret) {
        fs_error(LOG_ERR, "bind failed in '%s', %d segments gave errors",
                 fsp_kb_name(q->link), ret);

        exit(1);
    }

    return ret;
}
#endif

static int bind_reverse(fs_query *q, int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit)
{
    /* check for no possible bindings */
    for (int s=0; s<4; s++) {
        if (rids[s]->length == 1 && rids[s]->data[0] == FS_RID_NULL) {
            int slots = 0;
            if (flags & FS_BIND_MODEL) slots++;
            if (flags & FS_BIND_SUBJECT) slots++;
            if (flags & FS_BIND_PREDICATE) slots++;
            if (flags & FS_BIND_OBJECT) slots++;
            *result = calloc(slots, sizeof(fs_rid_vector));
            for (int s=0; s<slots; s++) {
                (*result)[s] = fs_rid_vector_new(0);
            }

            return 0;
        }
    }
        
    int ret = fsp_reverse_bind_all(q->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    if (ret) {
        fs_error(LOG_CRIT, "reverse bind failed");

        exit(1);
    }

    return ret;
}

static void warning_handler(void *user_data, raptor_locator* locator, const char *message)
{
    fs_query *q = user_data;

    char *msg = g_strdup_printf("parser warning: %s at line %d", message, raptor_locator_line(locator));
    q->warnings = g_slist_prepend(q->warnings, msg);
    fs_query_add_freeable(q, msg);
}

static void error_handler(void *user_data, raptor_locator* locator, const char *message)
{
    fs_query *q = user_data;

    char *msg = g_strdup_printf("parser error: %s at line %d", message, raptor_locator_line(locator));
    q->warnings = g_slist_prepend(q->warnings, msg);
    q->errors++;
    fs_query_add_freeable(q, msg);
}

guint fs_freq_hash(gconstpointer key)
{
    const fs_quad_freq *f = key;

    return (guint)(f->pri ^ f->sec);
}

gboolean fs_freq_equal(gconstpointer va, gconstpointer vb)
{
    const fs_quad_freq *a = va;
    const fs_quad_freq *b = vb;

    return a->pri == b->pri && a->sec == b->sec;
}

static void insert_freq(GHashTable *h, fs_quad_freq *f)
{
    fs_quad_freq *old = (fs_quad_freq *)g_hash_table_lookup(h, f);
    if (old) {
        old->freq += f->freq;
    } else {
        g_hash_table_insert(h, f, f);
    }
    fs_quad_freq ponly = *f;
    ponly.sec = FS_RID_NULL;
    old = (fs_quad_freq *)g_hash_table_lookup(h, &ponly);
    if (!old) {
        old = calloc(1, sizeof(fs_quad_freq));
        old->pri = ponly.pri;
        old->sec = ponly.sec;
        g_hash_table_insert(h, old, old);
    }
    old->freq += ponly.freq;
}

fs_query_state *fs_query_init(fsp_link *link)
{
    fs_query_state *qs = calloc(1, sizeof(fs_query_state));
    g_static_mutex_init(&qs->cache_mutex);
    qs->link = link;
    const char *features = fsp_link_features(link);
    qs->freq_available = strstr(features, " freq ") ? 1 : 0;
    if (qs->freq_available) {
        fs_quad_freq *freq;
        if (fsp_get_quad_freq_all(qs->link, FS_BIND_BY_SUBJECT, 1000, &freq)) {
            fs_error(LOG_ERR, "failed to get quad s freq data");
        } else {
            if (freq->freq) qs->freq_s = g_hash_table_new(fs_freq_hash, fs_freq_equal);
            for (fs_quad_freq *pos = freq; pos->freq; pos++) {
                insert_freq(qs->freq_s, pos);
            }
        }
        if (fsp_get_quad_freq_all(qs->link, FS_BIND_BY_OBJECT, 1000, &freq)) {
            fs_error(LOG_ERR, "failed to get quad o freq data");
        } else {
            if (freq->freq) qs->freq_o = g_hash_table_new(fs_freq_hash, fs_freq_equal);
            for (fs_quad_freq *pos = freq; pos->freq; pos++) {
                insert_freq(qs->freq_o, pos);
            }
        }
    }

#ifdef HAVE_RASQAL_WORLD
    qs->rasqal_world = rasqal_new_world();
    if (!qs->rasqal_world) {
        fs_error(LOG_ERR, "failed to get initialise rasqal world");
    }

#endif /* HAVE_RASQAL_WORLD */
    return qs;
}

int fs_query_fini(fs_query_state *qs)
{
    if (qs) {
#ifdef HAVE_RASQAL_WORLD
        if(qs->rasqal_world)
          rasqal_free_world(qs->rasqal_world);
#endif /* HAVE_RASQAL_WORLD */
        free(qs->bind_cache);
        g_static_mutex_free(&qs->cache_mutex);
        free(qs);
    }

    return 0;
}

fs_query *fs_query_execute(fs_query_state *qs, fsp_link *link, raptor_uri *bu, const char *query, int flags, int opt_level, int soft_limit)
{
    if (!qs) {
        fs_error(LOG_CRIT, "fs_query_execute() handed NULL query state");

        return NULL;
    }

    fsp_hit_limits_reset(link);

#ifndef HAVE_LAQRS
    while (isspace(*query)) {
        query++;
    }
    if (!strncasecmp(query, "EXPLAIN", 7)) {
        query += 7;
        flags |= FS_QUERY_EXPLAIN;
        while (isspace(*query)) {
            query++;
        }
    }
    if (!strncasecmp(query, "COUNT", 5)) {
        query += 5;
        flags |= FS_QUERY_COUNT;
    }
#endif

#ifdef HAVE_LAQRS
#ifndef HAVE_RASQAL_WORLD
    rasqal_query *rq = rasqal_new_query("laqrs", NULL);
#else /* HAVE_RASQAL_WORLD */
    rasqal_query *rq = rasqal_new_query(qs->rasqal_world, "laqrs", NULL);
#endif /* HAVE_RASQAL_WORLD */
    if (!rq) {
#ifndef HAVE_RASQAL_WORLD
        rq = rasqal_new_query("sparql", NULL);
#else /* HAVE_RASQAL_WORLD */
        rq = rasqal_new_query(qs->rasqal_world, "sparql", NULL);
#endif /* HAVE_RASQAL_WORLD */
    }
#else
#ifndef HAVE_RASQAL_WORLD
    rasqal_query *rq = rasqal_new_query("sparql", NULL);
#else /* HAVE_RASQAL_WORLD */
    rasqal_query *rq = rasqal_new_query(qs->rasqal_world, "sparql", NULL);
#endif /* HAVE_RASQAL_WORLD */
#endif
    if (!rq) {
        fs_error(LOG_ERR, "failed to initialise query system");

        return NULL;
   }

    fs_query *q = calloc(1, sizeof(fs_query));
    if (getenv("SHOW_TIMING")) {
        q->start_time = fs_time();
    }
    q->rq = rq;
    q->qs = qs;
    q->opt_level = opt_level;
    if (soft_limit) {
        q->soft_limit = soft_limit;
    } else {
        q->soft_limit = FS_FANOUT_LIMIT;
    }
    q->boolean = 1;
    rasqal_query_set_warning_handler(rq, q, warning_handler);
    rasqal_query_set_error_handler(rq, q, error_handler);
    int ret = rasqal_query_prepare(rq, (unsigned char *)query, bu);
    if (ret) {
	return q;
    }
#ifdef HAVE_LAQRS
    if (rasqal_query_get_explain(rq)) {
        flags |= FS_QUERY_EXPLAIN;
    }
#endif

    q->link = link;
    q->segments = fsp_link_segments(link);
    q->base = bu;
    rasqal_query_verb verb = rasqal_query_get_verb(rq);
#ifdef HAVE_LAQRS
    if (verb == RASQAL_QUERY_VERB_CONSTRUCT ||
        verb == RASQAL_QUERY_VERB_INSERT) {
#else
    if (verb == RASQAL_QUERY_VERB_CONSTRUCT) {
#endif
	q->construct = 1;
    }
    if (verb == RASQAL_QUERY_VERB_DESCRIBE) {
        q->describe = 1;
    }
    if (rasqal_query_get_order_condition(rq, 0)) {
        q->order = 1;
    }

    int explain = flags & FS_QUERY_EXPLAIN;

    //rasqal_query_print(rq, stdout);

#if 0
    /* This implements FROM, in a dumb and dangerous way, could be enabled by
     * some option */
    int idx = 0;
    int count = 0;
    FILE *errout = NULL;
    rasqal_data_graph *dg = rasqal_query_get_data_graph(rq, idx);
    if (dg) {
        errout = tmpfile();
    }
    while (dg) {
        fs_import(q->link, (char *)raptor_uri_as_string(dg->uri), (char *)raptor_uri_as_string(dg->name_uri), "auto", 0, 0, 0, errout, &count);
        dg = rasqal_query_get_data_graph(rq, ++idx);
    }
    if (idx) {
        fs_import_commit(q->link, 0, 0, 0, errout, &count);
        rewind(errout);
        while (!feof(errout)) {
            char tmp[1024];
            fgets(tmp, 1024, errout);
            char *msg = g_strdup(tmp);
            q->warnings = g_slist_prepend(q->warnings, msg);
            fs_query_add_freeable(q, msg);
        }
        fclose(errout);
    }
#endif
        
    q->limit = rasqal_query_get_limit(rq);
    q->offset = rasqal_query_get_offset(rq);

    raptor_sequence *vars = NULL;
    if (q->construct) {
	vars = raptor_new_sequence(NULL, NULL);
	for (int i=0; 1; i++) {
	    rasqal_triple *t = rasqal_query_get_construct_triple(rq, i);
	    if (!t) break;
	    check_cons_slot(q, vars, t->subject);
	    check_cons_slot(q, vars, t->predicate);
	    check_cons_slot(q, vars, t->object);
	}
    } else if (q->describe) {
        raptor_sequence *desc = rasqal_query_get_describe_sequence(rq);
        vars = raptor_new_sequence(NULL, NULL);
        for (int i=0; 1; i++) {
            rasqal_literal *l = raptor_sequence_get_at(desc, i);
            if (!l) break;
            switch (l->type) {
            case RASQAL_LITERAL_URI:
                /* we'll deal with these later */
                break;
            case RASQAL_LITERAL_VARIABLE:
                raptor_sequence_push(vars, l->value.variable);
                break;
            default:
                fs_error(LOG_ERR, "unexpected literal type");
                break;
            }
        }
    } else {
	vars = rasqal_query_get_bound_variable_sequence(rq);
    }
    if (!vars) {
	q->num_vars = 0;
	q->limit = 1;
    } else {
	q->num_vars = raptor_sequence_size(vars);
    }
    q->b = fs_binding_new(q->num_vars);

    for (int i=0; i < q->num_vars; i++) {
	rasqal_variable *v = raptor_sequence_get_at(vars, i);
	fs_binding_add(q->b, (char *)v->name, FS_RID_NULL, 1);
#ifdef HAVE_LAQRS
        if (v->expression) {
            fs_binding_set_expression(q->b, (char *)v->name, v->expression);
            q->expressions++;
        }
#endif
    }

    rasqal_graph_pattern *pattern = rasqal_query_get_query_graph_pattern(rq);
    q->flags = flags;
    if (q->construct || q->describe || rasqal_query_get_distinct(rq)) {
	q->flags |= FS_BIND_DISTINCT;
    }

    graph_pattern_walk(link, pattern, q, 0, 0, 0);
    graph_pattern_walk(link, pattern, q, 0, 1, 0);

    /* if we have more than one variable that has been prebound we need to
     * compute the combinatorial cross product of thier values so the we
     * correctly enumerate the possibiliities during execution. Fun. */ 
    int bound_variables = 0;
    int combinations = 1;
    int comb_factor[q->num_vars];
    for (int i=0; q->b[i].name; i++) {
        comb_factor[i] = 0;
    }
    for (int i=0; q->b[i].name; i++) {
        if (q->b[i].bound) {
            bound_variables++;
            comb_factor[i] = combinations;
            combinations *= q->b[i].vals->length;
        }
    }
    if (bound_variables > 1) {
        for (int i=0; q->b[i].name; i++) {
            if (!comb_factor[i]) {
                continue;
            }
            fs_rid_vector *newv = fs_rid_vector_new(0);
            for (int j=q->b[i].ubs->length; j<combinations; j++) {
                fs_rid_vector_append(q->b[i].ubs, 0);
            }
            for (int j=0; j<combinations; j++) {
                if (q->b[i].vals->length > 0) {
                    fs_rid_vector_append(newv, q->b[i].vals->data[(j / comb_factor[i]) % q->b[i].vals->length]);
                }
            }
            fs_rid_vector_free(q->b[i].vals);
            q->b[i].vals = newv;
        }
    }

#ifdef HAVE_LAQRS
    /* make sure variables in expressions are marked as needed */
    for (int i=0; i < q->num_vars; i++) {
	rasqal_variable *v = raptor_sequence_get_at(vars, i);
        if (v->expression) {
            check_variables(q, v->expression);
        }
    }
#endif

    q->bb[0] = q->b;
    for (int i=0; i <= q->block; i++) {
	if (i > 0) {
            if (q->union_group[i] != 0) {
                q->flags |= FS_BIND_UNION;
                q->flags &= ~FS_BIND_OPTIONAL;
            } else {
                q->flags &= ~FS_BIND_UNION;
                q->flags |= FS_BIND_OPTIONAL;
            }
	} else {
            q->flags &= ~FS_BIND_OPTIONAL;
            q->flags &= ~FS_BIND_UNION;
        }
	for (int j=0; j<q->blocks[i].length; j++) {
	    int chunk = fs_optimise_triple_pattern(q->qs, q,
	       (rasqal_triple **)(q->blocks[i].data), q->blocks[i].length, j);
	    /* execute triple pattern query */
	    if (explain) {
		printf("execute: ");
		if (!q->blocks[i].data[j]) {
		    printf("NULL");
		} else {
                    for (int k=0; k<chunk; k++) {
                        if (k) printf(" ");
                        rasqal_triple_print(q->blocks[i].data[j+k], stdout);
                    }
		}
                if (q->flags & FS_BIND_UNION) {
                    printf(" UNION");
                }
                if (q->flags & FS_BIND_OPTIONAL) {
                    printf(" OPTIONAL");
                }
                if (q->flags & FS_BIND_DISTINCT) {
                    printf(" DISTINCT");
                }
                if (q->flags & FS_BIND_SAME_MASK) {
                    printf(" SAME(?)");
                }
                if (q->soft_limit > 0) {
                    printf(" LIMIT %d", q->soft_limit);
                }
		printf("\n");
	    }
            int ret;
            if (chunk == 1) {
                ret = fs_handle_query_triple(q, i, q->blocks[i].data[j]);
            } else {
                rasqal_triple *in[chunk];
                for (int k=0; k<chunk; k++) {
                    in[k] = q->blocks[i].data[j+k];
                }
                ret = fs_handle_query_triple_multi(q, i, chunk, in);
                j += chunk-1;
            }
	    if (explain) {
		printf("%d bindings (%d)\n", fs_binding_length(q->b), ret);
	    }
            if (q->block == 0 && fs_binding_length(q->b) == 0) {
                q->boolean = 0;
            }
            if (ret == 0) {
                for (int var=0; q->b[var].name; var++) {
                    if (q->b[var].appears == i) {
                        fs_rid_vector_free(q->b[var].vals);
                        q->b[var].vals = NULL;
                        fs_rid_vector_free(q->b[var].ubs);
                        q->b[var].ubs = NULL;
                        q->b[var].vals = fs_rid_vector_new(fs_binding_length(q->b));
                        q->b[var].ubs = fs_rid_vector_new(fs_binding_length(q->b));
                        q->b[var].bound = 1;
                        for (int r=0; r<q->b[var].vals->length; r++) {
                            q->b[var].vals->data[r] = FS_RID_NULL;
                            q->b[var].ubs->data[r] = 0;
                        }
                    }
                }
                break;
            }
            /* if the query is false it must have failed */
            if (q->boolean == 0) {
                break;
            }
	}
        if (i > 0) {
printf("block join %d X %d\n", q->parent_block[i], i);
            fs_binding_merge(q, i, q->bb[i], q->bb[q->parent_block[i]], flags);
fs_binding_print(q->bb[0], stdout);
        }
    }

    if (explain) {
        double start_time = q->start_time;
	fs_query_free(q);
        q = calloc(1, sizeof(fs_query));
        q->start_time = start_time;
        q->flags = FS_QUERY_EXPLAIN;

	return q;
    }

    q->row = q->offset;
    if (q->row < 0) q->row = 0;
    q->lastrow = q->row;
    q->rows_output = 0;
    q->pending = calloc(q->segments, sizeof(fs_rid_vector *));
    for (int i=0; i<q->segments; i++) {
	q->pending[i] = fs_rid_vector_new(0);
    }

    if (q->flags & FS_BIND_DISTINCT) {
	for (int i=0; q->b[i].name; i++) {
	    if (q->b[i].proj || q->b[i].selected) {
		q->b[i].sort = 1;
	    } else {
		q->b[i].sort = 0;
	    }
	}
	fs_binding_sort(q->b);
	fs_binding_uniq(q->b);
    }

    /* this is neccesary because the DISTINCT phase may have
     * reduced the length of the projected columns */
    q->length = 0;
    for (int col=0; col < q->num_vars; col++) {
        if (!q->b[col].proj) continue;
        if (q->b[col].vals->length > q->length) {
            q->length = q->b[col].vals->length;
        }
    }

    /* if there are results we may need to apply FILTERs to check boolean
     * value */
    if (q->length > 0) {
        q->boolean = 0;
    }
    if (q->offset < 0) {
	q->offset = 0;
    }

    if (q->num_vars == 0) {
	/* ASK or similar */
        q->length = fs_binding_length(q->b);
        if (q->boolean && q->length == 0) q->length = 1;
        q->boolean = 0;

	return q;
    }

    if (q->flags & FS_QUERY_COUNT) {
        fs_binding_free(q->b);
        q->b = fs_binding_new();
        q->num_vars = 1;
        fs_binding_add(q->b, "count", 0, 1);
    }

    if (rasqal_query_get_order_condition(q->rq, 0)) {
	fs_query_order(q);
    }

    return q;
}

void fs_query_free(fs_query *q)
{
    if (q) {
        if (q->rq) rasqal_free_query(q->rq);
	fs_binding_free(q->b);
	if (q->resrow) free(q->resrow);
	if (q->ordering) free(q->ordering);
        if (q->pending) {
            for (int i=0; i<q->segments && q->pending; i++) {
                fs_rid_vector_free(q->pending[i]);
            }
            free(q->pending);
        }
	g_slist_free(q->warnings);
        if (q->blocks) {
            for (int i=0; i<FS_MAX_BLOCKS; i++) {
                if (q->blocks[i].data) free(q->blocks[i].data);
            }
        }

        GSList *it;
        for (it = q->free_list; it; it = it->next) {
            g_free(it->data);
        }
	g_slist_free(q->free_list);
        memset(q, 0, sizeof(fs_query));
	free(q);
    }
}

static void assign_slot(fs_query *q, rasqal_literal *l, int block)
{
    if (!q->b) {
	fs_error(LOG_ERR, "NULL binding");

	return;
    }
    if (l && l->type == RASQAL_LITERAL_VARIABLE) {
	char *vname = (char *)l->value.variable->name;
	if (l->value.variable->type == RASQAL_VARIABLE_TYPE_ANONYMOUS) {
	    char *tmp = vname;
	    vname = g_strdup_printf(":%s", tmp);
	    l->value.variable->name = (unsigned char *)vname;
	    free(tmp);
	    l->value.variable->type = RASQAL_VARIABLE_TYPE_NORMAL;
	}
	fs_binding *vb = fs_binding_get(q->b, vname);
	if (!vb) {
	    vb = fs_binding_add(q->b, vname, FS_RID_NULL, 0);
	}
	if (!vb->need_val && vb->appears != -1) {
	    vb->need_val = 1;
	}
	if (vb->appears == -1) {
	    vb->appears = block;
	}
    }
}

static void check_variables(fs_query *q, rasqal_expression *e)
{
    if (e->literal && e->literal->type == RASQAL_LITERAL_VARIABLE) {
	rasqal_variable *v = e->literal->value.variable;
	fs_binding *b = fs_binding_get(q->b, (char *)v->name);
	if (b) {
            b->need_val = 1;
            b->selected = 1;
        }

	return;
    }

    if (e->arg1) {
	check_variables(q, e->arg1);
    }
    if (e->arg2) {
	check_variables(q, e->arg2);
    }
    if (e->arg3) {
	check_variables(q, e->arg3);
    }
    if (e->args) {
        const int len = raptor_sequence_size(e->args);
        for (int i=0; i<len; i++) {
            rasqal_expression *ae = raptor_sequence_get_at(e->args, i);
            check_variables(q, ae);
        }
    }
}

static void filter_optimise_disjunct_equality(fs_query *q,
            rasqal_expression *e, int block, char **var, fs_rid_vector *res)
{
    /* check it's a disjuntive expression */
    if (e->op == RASQAL_EXPR_OR) {
        if (e->arg1->op == RASQAL_EXPR_EQ &&
            e->arg2->op == RASQAL_EXPR_OR) {
            /* recurse */
        } else if (e->arg1->op == RASQAL_EXPR_OR &&
                   e->arg2->op == RASQAL_EXPR_EQ) {
            /* recurse */
        } else if (e->arg1->op == RASQAL_EXPR_EQ &&
                   e->arg2->op == RASQAL_EXPR_EQ) {
            /* recurse */
        } else {
            fs_rid_vector_truncate(res, 0);

            return;
        }
    /* check it's var == const */
    } else if (e->op == RASQAL_EXPR_EQ) {
        rasqal_variable *v = NULL;
        fs_rid val, attr = FS_RID_NULL;
        if (e->arg1->op == RASQAL_EXPR_LITERAL &&
            e->arg1->literal->type == RASQAL_LITERAL_VARIABLE && 
            e->arg2->op == RASQAL_EXPR_LITERAL &&
            e->arg2->literal->type != RASQAL_LITERAL_VARIABLE) {
            v = e->arg1->literal->value.variable;
            val = const_literal_to_rid(q, e->arg2->literal, &attr);
        } else if (e->arg1->op == RASQAL_EXPR_LITERAL &&
            e->arg1->literal->type != RASQAL_LITERAL_VARIABLE && 
            e->arg2->op == RASQAL_EXPR_LITERAL &&
            e->arg2->literal->type == RASQAL_LITERAL_VARIABLE) {
            v = e->arg2->literal->value.variable;
            val = const_literal_to_rid(q, e->arg1->literal, &attr);
        } else {
            fs_rid_vector_truncate(res, 0);

            return;
        }
        if (val == FS_RID_NULL) return;
        /* if it's a value that can have multiple lexical forms for one
           conceptual value, then we need to check it with a real FILTER */
        if (attr == fs_c.xsd_integer || attr == fs_c.xsd_double ||
            attr == fs_c.xsd_float || attr == fs_c.xsd_decimal ||
            attr == fs_c.xsd_string || attr == fs_c.xsd_datetime) {
            fs_rid_vector_truncate(res, 0);

            return;
        }
        if (!*var) {
            *var = (char *)v->name;
        } else {
            if (strcmp(*var, (char *)v->name)) {
                /* disjunt variable name doesn't match previous, can't trivally
                 * optimise */
                fs_rid_vector_truncate(res, 0);

                return;
            }
        }
        fs_rid_vector_append(res, val);

        return;
    } else {
        fs_rid_vector_truncate(res, 0);

        return;
    }
        
    if (e->arg1) {
        filter_optimise_disjunct_equality(q, e->arg1, block, var, res);
    }
    if (e->arg2) {
        filter_optimise_disjunct_equality(q, e->arg2, block, var, res);
    }
    if (e->arg3) {
        filter_optimise_disjunct_equality(q, e->arg3, block, var, res);
    }
}

/* returns true if we have managed to optimise out the FILTER() expression */
static int filter_optimise(fs_query *q, rasqal_expression *e, int block)
{
    fs_rid_vector *res = fs_rid_vector_new(0);
    char *name = NULL;
    int retval = 0;
    filter_optimise_disjunct_equality(q, e, block, &name, res);
    if (res->length) {
        fs_binding *b = fs_binding_get(q->b, name);
        if (!b) {
            fs_error(LOG_ERR, "no binding for %s", name);

            return 0;
        }
        fs_rid_vector_append_vector(b->vals, res);
        for (int i=0; i<res->length; i++) res->data[i] = 0;
        fs_rid_vector_append_vector(b->ubs, res);
        b->bound = 1;
        retval = 1;
    }
    fs_rid_vector_free(res);

    return retval;
}

static void graph_pattern_walk(fsp_link *link, rasqal_graph_pattern *pattern,
	fs_query *q, int block, const int pass, int uni)
{
    if (!pattern) {
	return;
    }

    int op = rasqal_graph_pattern_get_operator(pattern);

    if (op == RASQAL_GRAPH_PATTERN_OPERATOR_OPTIONAL) {
	if (pass == 0) return;
	(q->block)++;
        q->parent_block[q->block] = block;
	block = q->block;
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_UNION) {
	if (pass == 0) return;
        (q->unions)++;
        for (int index=0; 1; index++) {
            rasqal_graph_pattern *sgp =
                        rasqal_graph_pattern_get_sub_graph_pattern(pattern, index);
            if (!sgp) break;
            graph_pattern_walk(link, sgp, q, block, pass, 1);
        }
    } else if (uni) {
	if (pass == 0) return;
	(q->block)++;
        q->union_group[q->block] = q->unions;
        q->parent_block[q->block] = block;
	block = q->block;
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_BASIC ||
	op == RASQAL_GRAPH_PATTERN_OPERATOR_GROUP ||
        op == RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH) {
        /* do nothing */
    } else {
	fs_error(LOG_ERR, "GP operator %d not supported treating as BGP", op);
    }

    if (block == 0 && pass == 1) {
	goto skip_assign;
    }

    for (int i=0; 1; i++) {
	rasqal_triple *t = rasqal_graph_pattern_get_triple(pattern, i);
	if (!t) break;

	fs_p_vector_append(q->blocks+q->block, (void *)t);
	assign_slot(q, t->origin, block);
	assign_slot(q, t->subject, block);
	assign_slot(q, t->predicate, block);
	assign_slot(q, t->object, block);
    }

skip_assign:

    for (int index=0; 1; index++) {
	rasqal_graph_pattern *sgp =
		    rasqal_graph_pattern_get_sub_graph_pattern(pattern, index);
	if (!sgp) break;
	graph_pattern_walk(link, sgp, q, block, pass, 0);
    }

    if (pass == 1) {
	raptor_sequence *s =
	    rasqal_graph_pattern_get_constraint_sequence(pattern);
	if (s) {
	    for (int c=0; 1; c++) {
		rasqal_expression *e = raptor_sequence_get_at(s, c);
		if (!e) break;

		check_variables(q, e);
		if (filter_optimise(q, e, block)) {
                    /* stop us from trying to evaluate this expression later */
                    raptor_sequence_set_at(s, c, NULL);
                }
	    }

	    if (q->constraints[block]) {
		raptor_sequence_join(q->constraints[block], s);
	    } else {
		q->constraints[block] = s;
	    }
	}
	for (int c=0; 1; c++) {
	    rasqal_expression *e = rasqal_query_get_order_condition(q->rq, c);
	    if (!e) break;
	    check_variables(q, e);
	}
    }
}

fs_rid_vector **fs_distinct_results(fs_rid_vector **r, int count)
{
    /* handle trivial cases */
    if (count == 0 || !r[0] || r[0]->length < 2) {
	return r;
    }

    /* inplace sort across the vectors */
    fs_rid_vector_array_sort(r, count, 0, r[0]->length - 1);

    fs_rid_vector *tmp[count];
    for (int i=0; i<count; i++) {
	tmp[i] = fs_rid_vector_new(0);
	fs_rid_vector_append(tmp[i], r[i]->data[0]);
    }

    /* uniq the results */
    int outp = 1;
    for (int i=1; i < r[0]->length; i++) {
	int same = 1;
	for (int j=0; j<count; j++) {
	    if (r[j]->data[i] != tmp[j]->data[outp-1]) {
		same = 0;
		break;
	    }
	}
	if (!same) {
	    for (int j=0; j<count; j++) {
		fs_rid_vector_append(tmp[j], r[j]->data[i]);
	    }
	    outp++;
	}
    }

    for (int i=0; i<count; i++) {
	fs_rid_vector_free(r[i]);
	r[i] = tmp[i];
    }
    
    return r;
}

static void desc_action(int flags, fs_rid_vector *slots[], char out[4][DESC_SIZE])
{
    for (int slot=0; slot<4; slot++) {
	if ((1 << slot) & flags) {
	    out[slot][0] = '?';
	    out[slot][1] = '\0';
	} else {
	    out[slot][0] = '_';
	    out[slot][1] = '\0';
	}
	if (slots[slot]->length > 0) {
	    for (int i=0; i < slots[slot]->length && i < 10; i++) {
		if (i == 0) {
		    strcat(out[slot], "[");
		} else {
		    strcat(out[slot], " ");
		}
		char tmp[256];
		sprintf(tmp, "%016llx", slots[slot]->data[i]);
		strcat(out[slot], tmp);
	    }
	    if (slots[slot]->length > 10) {
		sprintf(out[slot] + strlen(out[slot]), " ... (%d)", slots[slot]->length);
	    }
	    strcat(out[slot], "]");
	}
    }
}

static void check_occurances(char *pattern, char *vars[4], int bits, int *occur)
{
    char *a = NULL, *b = NULL;

    for (int s=0; s<4; s++) {
	if (pattern[s] == 'A') {
	    if (a == NULL && vars[s] != NULL) {
		a = vars[s];
	    } else {
		if (!a || !vars[s] || strcmp(a, vars[s])) {
		    return;
		}
	    }
	} else if (pattern[s] == 'B') {
	    if (b == NULL && vars[s] != NULL) {
		b = vars[s];
	    } else {
		if (!b || !vars[s] || strcmp(b, vars[s])) {
		    return;
		}
	    }
	}
    }

    *occur = bits;
}

static int bind_pattern(fs_query *q, int block, fs_binding *b, rasqal_triple *t, fs_rid_vector *slot[4], char *varnames[], int *numbindings, int *tobind)
{
    int ret;
    int bind = 0;

    ret = fs_bind_slot(q, block, b, t->origin, slot[0], &bind, &varnames[*numbindings], 0);
    if (ret) {
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind model failed");
#endif
        return ret;
    }
    if (bind || (*tobind & FS_BIND_MODEL && varnames[*numbindings])) {
	*tobind |= FS_BIND_MODEL;
	(*numbindings)++;
    } else {
        *tobind &= ~FS_BIND_MODEL;
    }
    ret = fs_bind_slot(q, block, b, t->subject, slot[1], &bind, &varnames[*numbindings], 0);
    if (ret) {
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind subject failed");
#endif
        return ret;
    }
    if (bind) {
	for (int i=0; i<*numbindings; i++) {
	    if (!strcmp(varnames[i], varnames[*numbindings])) {
		bind = 0;
		break;
	    }
	}
    }
    if (bind || (*tobind & FS_BIND_SUBJECT && varnames[*numbindings])) {
	*tobind |= FS_BIND_SUBJECT;
	(*numbindings)++;
    } else {
        *tobind &= ~FS_BIND_SUBJECT;
    }
    ret = fs_bind_slot(q, block, b, t->predicate, slot[2], &bind, &varnames[*numbindings], 0);
    if (ret) {
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind predicte failed");
#endif
        return ret;
    }
    if (bind) {
	for (int i=0; i<*numbindings; i++) {
	    if (!strcmp(varnames[i], varnames[*numbindings])) {
		bind = 0;
		break;
	    }
	}
    }
    if (bind || (*tobind & FS_BIND_PREDICATE && varnames[*numbindings])) {
	*tobind |= FS_BIND_PREDICATE;
	(*numbindings)++;
    } else {
        *tobind &= ~FS_BIND_PREDICATE;
    }
    ret = fs_bind_slot(q, block, b, t->object, slot[3], &bind, &varnames[*numbindings], 1);
    if (ret) {
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind object failed");
#endif
        return ret;
    }
    if (bind) {
	for (int i=0; i<*numbindings; i++) {
	    if (!strcmp(varnames[i], varnames[*numbindings])) {
		bind = 0;
		break;
	    }
	}
    }
    if (bind || (*tobind & FS_BIND_OBJECT && varnames[*numbindings])) {
	*tobind |= FS_BIND_OBJECT;
	(*numbindings)++;
    } else {
        *tobind &= ~FS_BIND_OBJECT;
    }

    /* this is just an optimisation, it saves sending stuff
       over the wire unneccesarily */
    for (int i=0; i < 4; i++) {
	fs_rid_vector_uniq(slot[i], 0);
    }

    /* check for co-occurance of variables in pattern */
    char *vars[4] = {
	t->origin && t->origin->type == RASQAL_LITERAL_VARIABLE ?
	    (char *)t->origin->value.variable->name : NULL,
	t->subject->type == RASQAL_LITERAL_VARIABLE ?
	    (char *)t->subject->value.variable->name : NULL,
	t->predicate->type == RASQAL_LITERAL_VARIABLE ?
	    (char *)t->predicate->value.variable->name : NULL,
	t->object->type == RASQAL_LITERAL_VARIABLE ?
	    (char *)t->object->value.variable->name : NULL
    };

    int occur = 0;
    if (vars[0] && vars[1] && !strcmp(vars[0], vars[1])) occur++;
    if (vars[0] && vars[2] && !strcmp(vars[0], vars[2])) occur++;
    if (vars[0] && vars[3] && !strcmp(vars[0], vars[3])) occur++;
    if (vars[1] && vars[2] && !strcmp(vars[1], vars[2])) occur++;
    if (vars[1] && vars[3] && !strcmp(vars[1], vars[3])) occur++;
    if (vars[2] && vars[3] && !strcmp(vars[2], vars[3])) occur++;

    if (occur == 0) return 0;

    occur = 0;

#define CHECK_O(p) check_occurances(#p, vars, FS_BIND_SAME_ ## p, &occur)
    CHECK_O(XXAA);
    CHECK_O(XAXA);
    CHECK_O(XAAX);
    CHECK_O(XAAA);
    CHECK_O(AXXA);
    CHECK_O(AXAX);
    CHECK_O(AXAA);
    CHECK_O(AAXX);
    CHECK_O(AAXA);
    CHECK_O(AAAX);
    CHECK_O(AAAA);
    CHECK_O(AABB);
    CHECK_O(ABAB);
    CHECK_O(ABBA);

    if (occur == 0) {
	fs_error(LOG_CRIT, "found co-occuring variables, but could not "
		 "identify pattern");
    }

    *tobind |= occur;

    return 0;
}

/*

Documentation for bind() return results:

results = NULL
    This menas that the bind failed, ie. there were no matches, but you only
    see this if cols==0.

results = { vector->length == 0 }
    This means that the bind failed (no matches) if cols > 0.
    The bind succeeded if cols == 0.

results = { vector->length > 0 )
     The bind succeeded.

*/

static int process_results(fs_query *q, int block, fs_binding *oldb,
        fs_binding *b, int flags,
	fs_rid_vector *results[], char *varnames[], int numbindings,
	fs_rid_vector *slot[4])
{
    int ret = 0;

    if (results) {
        /* if there are no bindings in results, but it didn't fail */
        if (!results[0]) {
            for (int i=0; oldb[i].name; i++) {
                fs_rid_vector_append_vector(b[i].vals, oldb[i].vals);
                fs_rid_vector_append_vector(b[i].ubs, oldb[i].ubs);
                b[i].bound |= oldb[i].bound;
            }
            free(results);
            for (int x=0; x<4; x++) {
                fs_rid_vector_clear(slot[x]);
            }

            return 1;
        }

	if (flags & FS_BIND_DISTINCT) {
	    results = fs_distinct_results(results, numbindings);
	}

	for (int col=0; col<numbindings; col++) {
	    if (varnames[col]) {
                fs_binding *bv = fs_binding_get(b, varnames[col]);
                /* if the varaible is repeated in one triple pattern then it
                 * will appear more than once, but we need to make sure not to
                 * add it twice */
                if (col > 0 && fs_binding_length(bv) > 0) {
                    int repeat = 0;
                    for (int colb = 0; colb < col; colb++) {
                        if (!strcmp(varnames[col], varnames[colb])) {
                            repeat = 1;
                            break;
                        }
                    }
                    if (repeat) continue;
                }
                if (!bv) {
                    fs_error(LOG_CRIT, "unmatched variable name '%s'", varnames[col]);
                    continue;
                }
                if (q->union_group[block] > 0 &&
                    q->union_group[bv->appears] > 0) {
                    for (int r=0; results[col] && r<results[col]->length; r++) {
                        fs_rid_vector_append(bv->ubs, block);
                    }
                } else {
                    for (int r=0; results[col] && r<results[col]->length; r++) {
                        fs_rid_vector_append(bv->ubs, 0);
                    }
                }
		fs_binding_add_vector(b, varnames[col], results[col]);
                ret += results[col] ? results[col]->length : 0;
	    } else {
		fs_error(LOG_ERR, "column %d has no varname in sub results", col);
	    }
	    fs_rid_vector_free(results[col]);
	}
        free(results);
        /* we don't want the merge to try and do the OPTINAL etc. logic yet */
        int mergeflags = flags & (FS_BIND_DISTINCT);
        fs_binding_merge(q, block, oldb, b, mergeflags);
    } else {
        if (!(flags & (FS_BIND_OPTIONAL | FS_BIND_UNION))) {
            q->boolean = 0;
        }
    }
    fs_binding_free(oldb);

    return ret;
}

static int fs_handle_query_triple(fs_query *q, int block, rasqal_triple *t)
{
    fs_rid_vector *slot[4];
    slot[0] = fs_rid_vector_new(0);
    slot[1] = fs_rid_vector_new(0);
    slot[2] = fs_rid_vector_new(0);
    slot[3] = fs_rid_vector_new(0);
    int ret = 0;

    fs_binding *b;
    if (!q->bb[block]) {
        /* copy the binding table from the parent block */
        q->bb[block] = fs_binding_copy(q->bb[q->parent_block[block]]);
    }
    b = q->bb[block];
    int tobind = q->flags;

    const int explain = tobind & FS_QUERY_EXPLAIN;
#if 0
    const int optional = tobind & FS_BIND_OPTIONAL;
#endif
    const int optional = 0;
    const int union_block = optional ? 0 : block;

    char *varnames[4] = { NULL, NULL, NULL, NULL };
    if (!optional) fs_binding_clear_used_all(b);
    fs_binding *oldb = fs_binding_copy_and_clear(b);
    fs_rid_vector **results = NULL;

    /* if theres a patterns with lots of bindings for the subject and one
     * predicate we can bind_many it */
    if (fs_opt_is_const(oldb, t->subject, union_block) &&
        (fs_opt_num_vals(oldb, t->subject) <= fs_opt_num_vals(oldb, t->object) ||
        (t->predicate->type == RASQAL_LITERAL_URI &&
         !strcmp((char *)raptor_uri_as_string(t->predicate->value.uri), RDF_TYPE)))) {
	int numbindings = 0;
	tobind |= FS_BIND_SUBJECT;

	if (bind_pattern(q, block, oldb, t, slot, varnames, &numbindings, &tobind)) {
            for (int x=0; x<4; x++) {
                fs_rid_vector_free(slot[x]);
            }
#ifdef DEBUG_BIND
            fs_error(LOG_ERR, "bind_pattern failed");
#endif

	    return 0;
	}

        char *scope = NULL;
        if (slot[1]->length > 0) {
            fs_bind_cache_wrapper(q->qs, q, 0, tobind | FS_BIND_BY_SUBJECT,
                     slot, &results, -1, q->order ? -1 : q->soft_limit);
            scope = "mmmm";
        } else {
            /* by unbinding things bound in UNION blocks we've made it not
             * possible to bind many anymore */
            fs_bind_cache_wrapper(q->qs, q, 1, tobind | FS_BIND_BY_SUBJECT,
                     slot, &results, -1, q->order ? -1 : q->soft_limit);
            scope = "UUUU";
        }
	if (explain) {
	    char desc[4][DESC_SIZE];
	    desc_action(tobind, slot, desc);
	    printf("%ss (%s,%s,%s,%s) -> %d\n", scope, desc[0], desc[1], desc[2], desc[3], results ? (results[0] ? results[0]->length : -1) : -2);
	}

        ret = process_results(q, block, oldb, b, tobind, results, varnames, numbindings, slot);
        for (int x=0; x<4; x++) {
            fs_rid_vector_free(slot[x]);
        }

        return ret;
    }

    /* if theres a patterns with lots of bindings for the object and one
     * predicate we can bind_many it */
    if (fs_opt_is_const(oldb, t->object, union_block)) {
	int numbindings = 0;
	tobind |= FS_BIND_OBJECT;

	if (bind_pattern(q, block, oldb, t, slot, varnames, &numbindings, &tobind)) {
            for (int x=0; x<4; x++) {
                fs_rid_vector_free(slot[x]);
            }
#ifdef DEBUG_BIND
            fs_error(LOG_ERR, "bind_pattern failed");
#endif

	    return 0;
	}

        char *scope = NULL;
        fs_bind_cache_wrapper(q->qs, q, 1, tobind | FS_BIND_BY_OBJECT,
                 slot, &results, -1, q->order ? -1 : q->soft_limit);
        scope = "NNNN";
	if (explain) {
	    char desc[4][DESC_SIZE];
	    desc_action(tobind, slot, desc);
	    printf("%so (%s,%s,%s,%s) -> %d\n", scope, desc[0], desc[1], desc[2], desc[3], results ? (results[0] ? results[0]->length : -1) : -2);
	}

	ret = process_results(q, block, oldb, b, tobind, results, varnames, numbindings, slot);
        for (int x=0; x<4; x++) {
            fs_rid_vector_free(slot[x]);
        }

        return ret;
    }

    /* there are no constant terms in the subject or object slot, so we
     * need to bind_all. We used to (before r872) enumerate the possibilities
     * at this stage, but I can no longer see why that would be a
     * good idea, and iters had the wrong value in it */
    int numbindings = 0;

    if (bind_pattern(q, block, oldb, t, slot, varnames, &numbindings, &tobind)) {
        for (int x=0; x<4; x++) {
            fs_rid_vector_free(slot[x]);
        }
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind_pattern failed");
#endif

        return 0;
    }

    fs_bind_cache_wrapper(q->qs, q, 1, tobind | FS_BIND_BY_SUBJECT,
             slot, &results, -1, q->order ? -1 : q->soft_limit);
    if (explain) {
        char desc[4][DESC_SIZE];
        desc_action(tobind, slot, desc);
        printf("nnnns (%s,%s,%s,%s) -> %d\n", desc[0], desc[1], desc[2], desc[3], results ? (results[0] ? results[0]->length : -1) : -2);
    }

    ret = process_results(q, block, oldb, b, tobind, results, varnames, numbindings, slot);
    for (int x=0; x<4; x++) {
	fs_rid_vector_free(slot[x]);
    }

    return ret;
}

/* this handles multiple triples that the optimiser believes can be dealt with
 * as one bind operation */
static int fs_handle_query_triple_multi(fs_query *q, int block, int count, rasqal_triple *t[])
{
    fs_rid_vector *slot[4];
    slot[0] = fs_rid_vector_new(0);
    slot[1] = fs_rid_vector_new(0);
    slot[2] = fs_rid_vector_new(0);
    slot[3] = fs_rid_vector_new(0);
    int ret = 0;

    fs_binding *b = q->b;
    int tobind = q->flags;

    const int explain = tobind & FS_QUERY_EXPLAIN;
    const int optional = tobind & FS_BIND_OPTIONAL;
    const int union_block = optional ? 0 : block;

    char *varnames[4] = { NULL, NULL, NULL, NULL };
    if (!optional) fs_binding_clear_used_all(b);
    fs_binding *oldb = fs_binding_copy_and_clear(b);
    fs_rid_vector **results = NULL;

    /* check the patterns are all of the form { ?s <const> <const> . } */
    for (int i=0; i<count; i++) {
        if (fs_opt_is_const(oldb, t[i]->subject, union_block)) {
            fs_error(LOG_ERR, "bad const subject argument to fs_handle_query_triple_multi()");
            return 0;
        }
        if (!fs_opt_is_const(oldb, t[i]->object, union_block) || !fs_opt_is_const(oldb, t[i]->predicate, union_block)) {
            fs_error(LOG_ERR, "bad non const object/predicate argument to fs_handle_query_triple_multi()");
            return 0;
        }
    }
    
    int numbindings = 0;
    tobind |= FS_BIND_SUBJECT;

    if (bind_pattern(q, block, oldb, t[0], slot, varnames, &numbindings, &tobind)) {
        for (int x=0; x<4; x++) {
            fs_rid_vector_free(slot[x]);
        }
#ifdef DEBUG_BIND
        fs_error(LOG_ERR, "bind_pattern failed");
#endif

        return 0;
    }
    for (int i=1; i<count; i++) {
        int bind;
        char *v;
        fs_bind_slot(q, block, oldb, t[i]->origin, slot[0], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->subject, slot[1], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->predicate, slot[2], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->object, slot[3], &bind, &v, 1);
    }
    bind_reverse(q, tobind | FS_BIND_BY_SUBJECT,
                 slot, &results, -1, q->order ? -1 : q->soft_limit);
    if (explain) {
        char desc[4][DESC_SIZE];
        desc_action(tobind, slot, desc);
        printf("nnnnr (%s,%s,%s,%s) -> %d\n", desc[0], desc[1], desc[2], desc[3], results ? (results[0] ? results[0]->length : -1) : -2);
    }

    ret = process_results(q, block, oldb, b, tobind, results, varnames, numbindings, slot);
    for (int x=0; x<4; x++) {
        fs_rid_vector_free(slot[x]);
    }

    return ret;
}

static fs_rid const_literal_to_rid(fs_query *q, rasqal_literal *l, fs_rid *attr)
{
    switch (l->type) {
	case RASQAL_LITERAL_URI: {
            char *uri = (char *)raptor_uri_as_string(l->value.uri);
            return fs_hash_uri(uri);
        }
	case RASQAL_LITERAL_STRING: {
            *attr = fs_c.empty;
            if (l->language) {
                char *langtag = g_utf8_strup(l->language, -1);
                *attr = fs_hash_literal(langtag, 0);
                g_free(langtag);
            } else if (l->datatype) {
                *attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
            }
	    return fs_hash_literal((char *)l->string, *attr);
        }
	case RASQAL_LITERAL_BOOLEAN:
            *attr = fs_c.xsd_boolean;
	    return fs_hash_literal(l->value.integer ?
			"true" : "false", *attr);
	case RASQAL_LITERAL_INTEGER:
            *attr = fs_c.xsd_integer;
	    return fs_hash_literal((char *)l->string, *attr);
	case RASQAL_LITERAL_DOUBLE:
            *attr = fs_c.xsd_double;
	    return fs_hash_literal((char *)l->string, *attr);
	case RASQAL_LITERAL_FLOAT:
            *attr = fs_c.xsd_float;
	    return fs_hash_literal((char *)l->string, *attr);
	case RASQAL_LITERAL_DECIMAL:
            *attr = fs_c.xsd_decimal;
	    return fs_hash_literal((char *)l->string, *attr);
	case RASQAL_LITERAL_DATETIME:
            *attr = fs_c.xsd_datetime;
	    return fs_hash_literal((char *)l->string, *attr);
	case RASQAL_LITERAL_VARIABLE:
            /* not const, don't handle here */
            break;
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
	case RASQAL_LITERAL_UNKNOWN:
	    fs_error(LOG_ERR, "error: found unhandled literal type %d", l->type);
            break;
    }
    *attr = FS_RID_NULL;

    return FS_RID_NULL;
}

int fs_bind_slot(fs_query *q, int block, fs_binding *b,
        rasqal_literal *l, fs_rid_vector *v, int *bind, char **vname,
        int lit_allowed)
{
    *bind = 0;

    if (!l) return 0;
    fs_rid attr;
    *vname = NULL;

    switch (l->type) {
	case RASQAL_LITERAL_VARIABLE:
	    *vname = (char *)l->value.variable->name;
	    fs_binding *vb = fs_binding_get(b, *vname);
	    if (!vb) {
		break;
	    }

            int bound_in_this_union = 0;
            if (block != -1) {
                (vb->bound_in_block[block])++;
                if (q->union_group[block] > 0 &&
                    q->union_group[block] == q->union_group[vb->appears] &&
                    vb->bound_in_block[block] == 1) {
                    bound_in_this_union = 1;
                }
            }
	    if ((!vb->bound || bound_in_this_union) && vb->need_val) {
		*bind = 1;
                if (block != -1) fs_binding_set_used(b, *vname);
	    } else if (vb->bound) {
		*bind = 1;
                if (block != -1) fs_binding_set_used(b, *vname);
                if (lit_allowed) {
                    fs_rid_vector_append_vector_no_nulls(v, vb->vals);
                } else {
                    fs_rid_vector_append_vector_no_nulls_lit(v, vb->vals);
                }
                if (v->length == 0) {
                    fs_rid_vector_append(v, FS_RID_NULL);
                }
	    }
	    break;
	case RASQAL_LITERAL_URI: {
            char *uri = (char *)raptor_uri_as_string(l->value.uri);
            fs_rid_vector_append(v, fs_hash_uri(uri));
	    break;
        }
	case RASQAL_LITERAL_STRING:
	    if (!lit_allowed) {
		return 1;
	    }
            if (l->language) {
                char *langtag = g_utf8_strup(l->language, -1);
                attr = fs_hash_literal(langtag, 0);
                g_free(langtag);
            } else if (l->datatype) {
                attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
            } else {
                attr = fs_c.empty;
            }
	    fs_rid_vector_append(v,
		    fs_hash_literal((char *)l->string, attr));
	    break;
	case RASQAL_LITERAL_BOOLEAN:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal(l->value.integer ?
			"true" : "false", fs_c.xsd_boolean));
	    break;
	case RASQAL_LITERAL_INTEGER:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal((char *)l->string, fs_c.xsd_integer));
	    break;
	case RASQAL_LITERAL_DOUBLE:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal((char *)l->string, fs_c.xsd_double));
	    break;
	case RASQAL_LITERAL_FLOAT:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal((char *)l->string, fs_c.xsd_float));
	    break;
	case RASQAL_LITERAL_DECIMAL:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal((char *)l->string, fs_c.xsd_decimal));
	    break;
	case RASQAL_LITERAL_DATETIME:
	    if (!lit_allowed) {
		return 1;
	    }
	    fs_rid_vector_append(v, fs_hash_literal((char *)l->string, fs_c.xsd_datetime));
	    break;
	case RASQAL_LITERAL_BLANK:
	case RASQAL_LITERAL_PATTERN:
	case RASQAL_LITERAL_QNAME:
	case RASQAL_LITERAL_UNKNOWN:
	    fs_error(LOG_ERR, "error: found unhandled literal type %d", l->type);
	    break;
    }

    return 0;
}

double fs_query_start_time(fs_query *q)
{
    if (q) {
        return q->start_time;
    }

    return 0.0;
}

int fs_query_flags(fs_query *q)
{
    if (q) return q->flags;

    return 0;
}

int fs_query_errors(fs_query *q)
{
    if (q) return q->errors;

    return 1;
}

/* vi:set expandtab sts=4 sw=4: */
