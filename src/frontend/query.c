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
#include "debug.h"
#include "common/4store.h"
#include "common/datatypes.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"
#include "common/rdf-constants.h"

//#define DEBUG_BIND

#define DESC_SIZE 1024

#define DEBUG_SIZE(n, thing) printf("@@ %d * sizeof(%s) = %zd\n", n, #thing, n * sizeof(thing))

static void graph_pattern_walk(fsp_link *link, rasqal_graph_pattern *p, fs_query *q, rasqal_literal *model, int optional, int uni);
static int fs_handle_query_triple(fs_query *q, int block, rasqal_triple *t);
static int fs_handle_query_triple_multi(fs_query *q, int block, int count, rasqal_triple *t[]);
static fs_rid const_literal_to_rid(fs_query *q, rasqal_literal *l, fs_rid *attr);
static void check_variables(fs_query *q, rasqal_expression *e, int dont_select);
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
        fs_error(LOG_ERR, "failed to allocate rasqal world");
    }
    if (rasqal_world_open(qs->rasqal_world)) {
        fs_error(LOG_ERR, "failed to intialise rasqal world");
	fs_query_fini(qs);
	return NULL;
    }
#endif /* HAVE_RASQAL_WORLD */

    return qs;
}

int fq_query_have_laqrs(void)
{
    int laqrs = 0;

#ifdef HAVE_RASQAL_WORLD
    rasqal_world *w = rasqal_new_world();
    if (!w) {
        fs_error(LOG_ERR, "failed to allocate rasqal world");
        return 0;
    }
    if (rasqal_world_open(w)) {
        fs_error(LOG_ERR, "failed to initialise rasqal world");
	rasqal_free_world(w);
	return 0;
    }
    rasqal_query *rq = rasqal_new_query(w, "laqrs", NULL);
#else
    rasqal_query *rq = rasqal_new_query("laqrs", NULL);
#endif /* HAVE_RASQAL_WORLD */
    if (rq) {
        laqrs = 1;
        rasqal_free_query(rq);
    }
#ifdef HAVE_RASQAL_WORLD
    rasqal_free_world(w);
#endif

    return laqrs;
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

static void tree_compact(fs_query *q)
{
    for (int block = 1; block <= q->block; block++) {
        int parent = q->parent_block[block];
        if (q->join_type[block] == FS_INNER &&
            (q->constraints[block] == NULL || q->constraints[parent] == NULL)) {
            /* if there's nothing special about this block, merge up */
            fs_p_vector_append_vector(q->blocks+parent, q->blocks+block);
            if (q->constraints[parent] == NULL) {
                q->constraints[parent] = q->constraints[block];
                q->constraints[block] = NULL;
            }
            for (int col=0; q->bb[0][col].name; col++) {
                if (q->bb[0][col].appears == block) {
                    q->bb[0][col].appears = parent;
                }
            }
#ifdef DEBUG_MERGE
printf("Merge B%d to B%d\n", block, parent);
#endif
            q->blocks[block].length = 0;
            for (int j=1; j<=q->block; j++) {
                if (q->parent_block[j] == block) {
                    q->parent_block[j] = parent;
                }
            }
        }
    }
}

fs_query *fs_query_execute(fs_query_state *qs, fsp_link *link, raptor_uri *bu, const char *query, int flags, int opt_level, int soft_limit)
{
    if (!qs) {
        fs_error(LOG_CRIT, "fs_query_execute() handed NULL query state");

        return NULL;
    }

    fsp_hit_limits_reset(link);

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
    if (verb == RASQAL_QUERY_VERB_ASK) {
        q->ask = 1;
    }
    if (verb == RASQAL_QUERY_VERB_DESCRIBE) {
        q->describe = 1;
    }
    if (rasqal_query_get_order_condition(rq, 0)) {
        q->order = 1;
    }

    if (flags & FS_QUERY_DEFAULT_GRAPH) {
        flags &= ~FS_QUERY_DEFAULT_GRAPH;
        q->default_graphs = fs_rid_vector_new(1);
        q->default_graphs->data[0] = FS_DEFAULT_GRAPH_RID;
    }

    int explain = flags & FS_QUERY_EXPLAIN;
#ifdef DEBUG_MERGE
    explain = 1;
    //rasqal_query_print(rq, stdout);
#endif

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
        
    for (int i=0; 1; i++) {
        rasqal_data_graph *dg = rasqal_query_get_data_graph(rq, i);
        if (!dg) break;
        if (i == 0 && q->default_graphs) {
            q->default_graphs->length = 0;
        }
        char *uri = (char *)raptor_uri_as_string(dg->uri);
        char *name_uri = (char *)raptor_uri_as_string(dg->name_uri);
        if (name_uri) {
            q->warnings = g_slist_prepend(q->warnings, "FROM NAMED is not currently supported");
        } else {
            if (!q->default_graphs) {
                q->default_graphs = fs_rid_vector_new(0);
            }
            fs_rid default_rid = fs_hash_uri(uri);
            fs_rid_vector_append(q->default_graphs, default_rid);
        }
    }

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
#if RASQAL_VERSION >= 917
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
#else
        fs_error(LOG_INFO, "sorry, describe is not supported by your version of rasqal");
	q->num_vars = 0;
	q->boolean = 0;

        return q;
#endif
    } else {
	vars = rasqal_query_get_bound_variable_sequence(rq);
    }
    if (!vars) {
	q->num_vars = 0;
	q->limit = 1;
    } else {
	q->num_vars = raptor_sequence_size(vars);
    }
    q->bb[0] = fs_binding_new();
    q->bt = q->bb[0];

    for (int i=0; i < q->num_vars; i++) {
	rasqal_variable *v = raptor_sequence_get_at(vars, i);
	fs_binding_add(q->bb[0], (char *)v->name, FS_RID_NULL, 1);
#ifdef HAVE_LAQRS
        if (v->expression) {
            fs_binding_set_expression(q->bb[0], (char *)v->name, v->expression);
            q->expressions++;
        }
#endif
    }

    rasqal_graph_pattern *pattern = rasqal_query_get_query_graph_pattern(rq);
    q->flags = flags;
    if (q->construct || q->describe || rasqal_query_get_distinct(rq)) {
	q->flags |= FS_BIND_DISTINCT;
    }

    graph_pattern_walk(link, pattern, q, NULL, 0, 0);

    tree_compact(q);

    /* if we have more than one variable that has been prebound we need to
     * compute the combinatorial cross product of thier values so the we
     * correctly enumerate the possibiliities during execution. Fun. */ 
    int bound_variables = 0;
    int combinations = 1;
    int comb_factor[q->num_vars];
    for (int i=0; q->bb[0][i].name; i++) {
        comb_factor[i] = 0;
    }
    for (int i=0; q->bb[0][i].name; i++) {
        if (q->bb[0][i].bound) {
            bound_variables++;
            comb_factor[i] = combinations;
            combinations *= q->bb[0][i].vals->length;
        }
    }
    if (bound_variables > 1) {
        for (int i=0; q->bb[0][i].name; i++) {
            if (!comb_factor[i]) {
                continue;
            }
            fs_rid_vector *newv = fs_rid_vector_new(0);
            for (int j=0; j<combinations; j++) {
                if (q->bb[0][i].vals->length > 0) {
                    fs_rid_vector_append(newv, q->bb[0][i].vals->data[(j / comb_factor[i]) % q->bb[0][i].vals->length]);
                }
            }
            fs_rid_vector_free(q->bb[0][i].vals);
            q->bb[0][i].vals = newv;
        }
    }

#ifdef HAVE_LAQRS
    /* make sure variables in expressions are marked as needed */
    for (int i=0; i < q->num_vars; i++) {
	rasqal_variable *v = raptor_sequence_get_at(vars, i);
        if (v->expression) {
            check_variables(q, v->expression, 0);
        }
    }
#endif

    for (int i=0; i <= q->block; i++) {
#if DEBUG_MERGE
printf("Processing B%d, parent is B%d\n", i, q->parent_block[i]);
#endif
        if (q->blocks[i].length == 0) {
            continue;
        }
        if (!q->bb[i]) {
            int tocopy = q->parent_block[i];
            while (!q->bb[tocopy]) {
                tocopy = q->parent_block[tocopy];
                if (tocopy == 0) break;
            }
            q->bb[i] = fs_binding_copy(q->bb[tocopy]);
        }
	for (int j=0; j<q->blocks[i].length; j++) {
	    int chunk = fs_optimise_triple_pattern(q->qs, q, i,
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
		printf("%d bindings (%d)\n", fs_binding_length(q->bb[i]), ret);
	    }
            if (q->block < 2 && ret == 0) {
                q->boolean = 0;
            }
            if (ret == 0) {
                for (int var=0; q->bb[i][var].name; var++) {
                    if (q->bb[0][var].appears == i) {
                        fs_rid_vector_free(q->bb[i][var].vals);
                        q->bb[i][var].vals = NULL;
                        q->bb[i][var].vals = fs_rid_vector_new(fs_binding_length(q->bb[i]));
                        q->bb[i][var].bound = 1;
                        for (int r=0; r<q->bb[i][var].vals->length; r++) {
                            q->bb[i][var].vals->data[r] = FS_RID_NULL;
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
#if DEBUG_MERGE > 1
printf("table after processing B%d:\n", i);
fs_binding_print(q->bb[i], stdout);
printf("\n");
#endif
    }

    /* perform all the UNIONs */
    int first_in_union;
    for (int i=q->unions; i>0; i--) {
        first_in_union = 0;
        for (int j=1; j<=q->block; j++) {
            if (q->union_group[j] == i) {
                if (first_in_union == 0) {
                    first_in_union = j;
                    if (q->constraints[j]) {
                        fs_binding *old = q->bb[j];
                        q->bb[j] = fs_binding_apply_filters(q, j, q->bb[j], q->constraints[j]);
                        fs_binding_free(old);
                        q->constraints[j] = NULL;
                    }
                } else {
                    if (q->constraints[j]) {
                        fs_binding *old = q->bb[j];
                        q->bb[j] = fs_binding_apply_filters(q, j, q->bb[j], q->constraints[j]);
                        fs_binding_free(old);
                        q->constraints[j] = NULL;
                    }
                    fs_binding_union(q, q->bb[first_in_union], q->bb[j]);
                    fs_binding_free(q->bb[j]);
                    q->bb[j] = NULL;
#ifdef DEBUG_MERGE
                    printf("B%d = B%d UNION B%d\n", first_in_union, first_in_union, j);
                    fs_binding_print(q->bb[first_in_union], stdout);
                    printf("\n");
#endif
                }
            }
        }
    }

    /* run through the blocks in the correct order to do the joins */
    for (int i=q->block; i>=0; i--) {
        int start = i > 1 ? i : 1;
        for (int j=start; j<=q->block; j++) {
            if (!q->bb[j]) {
#ifdef DEBUG_MERGE
                printf("skipping B%d, no bindings\n", j);
#endif
                continue;
            }
            if (q->parent_block[j] == i) {
                if (q->join_type[j] == FS_INNER) {
#ifdef DEBUG_MERGE
                    printf("block join B%d [X] B%d\n", i, j);
#endif
                    /* It's an normal join */
                    if (q->bb[i]) {
                        fs_binding *nb = fs_binding_join(q, q->bb[i], q->bb[j], FS_INNER);
                        fs_binding_free(q->bb[i]);
                        q->bb[i] = nb;
                        if (i == 0) q->bt = q->bb[i];
                        fs_binding_free(q->bb[j]);
                        q->bb[j] = NULL;
                    } else {
                        /* Bi is empty, just replace it */
                        q->bb[i] = q->bb[j];
                        q->bb[j] = NULL;
                    }
                } else if (q->join_type[j] == FS_UNION) {
                    /* It's a UNION, inner join */
#ifdef DEBUG_MERGE
                    printf("block join B%d [X] B%d\n", i, j);
#endif
                    if (q->bb[i]) {
                        fs_binding *nb = fs_binding_join(q, q->bb[i], q->bb[j], FS_INNER);
                        fs_binding_free(q->bb[i]);
                        q->bb[i] = nb;
                        if (i == 0) q->bt = q->bb[i];
                        fs_binding_free(q->bb[j]);
                        q->bb[j] = NULL;
                    } else {
                        /* Bi is empty, just replace it */
                        q->bb[i] = q->bb[j];
                        q->bb[j] = NULL;
                    }
                } else if (q->join_type[j] == FS_LEFT) {
                    /* It's an OPTIONAL, left join */
#ifdef DEBUG_MERGE
                    printf("block join B%d =X] B%d\n", i, j);
#endif
                    fs_binding *nb = fs_binding_join(q, q->bb[i], q->bb[j], FS_LEFT);
                    fs_binding_free(q->bb[i]);
                    q->bb[i] = nb;
                    if (i == 0) q->bt = q->bb[i];
                    fs_binding_free(q->bb[j]);
                    q->bb[j] = NULL;
                } else {
                    fs_error(LOG_ERR, "unknown join type joining B%d and B%d", i, j);
                }
            }
        }
    }

#ifdef DEBUG_MERGE
    explain = flags & FS_QUERY_EXPLAIN;
#endif

    if (explain) {
        double start_time = q->start_time;
	fs_query_free(q);
        q = calloc(1, sizeof(fs_query));
        q->start_time = start_time;
        q->flags = FS_QUERY_EXPLAIN;

	return q;
    }

    if (q->flags & FS_BIND_DISTINCT) {
        int sortable = 0;
	for (int i=0; q->bb[0][i].name; i++) {
	    if (q->bb[0][i].proj || q->bb[0][i].selected) {
		q->bb[0][i].sort = 1;
                sortable = 1;
	    } else {
		q->bb[0][i].sort = 0;
	    }
	}
        if (sortable) {
            fs_binding_sort(q->bb[0]);
            fs_binding_uniq(q->bb[0]);
        }
    }
    /* this is neccesary because the DISTINCT phase may have
     * reduced the length of the projected columns */
    q->length = 0;
    for (int col=1; col < q->num_vars+1; col++) {
        if (!q->bb[0][col].proj) continue;
        if (q->bb[0][col].vals->length > q->length) {
            q->length = q->bb[0][col].vals->length;
        }
    }
#if DEBUG_MERGE > 1
printf("After DISINTCT\n");
fs_binding_print(q->bb[0], stdout);
#endif

    int selected_not_projected = 0;
    for (int col = 1; q->bb[0][col].name; col++) {
        if (q->bb[0][col].selected && !q->bb[0][col].proj) {
            selected_not_projected = 1;
            break;
        }
    }
    /* If there are selected variables that are not projected then we might
     * not have performed a full distinct yet, so we need to run thorugh
     * q->offset disinct rows to make sure the OFFSET is correct */
    if (q->offset > 0 && selected_not_projected) {
        int offsetted = 0;
        while (offsetted < q->offset && q->row < q->length) {
            if (q->row > 0) {
                int dup = 1;
                /* scan right to left cos we'll find a difference quicker that
                 * way */
                for (int col=(q->num_vars); col > 0; col--) {
                    if (q->bb[0][col].vals->data[q->row] != q->bb[0][col].vals->data[(q->row)-1]) {
                        dup = 0;
                        break;
                    }
                }
                if (dup) {
                    (q->row)++;
                    continue;
                }
            }
            offsetted++;
            (q->row)++;
        }
    } else {
        q->row = q->offset;
    }

    if (q->row < 0) q->row = 0;
    q->lastrow = q->row;
    q->rows_output = 0;
    q->pending = calloc(q->segments, sizeof(fs_rid_vector *));
    for (int i=0; i<q->segments; i++) {
	q->pending[i] = fs_rid_vector_new(0);
    }

    if (q->offset < 0) {
	q->offset = 0;
    }

    if (q->num_vars == 0) {
	/* ASK or similar */
        q->length = fs_binding_length(q->bb[0]);

	return q;
    }

    if (q->flags & FS_QUERY_COUNT) {
        fs_binding_free(q->bb[0]);
        q->bb[0] = fs_binding_new();
        q->bt = q->bb[0];
        q->num_vars = 1;
        fs_binding_add(q->bb[0], "count", 0, 1);
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
	fs_binding_free(q->bb[0]);
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
                if (q->blocks[i].data) {
                    for (int j=0; j<q->blocks[i].length; j++) {
                        free(q->blocks[i].data[j]);
                    }
                    free(q->blocks[i].data);
                }
            }
        }

        GSList *it;
        for (it = q->free_list; it; it = it->next) {
            g_free(it->data);
        }
	g_slist_free(q->free_list);

        if (q->default_graphs) fs_rid_vector_free(q->default_graphs);

        memset(q, 0, sizeof(fs_query));
	free(q);
    }
}

static void assign_slot(fs_query *q, rasqal_literal *l, int block)
{
    if (!q->bb[0]) {
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
	fs_binding *vb = fs_binding_get(q->bb[0], vname);
	if (!vb) {
	    vb = fs_binding_add(q->bb[0], vname, FS_RID_NULL, 0);
	}
	if (!vb->need_val && vb->appears != -1) {
	    vb->need_val = 1;
	}
	if (vb->appears == -1) {
	    vb->appears = block;
	}
    }
}

static void check_variables(fs_query *q, rasqal_expression *e, int dont_select)
{
    if (e->literal && e->literal->type == RASQAL_LITERAL_VARIABLE) {
	rasqal_variable *v = e->literal->value.variable;
	fs_binding *b = fs_binding_get(q->bb[0], (char *)v->name);
	if (b) {
            b->need_val = 1;
            if (!dont_select) {
                b->selected = 1;
            }
        }

	return;
    }

    if (e->arg1) {
	check_variables(q, e->arg1, dont_select);
    }
    if (e->arg2) {
	check_variables(q, e->arg2, dont_select);
    }
    if (e->arg3) {
	check_variables(q, e->arg3, dont_select);
    }
    if (e->args) {
        const int len = raptor_sequence_size(e->args);
        for (int i=0; i<len; i++) {
            rasqal_expression *ae = raptor_sequence_get_at(e->args, i);
            check_variables(q, ae, dont_select);
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
        fs_binding *b = fs_binding_get(q->bb[0], name);
        if (!b) {
            fs_error(LOG_ERR, "no binding for %s", name);

            return 0;
        }
        fs_rid_vector_append_vector(b->vals, res);
        b->bound = 1;
        retval = 1;
    }
    fs_rid_vector_free(res);

    return retval;
}

static void graph_pattern_walk(fsp_link *link, rasqal_graph_pattern *pattern,
	fs_query *q, rasqal_literal *model, int parent, int uni)
{
    if (!pattern) {
	return;
    }

    int union_sub = 0;

    int op = rasqal_graph_pattern_get_operator(pattern);

    if (op == RASQAL_GRAPH_PATTERN_OPERATOR_OPTIONAL) {
	(q->block)++;
        q->parent_block[q->block] = parent;
        q->join_type[q->block] = FS_LEFT;
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_UNION) {
        (q->unions)++;
        union_sub = q->unions;
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_BASIC ||
	       op == RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH) {
        if (op == RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH) {
#if RASQAL_VERSION >= 917
            model = rasqal_graph_pattern_get_origin(pattern);
#endif
            if (!model) {
                fs_error(LOG_ERR, "expected origin from pattern, but got NULL");
            }
        }
        (q->block)++;
        q->parent_block[q->block] = parent;
        q->join_type[q->block] = FS_INNER;
#if RASQAL_VERSION >= 917
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_FILTER) {
        rasqal_expression *e =
            rasqal_graph_pattern_get_filter_expression(pattern);
        if (e) {
            /* we need to flag if it's a UNION FILTER so we don't
             * unneccesarily set the selected flag on the variables */
            check_variables(q, e, uni);
            if (filter_optimise(q, e, q->block)) {
                /* stop us from trying to evaluate this expression later */
                e = NULL;
            }
        }
        if (e) {
            if (!q->constraints[parent]) {
                q->constraints[parent] = raptor_new_sequence(NULL, NULL);
            }
#ifdef DEBUG_FILTER
            printf("ADD ");
            rasqal_expression_print(e, stdout);
            printf(" to B%d\n", parent);
#endif
            raptor_sequence_push(q->constraints[parent], e);
        }
#endif
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_GROUP) {
        /* do nothing */
    } else {
	fs_error(LOG_ERR, "Unknown GP operator %d not supported", op);
    }

    if (uni) {
        q->union_group[q->block] = uni;
        q->parent_block[q->block] = parent;
        q->join_type[q->block] = FS_UNION;
    }

    for (int i=0; 1; i++) {
	rasqal_triple *rt = rasqal_graph_pattern_get_triple(pattern, i);
	if (!rt) break;
        rasqal_triple *t = calloc(1, sizeof(rasqal_triple));
#if RASQAL_VERSION >= 917
        t->origin = model;
#else
        t->origin = rt->origin;
#endif
        t->subject = rt->subject;
        t->predicate = rt->predicate;
        t->object = rt->object;
#ifdef DEBUG_MERGE
        printf("ADD ");
        rasqal_triple_print(rt, stdout);
        printf(" to B%d\n", q->block);
#endif
	fs_p_vector_append(q->blocks+q->block, (void *)t);
	assign_slot(q, t->origin, q->block);
	assign_slot(q, t->subject, q->block);
	assign_slot(q, t->predicate, q->block);
	assign_slot(q, t->object, q->block);
    }

    const int this_block = q->block;
    /* descend into next level down */
    for (int index=0; 1; index++) {
        rasqal_graph_pattern *sgp =
                    rasqal_graph_pattern_get_sub_graph_pattern(pattern, index);
        if (!sgp) break;
        graph_pattern_walk(link, sgp, q, model, this_block, union_sub);
    }

#if RASQAL_VERSION < 917
    raptor_sequence *s =
        rasqal_graph_pattern_get_constraint_sequence(pattern);
    if (s) {
        for (int c=0; 1; c++) {
            rasqal_expression *e = raptor_sequence_get_at(s, c);
            if (!e) break;

            /* we need to flag if it's a UNION FILTER so we don't
             * unneccesarily set the selected flag on the variables */
            check_variables(q, e, uni);
            if (filter_optimise(q, e, q->block)) {
                /* stop us from trying to evaluate this expression later */
                raptor_sequence_set_at(s, c, NULL);
            }
        }

        if (q->constraints[this_block]) {
            raptor_sequence_join(q->constraints[this_block], s);
        } else {
            q->constraints[this_block] = s;
        }
    }
#endif
    for (int c=0; 1; c++) {
        rasqal_expression *e = rasqal_query_get_order_condition(q->rq, c);
        if (!e) break;
        check_variables(q, e, 0);
    }
}

#if 0
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
#endif

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
    if (q->default_graphs) {
        if (!t->origin && slot[0]->length == 0) {
            fs_rid_vector_append_vector(slot[0], q->default_graphs);
            *tobind &= ~FS_QUERY_DEFAULT_GRAPH;
        } else {
            /* setting this prevents the backend from binding to values in the default
             * graph */
            *tobind |= FS_QUERY_DEFAULT_GRAPH;
        }
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
                b[i].bound |= oldb[i].bound;
            }
            free(results);
            for (int x=0; x<4; x++) {
                fs_rid_vector_clear(slot[x]);
            }
            fs_binding_free(oldb);

            return 1;
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
		fs_binding_add_vector(b, varnames[col], results[col]);
                ret += results[col] ? results[col]->length : 0;
	    } else {
		fs_error(LOG_ERR, "column %d has no varname in sub results", col);
	    }
	    fs_rid_vector_free(results[col]);
	}
        free(results);

#if 0
        /* this code proves to cost more overall, though you'd expect it save
         * effort, lookout for pathalogical cases where it makes a huge
         * difference, on the "baseball" benchmark it makes things slower */
        /* do some early DISTINCTing, to save us work later */
	if (flags & FS_BIND_DISTINCT) {
            for (int c=0; b[c].name; c++) {
                if (b[c].bound) b[c].sort = 1;
            }
            fs_binding_sort(b);
            fs_binding_uniq(b);
            for (int c=0; b[c].name; c++) {
                b[c].sort = 0;
            }
        }
#endif
            
        fs_binding_merge(q, block, oldb, b);
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

    fs_binding *b = q->bb[block];
    if (!b) {
        fs_error(LOG_ERR, "binding block is NULL");

        return 1;
    }
    int tobind = q->flags;

#ifdef DEBUG_MERGE
    const int explain = 1;
#else
    const int explain = tobind & FS_QUERY_EXPLAIN;
#endif

    char *varnames[4] = { NULL, NULL, NULL, NULL };
    fs_binding_clear_used_all(b);
    fs_binding *oldb = fs_binding_copy_and_clear(b);
    fs_rid_vector **results = NULL;

    /* if theres a patterns with lots of bindings for the subject and one
     * predicate we can bind_many it */
    if (fs_opt_is_const(oldb, t->subject) &&
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

        fs_bind_cache_wrapper(q->qs, q, 0, tobind | FS_BIND_BY_SUBJECT,
                 slot, &results, -1, q->order ? -1 : q->soft_limit);
	if (explain) {
	    char desc[4][DESC_SIZE];
	    desc_action(tobind, slot, desc);
	    printf("mmmms (%s,%s,%s,%s) -> %d\n", desc[0], desc[1], desc[2], desc[3], results ? (results[0] ? results[0]->length : -1) : -2);
	}

        ret = process_results(q, block, oldb, b, tobind, results, varnames, numbindings, slot);
        for (int x=0; x<4; x++) {
            fs_rid_vector_free(slot[x]);
        }

        return ret;
    }

    /* if theres a patterns with lots of bindings for the object and one
     * predicate we can bind_many it */
    if (fs_opt_is_const(oldb, t->object)) {
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
     * need to bind_all. */
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

    fs_binding *b = q->bb[block];
    int tobind = q->flags;

    const int explain = tobind & FS_QUERY_EXPLAIN;

    char *varnames[4] = { NULL, NULL, NULL, NULL };
    fs_binding_clear_used_all(b);
    fs_binding *oldb = fs_binding_copy_and_clear(b);
    fs_rid_vector **results = NULL;

    /* check the patterns are all of the form { ?s <const> <const> . } */
    for (int i=0; i<count; i++) {
        if (fs_opt_is_const(oldb, t[i]->subject)) {
            fs_error(LOG_ERR, "bad const subject argument to fs_handle_query_triple_multi()");
            return 0;
        }
        if (!fs_opt_is_const(oldb, t[i]->object) || !fs_opt_is_const(oldb, t[i]->predicate)) {
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
    int use_model = 0;
    for (int i=1; i<count; i++) {
        int bind;
        char *v;
        if (t[i]->origin) use_model = 1;
        fs_bind_slot(q, block, oldb, t[i]->origin, slot[0], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->subject, slot[1], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->predicate, slot[2], &bind, &v, 0);
        fs_bind_slot(q, block, oldb, t[i]->object, slot[3], &bind, &v, 1);
    }
    if (!use_model && slot[0]->length == 0 && q->default_graphs) {
        fs_rid_vector_append_vector(slot[0], q->default_graphs);
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
#if RASQAL_VERSION >= 917
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
#endif
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
                fs_binding *b0 = fs_binding_get(q->bb[0], *vname);
                (b0->bound_in_block[block])++;
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
#if RASQAL_VERSION >= 917
        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
#endif
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
