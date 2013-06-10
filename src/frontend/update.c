/*
 *  Copyright (C) 2009 Steve Harris
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
 *  $Id: update.c $
 */

#include <string.h>
#include <stdio.h>
#include <glib.h>
#include <pcre.h>
#include <rasqal.h>

#include "debug.h"
#include "update.h"
#include "import.h"
#include "query.h"
#include "../common/4store.h"
#include "../common/4s-internals.h"
#include "../common/error.h"
#include "../common/rdf-constants.h"

struct update_context {
    fsp_link *link;
    int segments;
    fs_query_state *qs;
    fs_query *q;
    rasqal_query *rq;
    GSList *messages;
    GSList *freeable;
    rasqal_literal *graph;
    rasqal_update_operation *op;
    int opid;
    int error;
};

#define QUAD_BUF_SIZE 4096

struct quad_buf {
    int length;
    fs_rid quads[QUAD_BUF_SIZE][4];
};
static struct quad_buf *quad_buffer = NULL;

int fs_load(struct update_context *uc, char *resuri, char *graphuri);

int fs_clear(struct update_context *uc, char *graphuri);

int fs_add(struct update_context *uc, char *from, char *to);

int fs_move(struct update_context *uc, char *from, char *to);

int fs_copy(struct update_context *uc, char *from, char *to);

fs_rid fs_hash_rasqal_literal(struct update_context *uc, rasqal_literal *l, int row);
void fs_resource_from_rasqal_literal(struct update_context *uctxt,
                                     rasqal_literal *l, fs_resource *res, int row);

static void add_message(struct update_context *uc, char *m, int freeable)
{
    uc->messages = g_slist_append(uc->messages, m);
    if (freeable) {
        uc->freeable = g_slist_prepend(uc->freeable, m);
    }
}

static void error_handler(void *user_data, raptor_log_message *message)
{
    struct update_context *uc = user_data;

    char *msg = g_strdup_printf("%s: %s at line %d of operation %d", 
            raptor_log_level_get_label(message->level), message->text, raptor_locator_line(message->locator), uc->opid);
    uc->error = (message->level == RAPTOR_LOG_LEVEL_ERROR);
    add_message(uc, msg, 1);
    fs_error(LOG_ERR, "%s", msg);
}

static char *build_update_error_message(GSList *messages) {
    int num_messages = g_slist_length(messages);
    char **strv = calloc(sizeof(char *), num_messages+1);
    int pos = 0;
    for (GSList *it=messages; it; it=it->next) {
        strv[pos++] = it->data;
    }
    char *message = g_strjoinv("\n", strv);
    free(strv);
    return message;
}

static int any_vars(rasqal_triple *t)
{
    if (t->origin && t->origin->type == RASQAL_LITERAL_VARIABLE) {
        return 1;
    }
    if (t->subject->type == RASQAL_LITERAL_VARIABLE) {
        return 1;
    }
    if (t->predicate->type == RASQAL_LITERAL_VARIABLE) {
        return 1;
    }
    if (t->object->type == RASQAL_LITERAL_VARIABLE) {
        return 1;
    }

    return 0;
}

static int delete_rasqal_triple(struct update_context *uc, fs_rid_vector *vec[], rasqal_triple *triple, int row)
{
    fs_rid m, s, p, o;

    if (triple->origin) {
        m = fs_hash_rasqal_literal(uc, triple->origin, row);
        if (m == FS_RID_NULL) return 1;
    } else if (uc->op->graph_uri) {
        m = fs_hash_uri((char *)raptor_uri_as_string(uc->op->graph_uri));
    } else {
        /* m can be wildcard in the absence of GRAPH, WITH etc. */
        m = FS_RID_NULL;
    }
    s = fs_hash_rasqal_literal(uc, triple->subject, row);
    if (s == FS_RID_NULL) return 1;
    p = fs_hash_rasqal_literal(uc, triple->predicate, row);
    if (p == FS_RID_NULL) return 1;
    o = fs_hash_rasqal_literal(uc, triple->object, row);
    if (o == FS_RID_NULL) return 1;

    /* as long as s, p, and o are bound, we can add this quad */
    fs_rid_vector_append(vec[0], m);
    fs_rid_vector_append(vec[1], s);
    fs_rid_vector_append(vec[2], p);
    fs_rid_vector_append(vec[3], o);

    if (fs_rid_vector_contains(vec[0], fs_c.system_config))
        fsp_reload_acl_system(uc->link);

    if (fs_rid_vector_length(vec[0]) > 999) {
        fsp_delete_quads_all(uc->link, vec);
        for (int s=0; s<4; s++) {
            fs_rid_vector_truncate(vec[s], 0);
        }
    }

    return 0;
}

static int insert_rasqal_triple(struct update_context *uc, rasqal_triple *triple, int row)
{
    fs_rid quad_buf[1][4];
    fs_resource res;
    if (triple->origin) {
        fs_resource_from_rasqal_literal(uc, triple->origin, &res, row);
        quad_buf[0][0] = fs_hash_rasqal_literal(uc, triple->origin, row);
    } else if (uc->op->graph_uri) {
        res.lex = (char *)raptor_uri_as_string(uc->op->graph_uri);
        res.attr = FS_RID_NULL;
        quad_buf[0][0] =
            fs_hash_uri((char *)raptor_uri_as_string(uc->op->graph_uri));
    } else {
        quad_buf[0][0] = fs_c.default_graph;
        res.lex = FS_DEFAULT_GRAPH;
        res.attr = FS_RID_NULL;
    }

    if (quad_buf[0][0] == fs_c.system_config)
        fsp_reload_acl_system(uc->link);

    if (!FS_IS_URI(quad_buf[0][0])) {
        return 1;
    }
    quad_buf[0][1] = fs_hash_rasqal_literal(uc, triple->subject, row);
    if (FS_IS_LITERAL(quad_buf[0][1])) {
        return 1;
    }
    quad_buf[0][2] = fs_hash_rasqal_literal(uc, triple->predicate, row);
    if (!FS_IS_URI(quad_buf[0][2])) {
        return 1;
    }
    quad_buf[0][3] = fs_hash_rasqal_literal(uc, triple->object, row);
    res.rid = quad_buf[0][0];
    if (res.lex) fsp_res_import(uc->link, FS_RID_SEGMENT(quad_buf[0][0], uc->segments), 1, &res);
    res.rid = quad_buf[0][1];
    fs_resource_from_rasqal_literal(uc, triple->subject, &res, 0);
    if (res.lex) fsp_res_import(uc->link, FS_RID_SEGMENT(quad_buf[0][1], uc->segments), 1, &res);
    res.rid = quad_buf[0][2];
    fs_resource_from_rasqal_literal(uc, triple->predicate, &res, 0);
    if (res.lex) fsp_res_import(uc->link, FS_RID_SEGMENT(quad_buf[0][2], uc->segments), 1, &res);
    res.rid = quad_buf[0][3];
    fs_resource_from_rasqal_literal(uc, triple->object, &res, 0);
    if (res.lex) fsp_res_import(uc->link, FS_RID_SEGMENT(quad_buf[0][3], uc->segments), 1, &res);
    fsp_quad_import(uc->link, FS_RID_SEGMENT(quad_buf[0][1], uc->segments), FS_BIND_BY_SUBJECT, 1, quad_buf);
//printf("I %016llx %016llx %016llx %016llx\n", quad_buf[0][0], quad_buf[0][1], quad_buf[0][2], quad_buf[0][3]);

    return 0;
}

static char *graph_arg(raptor_uri *u)
{
    if (!u) {
        return NULL;
    }

    return (char *)raptor_uri_as_string(u);
}

static int update_op(struct update_context *uc)
{
    fs_rid_vector *vec[4];
    switch (uc->op->type) {
    case RASQAL_UPDATE_TYPE_UNKNOWN:
        add_message(uc, "Unknown update operation", 0);
        return 1;
    case RASQAL_UPDATE_TYPE_CLEAR:
        fs_clear(uc, graph_arg(uc->op->graph_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_CREATE:
        return 0;
    case RASQAL_UPDATE_TYPE_DROP:
        fs_clear(uc, graph_arg(uc->op->graph_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_LOAD:
        fs_load(uc, graph_arg(uc->op->document_uri),
                    graph_arg(uc->op->graph_uri));
        return 0;
#if RASQAL_VERSION >= 924
    case RASQAL_UPDATE_TYPE_ADD:
        fs_add(uc, graph_arg(uc->op->graph_uri),
                   graph_arg(uc->op->document_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_MOVE:
        fs_move(uc, graph_arg(uc->op->graph_uri),
                    graph_arg(uc->op->document_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_COPY:
        fs_copy(uc, graph_arg(uc->op->graph_uri),
                    graph_arg(uc->op->document_uri));
        return 0;
#endif
    case RASQAL_UPDATE_TYPE_UPDATE:
        break;
    }

    fs_hash_freshen();

    raptor_sequence *todel = NULL;
    raptor_sequence *toins = NULL;

    if (uc->op->delete_templates && !uc->op->where) {
        int where = 0;

        /* check to see if it's a DELETE WHERE { } */
        for (int t=0; t<raptor_sequence_size(uc->op->delete_templates); t++) {
            rasqal_triple *tr = raptor_sequence_get_at(uc->op->delete_templates, t);
            if (any_vars(tr)) {
                where = 1;
                break;
            }
        }
        if (where) {
            fs_error(LOG_ERR, "DELETE WHERE { x } not yet supported");
            add_message(uc, "DELETE WHERE { x } not yet supported, use DELETE { x } WHERE { x }", 0);

            return 1;
        }
    }

#if RASQAL_VERSION >= 923
    if (uc->op->where) {
        todel = raptor_new_sequence(NULL, NULL);
        toins = raptor_new_sequence(NULL, NULL);
        raptor_sequence *todel_p = raptor_new_sequence(NULL, NULL);
        raptor_sequence *toins_p = raptor_new_sequence(NULL, NULL);
        raptor_sequence *vars = raptor_new_sequence(NULL, NULL);

        fs_query *q = calloc(1, sizeof(fs_query));
        uc->q = q;
        q->qs = uc->qs;
        q->rq = uc->rq;
        q->flags = FS_BIND_DISTINCT;
#ifdef DEBUG_MERGE
        q->flags |= FS_QUERY_CONSOLE_OUTPUT;
#endif
        q->boolean = 1;
        q->opt_level = 3;
        q->soft_limit = -1;
        q->segments = fsp_link_segments(uc->link);
        q->link = uc->link;
        q->bb[0] = fs_binding_new();
        q->bt = q->bb[0];

        /* hashtable to hold runtime created resources */
        q->tmp_resources = g_hash_table_new_full(fs_rid_hash, fs_rid_equal, g_free, fs_free_cached_resource);

        /* add column to denote join ordering */
        fs_binding_create(q->bb[0], "_ord", FS_RID_NULL, 0);

        if (uc->op->delete_templates) {
            for (int t=0; t<raptor_sequence_size(uc->op->delete_templates); t++) {
                rasqal_triple *tr = raptor_sequence_get_at(uc->op->delete_templates, t);
                if (any_vars(tr)) {
                    fs_check_cons_slot(q, vars, tr->subject);
                    fs_check_cons_slot(q, vars, tr->predicate);
                    fs_check_cons_slot(q, vars, tr->object);
                    raptor_sequence_push(todel_p, tr);
                } else {
                    raptor_sequence_push(todel, tr);
                }
            }
        }

        if (uc->op->insert_templates) {
            for (int t=0; t<raptor_sequence_size(uc->op->insert_templates); t++) {
                rasqal_triple *tr = raptor_sequence_get_at(uc->op->insert_templates, t);
                if (any_vars(tr)) {
                    fs_check_cons_slot(q, vars, tr->subject);
                    fs_check_cons_slot(q, vars, tr->predicate);
                    fs_check_cons_slot(q, vars, tr->object);
                    raptor_sequence_push(toins_p, tr);
                } else {
                    raptor_sequence_push(toins, tr);
                }
            }
        }

        q->num_vars = raptor_sequence_size(vars);

        for (int i=0; i < q->num_vars; i++) {
            rasqal_variable *v = raptor_sequence_get_at(vars, i);
            fs_binding_add(q->bb[0], v, FS_RID_NULL, 1);
        }

        /* perform the WHERE match */
        fs_query_process_pattern(q, uc->op->where, vars);

        q->length = fs_binding_length(q->bb[0]);

        for (int s=0; s<4; s++) {
            vec[s] = fs_rid_vector_new(0);
        }
        for (int t=0; t<raptor_sequence_size(todel_p); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(todel_p, t);
            for (int row=0; row < q->length; row++) {
                delete_rasqal_triple(uc, vec, triple, row);
            }
            if (fs_rid_vector_length(vec[0]) > 1000) {
                fsp_delete_quads_all(uc->link, vec);
            }
        }
        if (fs_rid_vector_length(vec[0]) > 0) {
            fsp_delete_quads_all(uc->link, vec);
        }
        for (int s=0; s<4; s++) {
//fs_rid_vector_print(vec[s], 0, stdout);
            fs_rid_vector_free(vec[s]);
            vec[s] = NULL;
        }

        for (int t=0; t<raptor_sequence_size(toins_p); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(toins_p, t);
            for (int row=0; row < q->length; row++) {
                insert_rasqal_triple(uc, triple, row);
            }
        }

        /* must not free the rasqal_query */
        q->rq = NULL;
        fs_query_free(q);
        uc->q = NULL;
    } else {
        todel = uc->op->delete_templates;
        toins = uc->op->insert_templates;
    }
#else
    if (uc->op->where) {
        fs_error(LOG_ERR, "DELETE/INSERT WHERE requires Rasqal 0.9.23 or newer");
        add_message(uc, "DELETE/INSERT WHERE requires Rasqal 0.9.23 or newer", 0);
    }
#endif

    /* delete constant triples */
    if (todel) {
        for (int s=0; s<4; s++) {
            vec[s] = fs_rid_vector_new(0);
        }
        for (int t=0; t<raptor_sequence_size(todel); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(todel, t);
            if (any_vars(triple)) {
                continue;
            }
            delete_rasqal_triple(uc, vec, triple, 0);
        }
        if (fs_rid_vector_length(vec[0]) > 0) {
            fsp_delete_quads_all(uc->link, vec);
        }
        for (int s=0; s<4; s++) {
            fs_rid_vector_free(vec[s]);
            vec[s] = NULL;
        }
    }

    /* insert constant triples */
    if (toins) {
        for (int t=0; t<raptor_sequence_size(toins); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(toins, t);
            if (any_vars(triple)) {
                continue;
            }
            insert_rasqal_triple(uc, triple, 0);
        }
    }
    fs_hash_freshen();

    return 0;
}

int fs_update(fs_query_state *qs, char *update, char **message, int unsafe)
{
    rasqal_query *rq = rasqal_new_query(qs->rasqal_world, "sparql11-update", NULL);
    if (!rq) {
        *message = g_strdup_printf("Unable to initialise update parser");

        return 1;
    }
    struct update_context uctxt;
    rasqal_world_set_log_handler(qs->rasqal_world, &uctxt, error_handler);

    memset(&uctxt, 0, sizeof(uctxt));
    uctxt.link = qs->link;
    uctxt.segments = fsp_link_segments(qs->link);
    uctxt.qs = qs;
    uctxt.rq = rq;
    raptor_uri *bu = raptor_new_uri(qs->raptor_world, (unsigned char *)"local:local");
    rasqal_query_prepare(rq, (unsigned char *)update, bu);
    if (uctxt.error) {
        if (uctxt.messages) {
            *message = build_update_error_message(uctxt.messages);
            g_slist_free(uctxt.messages);
        }
        return 1;
    }
    if (!quad_buffer) {
        quad_buffer = calloc(uctxt.segments, sizeof(struct quad_buf));
    }

    int ok = 1;
    for (int i=0; 1; i++) {
        rasqal_update_operation *op = rasqal_query_get_update_operation(rq, i);
        if (!op) {
            break;
        }
        uctxt.op = op;
        uctxt.opid = i;
        if (update_op(&uctxt)) {
            ok = 0;
            break;
        }
    }
    fsp_res_import_commit_all(qs->link);
    fsp_quad_import_commit_all(qs->link, FS_BIND_BY_SUBJECT);
    fsp_stop_import_all(qs->link);

    rasqal_free_query(rq);

    if (uctxt.messages) {
        *message = build_update_error_message(uctxt.messages);
        g_slist_free(uctxt.messages);
    }
    for (GSList *it=uctxt.freeable; it; it=it->next) {
        g_free(it->data);
    }
    g_slist_free(uctxt.freeable);

    raptor_free_uri(bu);

    if (ok) {
        return 0;
    } else {
        return 1;
    }
}

fs_rid fs_hash_rasqal_literal(struct update_context *uc, rasqal_literal *l, int row)
{
    if (!l) return FS_RID_NULL;

    if (l->type == RASQAL_LITERAL_VARIABLE) {
        if (uc->q) {
            return fs_binding_get_val(uc->q->bb[0], l->value.variable, row, NULL);
        }
        fs_error(LOG_ERR, "no variables bound");

        return FS_RID_NULL;
    }

    rasqal_literal_type type = rasqal_literal_get_rdf_term_type(l);
    switch (type) {
    case RASQAL_LITERAL_URI:
        return fs_hash_uri((char *)raptor_uri_as_string(l->value.uri));
    
    case RASQAL_LITERAL_UNKNOWN:
    case RASQAL_LITERAL_STRING:
    case RASQAL_LITERAL_XSD_STRING: {
        fs_rid attr = 0;
        if (l->datatype) {
            attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
        } else if (l->language) {
            /* lang tags are normalised to upper case internally */
            char *lang = g_ascii_strup((char *)l->language, -1);
            attr = fs_hash_literal(lang, 0);
            g_free(lang);
        }

        return fs_hash_literal((char *)rasqal_literal_as_string(l), attr);
    }

    case RASQAL_LITERAL_BLANK: {
        raptor_term_blank_value bnode;
        bnode.string = (unsigned char *)rasqal_literal_as_string(l);
        bnode.string_len = strlen((char *)bnode.string);

        return fs_bnode_id(uc->link, bnode);
    }

    case RASQAL_LITERAL_VARIABLE:
    case RASQAL_LITERAL_QNAME:
    case RASQAL_LITERAL_PATTERN:
    case RASQAL_LITERAL_BOOLEAN:
    case RASQAL_LITERAL_INTEGER:
    case RASQAL_LITERAL_INTEGER_SUBTYPE:
    case RASQAL_LITERAL_DECIMAL:
    case RASQAL_LITERAL_FLOAT:
    case RASQAL_LITERAL_DOUBLE:
    case RASQAL_LITERAL_DATETIME:
    case RASQAL_LITERAL_UDT:
#if RASQAL_VERSION >= 929
    case RASQAL_LITERAL_DATE:
#endif
        break;
    }
    fs_error(LOG_ERR, "bad rasqal literal (type %d)", type);

    return FS_RID_NULL;
}

void fs_resource_from_rasqal_literal(struct update_context *uctxt, rasqal_literal *l, fs_resource *res, int row)
{
    if (!l) {
        res->lex = "(null)";
        res->attr = FS_RID_NULL;

        return;
    }
    rasqal_literal_type type = l->type;
    if (type == RASQAL_LITERAL_VARIABLE) {
        /* right now you can't introduce new literals in INSERT, so it doesn't
         * matter */
        res->lex = NULL;
        res->attr = FS_RID_GONE;
    } else if (type == RASQAL_LITERAL_URI) {
	res->lex = (char *)raptor_uri_as_string(l->value.uri);
        res->attr = FS_RID_NULL;
    } else {
        res->lex = (char *)l->string;
        res->attr = 0;
        fs_resource ares;
        ares.lex = NULL;
        if (l->datatype) {
            res->attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
            ares.rid = res->attr;
            ares.lex = (char *)raptor_uri_as_string(l->datatype);
            ares.attr = FS_RID_NULL;
        } else if (l->language) {
            res->attr = fs_hash_literal(l->language, 0);
            ares.rid = res->attr;
            ares.lex = (char *)l->language;
            ares.attr = 0;
        }
        /* insert attribute resource if there is one */
        if (ares.lex) {
            fsp_res_import(uctxt->link, FS_RID_SEGMENT(ares.rid, uctxt->segments), 1, &ares);
        }
    }
}

/* Does LOAD <resuri> [INTO <graphuri>] */

int fs_load(struct update_context *uc, char *resuri, char *graphuri)
{
    FILE *errout = NULL;
    errout = tmpfile();
    int count = 0;
    int errors = 0;
    if (fsp_start_import_all(uc->link)) {
        errors++;
        add_message(uc, "aborting import", 0);
        fclose(errout);

        return errors;
    }

    char *model = graphuri ? graphuri : resuri;
    fs_import(uc->link, model, resuri, "auto", 0, 0, 0, errout, &count);
    fs_import_commit(uc->link, 0, 0, 0, errout, &count);
    fsp_stop_import_all(uc->link);
    rewind(errout);
    char tmp[1024];
    if (fgets(tmp, 1024, errout)) {
        errors++;
        add_message(uc, g_strdup(tmp), 1);
    } else {
        if (graphuri) {
            add_message(uc, g_strdup_printf("Imported <%s> into <%s>",
                resuri, graphuri), 1);
        } else {
            add_message(uc, g_strdup_printf("Imported <%s>", resuri), 1);
        }
    }
    fclose(errout);

    return errors;
}

/* Does CLEAR <graphuri> */

int fs_clear(struct update_context *uc, char *graphuri)
{
    fs_rid_vector *mvec = fs_rid_vector_new(0);
    fs_rid mrid;
    if (graphuri) {
        mrid = fs_hash_uri(graphuri);
    } else {
        graphuri = FS_DEFAULT_GRAPH;
        mrid = fs_c.default_graph;
    }
    fs_rid_vector_append(mvec, mrid);

    int errors = 0;
    if (fsp_delete_model_all(uc->link, mvec)) {
        errors++;
        add_message(uc, g_strdup_printf("Error while trying to delete %s", graphuri), 1);
    } else {
        add_message(uc, g_strdup_printf("Deleted <%s>", graphuri), 1);
    }
    fs_rid_vector_free(mvec);

    return errors;
}

static int flush_triples(struct update_context *uc)
{
    for (int s=0; s<uc->segments; s++) {
        if (quad_buffer[s].length > 0) {
            fsp_quad_import(uc->link, s, FS_BIND_BY_SUBJECT, quad_buffer[s].length, quad_buffer[s].quads);
            quad_buffer[s].length = 0;
        }
    }

    return 0;
}

/* insert the triples in s, p, o into model */
static int insert_triples(struct update_context *uc, fs_rid model, fs_rid_vector *s, fs_rid_vector *p, fs_rid_vector *o)
{
    for (int i=0; i<s->length; i++) {
        int segment = FS_RID_SEGMENT(s->data[i], uc->segments);
        int pos = quad_buffer[segment].length;
        quad_buffer[segment].quads[pos][0] = model;
        quad_buffer[segment].quads[pos][1] = s->data[i];
        quad_buffer[segment].quads[pos][2] = p->data[i];
        quad_buffer[segment].quads[pos][3] = o->data[i];
        quad_buffer[segment].length++;
        if (quad_buffer[segment].length == QUAD_BUF_SIZE) {
            fsp_quad_import(uc->link, segment, FS_BIND_BY_SUBJECT, quad_buffer[segment].length, quad_buffer[segment].quads);
            quad_buffer[segment].length = 0;
        }
    }
    flush_triples(uc);
    //if (model == fs
    printf("%p ",uc->link->kb_name); 
    return 0;
}

/* whem moving bnodes between graphs, this assigns them new RIDs */

static void map_bnodes(struct update_context *uc, fs_rid_vector *r)
{
    for (int i=0; i<r->length; i++) {
        if (FS_IS_BNODE(r->data[i]) && r->data[i] != FS_RID_NULL) {
            char tmp[32];
            sprintf(tmp, "f_%016llx", r->data[i]);
            raptor_term_blank_value bnode;
            bnode.string = (unsigned char *)tmp;
            bnode.string_len = 0;

            r->data[i] = fs_bnode_id(uc->link, bnode);
        }
    }
}

/* Does ADD <from> TO <to> */

int fs_add(struct update_context *uc, char *from, char *to)
{
    fs_rid_vector *mvec = fs_rid_vector_new(0);
    fs_rid_vector *empty = fs_rid_vector_new(0);

    fs_rid fromrid, torid;
    if (from) {
        fromrid = fs_hash_uri(from);
    } else {
        from = FS_DEFAULT_GRAPH;
        fromrid = fs_c.default_graph;
    }
    if (to) {
        torid = fs_hash_uri(to);
    } else {
        to = FS_DEFAULT_GRAPH;
        torid = fs_c.default_graph;
    }

    if (fromrid == torid) {
        /*don't need to do anything */
        add_message(uc, g_strdup_printf("Added <%s> to <%s>", from, to), 1);
        add_message(uc, "0 triples added, 0 removed", 0);

        return 0;
    }

    fs_rid_vector_append(mvec, fromrid);

    int errors = 0;

    /* search for all the triples in from */
    fs_rid_vector **results;
    fs_rid_vector *slot[4] = { mvec, empty, empty, empty };
    fs_bind_cache_wrapper(uc->qs, NULL, 1, FS_BIND_BY_SUBJECT | FS_BIND_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT,
             slot, &results, -1, -1);
    fs_rid_vector_free(mvec);
    fs_rid_vector_free(empty);

    if (!results || results[0]->length == 0) {
        /* there's nothing to add */
        if (results) {
            for (int i=0; i<3; i++) {
                fs_rid_vector_free(results[i]);
            }
            free(results);
        }
        add_message(uc, g_strdup_printf("Added <%s> to <%s>", from, to), 1);
        add_message(uc, "0 triples added, 0 removed", 0);

        return 0;
    }

    map_bnodes(uc, results[0]);
    map_bnodes(uc, results[1]);
    map_bnodes(uc, results[2]);

    fs_resource tores;
    tores.lex = to;
    tores.attr= FS_RID_NULL;
    tores.rid = torid;
    fsp_res_import(uc->link, FS_RID_SEGMENT(torid, uc->segments), 1, &tores);
    
    insert_triples(uc, torid, results[0], results[1], results[2]);

    add_message(uc, g_strdup_printf("Added <%s> to <%s>", from, to), 1);
    add_message(uc, g_strdup_printf("%d triples added, 0 removed", results[0]->length), 1);

    for (int i=0; i<3; i++) {
        fs_rid_vector_free(results[i]);
    }
    free(results);

    return errors;
}

/* Does MOVE <from> TO <to> */

int fs_move(struct update_context *uc, char *from, char *to)
{
    fs_rid_vector *mvec = fs_rid_vector_new(0);
    fs_rid_vector *empty = fs_rid_vector_new(0);

    fs_rid fromrid, torid;
    if (from) {
        fromrid = fs_hash_uri(from);
    } else {
        from = FS_DEFAULT_GRAPH;
        fromrid = fs_c.default_graph;
    }
    if (to) {
        torid = fs_hash_uri(to);
    } else {
        to = FS_DEFAULT_GRAPH;
        torid = fs_c.default_graph;
    }

    if (fromrid == torid) {
        /*don't need to do anything */
        fs_rid_vector_free(mvec);
        fs_rid_vector_free(empty);
        add_message(uc, g_strdup_printf("Moved <%s> to <%s>", from, to), 1);
        add_message(uc, "0 triples added, 0 removed", 0);

        return 0;
    }

    fs_rid_vector_append(mvec, fromrid);

    /* search for all the triples in from */
    fs_rid_vector **results;
    fs_rid_vector *slot[4] = { mvec, empty, empty, empty };

    /* see if there's any data in <from> */
    fs_bind_cache_wrapper(uc->qs, NULL, 1, FS_BIND_BY_SUBJECT | FS_BIND_SUBJECT,
             slot, &results, -1, 1);
    if (!results || results[0]->length == 0) {
        if (results) {
            fs_rid_vector_free(results[0]);
            free(results);
        }
        fs_rid_vector_free(mvec);
        fs_rid_vector_free(empty);
        add_message(uc, g_strdup_printf("<%s> is empty, not moving", from), 1);

        return 1;
    }

    fs_rid_vector_free(results[0]);
    free(results);

    /* get the contents of <from> */
    fs_bind_cache_wrapper(uc->qs, NULL, 1, FS_BIND_BY_SUBJECT | FS_BIND_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT,
             slot, &results, -1, -1);

    /* delete <to> */
    mvec->data[0] = torid;
    if (fsp_delete_model_all(uc->link, mvec)) {
        add_message(uc, g_strdup_printf("Error while trying to delete %s", to), 1);

        return 1;
    }

    /* delete <from> */
    mvec->data[0] = fromrid;
    if (fsp_delete_model_all(uc->link, mvec)) {
        add_message(uc, g_strdup_printf("Error while trying to delete %s", from), 1);

        return 1;
    }

    /* insert <to> */
    fs_resource tores;
    tores.lex = to;
    tores.attr= FS_RID_NULL;
    tores.rid = torid;
    fsp_res_import(uc->link, FS_RID_SEGMENT(torid, uc->segments), 1, &tores);
    
    insert_triples(uc, torid, results[0], results[1], results[2]);

    fs_rid_vector_free(mvec);
    fs_rid_vector_free(empty);

    add_message(uc, g_strdup_printf("Moved <%s> to <%s>", from, to), 1);
    add_message(uc, g_strdup_printf("%d triples added, ?? removed", results[0]->length), 1);

    for (int i=0; i<3; i++) {
        fs_rid_vector_free(results[i]);
    }
    free(results);

    return 0;
}

/* Does COPY <from> TO <to> */

int fs_copy(struct update_context *uc, char *from, char *to)
{
    fs_rid_vector *mvec = fs_rid_vector_new(0);
    fs_rid_vector *empty = fs_rid_vector_new(0);

    fs_rid fromrid, torid;
    if (from) {
        fromrid = fs_hash_uri(from);
    } else {
        from = FS_DEFAULT_GRAPH;
        fromrid = fs_c.default_graph;
    }
    if (to) {
        torid = fs_hash_uri(to);
    } else {
        to = FS_DEFAULT_GRAPH;
        torid = fs_c.default_graph;
    }

    if (fromrid == torid) {
        /*don't need to do anything */
        fs_rid_vector_free(mvec);
        fs_rid_vector_free(empty);
        add_message(uc, g_strdup_printf("Copied <%s> to <%s>", from, to), 1);
        add_message(uc, "0 triples added, 0 removed", 0);

        return 0;
    }

    fs_rid_vector_append(mvec, fromrid);

    /* search for all the triples in from */
    fs_rid_vector **results;
    fs_rid_vector *slot[4] = { mvec, empty, empty, empty };

    /* see if there's any data in <from> */
    fs_bind_cache_wrapper(uc->qs, NULL, 1, FS_BIND_BY_SUBJECT | FS_BIND_SUBJECT,
             slot, &results, -1, 1);
    if (!results || results[0]->length == 0) {
        if (results) {
            fs_rid_vector_free(results[0]);
            free(results);
        }
        fs_rid_vector_free(mvec);
        fs_rid_vector_free(empty);
        add_message(uc, g_strdup_printf("<%s> is empty, not copying", from), 1);

        return 1;
    }

    fs_rid_vector_free(results[0]);
    free(results);

    /* get the contents of <from> */
    fs_bind_cache_wrapper(uc->qs, NULL, 1, FS_BIND_BY_SUBJECT | FS_BIND_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT,
             slot, &results, -1, -1);

    /* map old bnodes to new ones */
    map_bnodes(uc, results[0]);
    map_bnodes(uc, results[1]);
    map_bnodes(uc, results[2]);

    /* delete <to> */
    mvec->data[0] = torid;
    if (fsp_delete_model_all(uc->link, mvec)) {
        fs_rid_vector_free(mvec);
        fs_rid_vector_free(empty);
        add_message(uc, g_strdup_printf("Error while trying to delete %s", to), 1);

        return 1;
    }

    fs_rid_vector_free(mvec);
    fs_rid_vector_free(empty);

    /* insert <to> */
    fs_resource tores;
    tores.lex = to;
    tores.attr= FS_RID_NULL;
    tores.rid = torid;
    fsp_res_import(uc->link, FS_RID_SEGMENT(torid, uc->segments), 1, &tores);
    
    insert_triples(uc, torid, results[0], results[1], results[2]);

    add_message(uc, g_strdup_printf("Copied <%s> to <%s>", from, to), 1);
    add_message(uc, g_strdup_printf("%d triples added, ?? removed", results[0]->length), 1);

    for (int i=0; i<3; i++) {
        fs_rid_vector_free(results[i]);
    }
    free(results);

    return 0;
}
/* vi:set expandtab sts=4 sw=4: */
