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

#include "update.h"
#include "import.h"
#include "query.h"
#include "../common/4store.h"
#include "../common/hash.h"
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
};

int fs_load(struct update_context *uc, char *resuri, char *graphuri);

int fs_clear(struct update_context *uc, char *graphuri);

fs_rid fs_hash_rasqal_literal(struct update_context *uc, rasqal_literal *l, int row);
void fs_resource_from_rasqal_literal(struct update_context *uctxt,
                                     rasqal_literal *l, fs_resource *res, int row);

static void add_message(struct update_context *ct, char *m, int freeable)
{
    ct->messages = g_slist_append(ct->messages, m);
    if (freeable) {
        ct->freeable = g_slist_append(ct->freeable, m);
    }
}

static void error_handler(void *user_data, raptor_log_message *message)
{
    struct update_context *ct = user_data;

    char *msg = g_strdup_printf("%s: %s at line %d of operation %d", raptor_log_level_get_label(message->level), message->text, raptor_locator_line(message->locator), ct->opid);
    add_message(ct, msg, 1);
    fs_error(LOG_ERR, "%s", msg);
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

static int delete_rasqal_triple(struct update_context *ct, fs_rid_vector *vec[], rasqal_triple *triple, int row)
{
    fs_rid m, s, p, o;

    if (triple->origin) {
        m = fs_hash_rasqal_literal(ct, triple->origin, row);
        if (m == FS_RID_NULL) return 1;
    } else if (ct->op->graph_uri) {
        m = fs_hash_uri((char *)raptor_uri_as_string(ct->op->graph_uri));
    } else {
        /* m can be wildcard in the absence of GRAPH, WITH etc. */
        m = FS_RID_NULL;
    }
    s = fs_hash_rasqal_literal(ct, triple->subject, row);
    if (s == FS_RID_NULL) return 1;
    p = fs_hash_rasqal_literal(ct, triple->predicate, row);
    if (p == FS_RID_NULL) return 1;
    o = fs_hash_rasqal_literal(ct, triple->object, row);
    if (o == FS_RID_NULL) return 1;

    /* as long as s, p, and o are bound, we can add this quad */
    fs_rid_vector_append(vec[0], m);
    fs_rid_vector_append(vec[1], s);
    fs_rid_vector_append(vec[2], p);
    fs_rid_vector_append(vec[3], o);

    if (fs_rid_vector_length(vec[0]) > 999) {
        fsp_delete_quads_all(ct->link, vec);
        for (int s=0; s<4; s++) {
            fs_rid_vector_truncate(vec[s], 0);
        }
    }

    return 0;
}

static int insert_rasqal_triple(struct update_context *ct, rasqal_triple *triple, int row)
{
    fs_rid quad_buf[1][4];
    fs_resource res;
    if (triple->origin) {
        fs_resource_from_rasqal_literal(ct, triple->origin, &res, row);
        quad_buf[0][0] = fs_hash_rasqal_literal(ct, triple->origin, row);
    } else if (ct->op->graph_uri) {
        res.lex = (char *)raptor_uri_as_string(ct->op->graph_uri);
        res.attr = FS_RID_NULL;
        quad_buf[0][0] =
            fs_hash_uri((char *)raptor_uri_as_string(ct->op->graph_uri));
    } else {
        quad_buf[0][0] = fs_c.default_graph;
        res.lex = FS_DEFAULT_GRAPH;
        res.attr = FS_RID_NULL;
    }
    if (!FS_IS_URI(quad_buf[0][0])) {
        return 1;
    }
    quad_buf[0][1] = fs_hash_rasqal_literal(ct, triple->subject, row);
    if (FS_IS_LITERAL(quad_buf[0][1])) {
        return 1;
    }
    quad_buf[0][2] = fs_hash_rasqal_literal(ct, triple->predicate, row);
    if (!FS_IS_URI(quad_buf[0][2])) {
        return 1;
    }
    quad_buf[0][3] = fs_hash_rasqal_literal(ct, triple->object, row);
    res.rid = quad_buf[0][0];
    if (res.lex) fsp_res_import(ct->link, FS_RID_SEGMENT(quad_buf[0][0], ct->segments), 1, &res);
    res.rid = quad_buf[0][1];
    fs_resource_from_rasqal_literal(ct, triple->subject, &res, 0);
    if (res.lex) fsp_res_import(ct->link, FS_RID_SEGMENT(quad_buf[0][1], ct->segments), 1, &res);
    res.rid = quad_buf[0][2];
    fs_resource_from_rasqal_literal(ct, triple->predicate, &res, 0);
    if (res.lex) fsp_res_import(ct->link, FS_RID_SEGMENT(quad_buf[0][2], ct->segments), 1, &res);
    res.rid = quad_buf[0][3];
    fs_resource_from_rasqal_literal(ct, triple->object, &res, 0);
    if (res.lex) fsp_res_import(ct->link, FS_RID_SEGMENT(quad_buf[0][3], ct->segments), 1, &res);
    fsp_quad_import(ct->link, FS_RID_SEGMENT(quad_buf[0][1], ct->segments), FS_BIND_BY_SUBJECT, 1, quad_buf);
//printf("I %016llx %016llx %016llx %016llx\n", quad_buf[0][0], quad_buf[0][1], quad_buf[0][2], quad_buf[0][3]);

    return 0;
}

static int update_op(struct update_context *ct)
{
    fs_rid_vector *vec[4];

    switch (ct->op->type) {
    case RASQAL_UPDATE_TYPE_UNKNOWN:
        add_message(ct, "Unknown update operation", 0);
        return 1;
    case RASQAL_UPDATE_TYPE_CLEAR:
        fs_clear(ct, (char *)raptor_uri_as_string(ct->op->graph_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_CREATE:
        return 0;
    case RASQAL_UPDATE_TYPE_DROP:
        fs_clear(ct, (char *)raptor_uri_as_string(ct->op->graph_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_LOAD:
        fs_load(ct, (char *)raptor_uri_as_string(ct->op->document_uri),
                    (char *)raptor_uri_as_string(ct->op->graph_uri));
        return 0;
    case RASQAL_UPDATE_TYPE_UPDATE:
        break;
#if RASQAL_VERSION >= 924
    //case RASQAL_UPDATE_TYPE_ADD:
    //case RASQAL_UPDATE_TYPE_MOVE:
    //case RASQAL_UPDATE_TYPE_COPY:
#endif
    }

    fs_hash_freshen();

    raptor_sequence *todel = NULL;
    raptor_sequence *toins = NULL;

#if RASQAL_VERSION >= 923
    if (ct->op->where) {
        todel = raptor_new_sequence(NULL, NULL);
        toins = raptor_new_sequence(NULL, NULL);
        raptor_sequence *todel_p = raptor_new_sequence(NULL, NULL);
        raptor_sequence *toins_p = raptor_new_sequence(NULL, NULL);
        raptor_sequence *vars = raptor_new_sequence(NULL, NULL);

        fs_query *q = calloc(1, sizeof(fs_query));
        ct->q = q;
        q->qs = ct->qs;
        q->rq = ct->rq;
        q->flags = FS_BIND_DISTINCT;
        q->opt_level = 3;
        q->soft_limit = -1;
        q->segments = fsp_link_segments(ct->link);
        q->link = ct->link;
        q->bb[0] = fs_binding_new();
        q->bt = q->bb[0];
        /* add column to denote join ordering */
        fs_binding_create(q->bb[0], "_ord", FS_RID_NULL, 0);

        if (ct->op->delete_templates) {
            for (int t=0; t<raptor_sequence_size(ct->op->delete_templates); t++) {
                rasqal_triple *tr = raptor_sequence_get_at(ct->op->delete_templates, t);
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

        if (ct->op->insert_templates) {
            for (int t=0; t<raptor_sequence_size(ct->op->insert_templates); t++) {
                rasqal_triple *tr = raptor_sequence_get_at(ct->op->insert_templates, t);
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
            fs_binding *b = fs_binding_get(q->bb[0], v);
            if (b) {
                b->need_val = 1;
            } else {
                fs_binding_add(q->bb[0], v, FS_RID_NULL, 1);
            }
        }

        /* perform the WHERE match */
        fs_query_process_pattern(q, ct->op->where, vars);

        q->length = fs_binding_length(q->bb[0]);

        for (int s=0; s<4; s++) {
            vec[s] = fs_rid_vector_new(0);
        }
        for (int t=0; t<raptor_sequence_size(todel_p); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(todel_p, t);
            for (int row=0; row < q->length; row++) {
                delete_rasqal_triple(ct, vec, triple, row);
            }
            if (fs_rid_vector_length(vec[0]) > 1000) {
                fsp_delete_quads_all(ct->link, vec);
            }
        }
        if (fs_rid_vector_length(vec[0]) > 0) {
            fsp_delete_quads_all(ct->link, vec);
        }
        for (int s=0; s<4; s++) {
//fs_rid_vector_print(vec[s], 0, stdout);
            fs_rid_vector_free(vec[s]);
            vec[s] = NULL;
        }

        for (int t=0; t<raptor_sequence_size(toins_p); t++) {
            rasqal_triple *triple = raptor_sequence_get_at(toins_p, t);
            for (int row=0; row < q->length; row++) {
                insert_rasqal_triple(ct, triple, row);
            }
        }

        /* must not free the rasqal_query */
        q->rq = NULL;
        fs_query_free(q);
        ct->q = NULL;
    } else {
        todel = ct->op->delete_templates;
        toins = ct->op->insert_templates;
    }
#else
    if (ct->op->where) {
        fs_error(LOG_ERR, "DELETE/INSERT WHERE requires Rasqal 0.9.23 or newer");
        add_message(ct, "DELETE/INSERT WHERE requires Rasqal 0.9.23 or newer", 0);
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
            delete_rasqal_triple(ct, vec, triple, 0);
        }
        if (fs_rid_vector_length(vec[0]) > 0) {
            fsp_delete_quads_all(ct->link, vec);
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
            insert_rasqal_triple(ct, triple, 0);
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

    rasqal_free_query(rq);

    if (uctxt.messages) {
        int num_messages = g_slist_length(uctxt.messages);
        char **strv = calloc(sizeof(char *), num_messages+1);
        int pos = 0;
        for (GSList *it=uctxt.messages; it; it=it->next) {
            strv[pos++] = it->data;
        }
        *message = g_strjoinv("\n", strv);
        free(strv);
    }
    g_slist_free(uctxt.messages);
    for (GSList *it=uctxt.freeable; it; it=it->next) {
        g_free(it->data);
    }
    g_slist_free(uctxt.freeable);

    if (ok) {
        return 0;
    } else {
        return 1;
    }
}

fs_rid fs_hash_rasqal_literal(struct update_context *ct, rasqal_literal *l, int row)
{
    if (!l) return FS_RID_NULL;

    rasqal_literal_type type = l->type;
    switch (type) {
    case RASQAL_LITERAL_UNKNOWN:
        fs_error(LOG_ERR, "unknown literal type received");

        return FS_RID_NULL;

    case RASQAL_LITERAL_URI:
	return fs_hash_uri((char *)raptor_uri_as_string(l->value.uri));

    case RASQAL_LITERAL_STRING:
    case RASQAL_LITERAL_XSD_STRING: {
        fs_rid attr = 0;
        if (l->datatype) {
            attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
        } else if (l->language) {
            attr = fs_hash_literal((char *)l->language, 0);
        }

        return fs_hash_literal((char *)rasqal_literal_as_string(l), attr);
    }

    case RASQAL_LITERAL_BLANK: {
        raptor_term_blank_value bnode;
        bnode.string = (unsigned char *)rasqal_literal_as_string(l);
        bnode.string_len = strlen((char *)bnode.string);

        return fs_bnode_id(ct->link, bnode);
    }

    case RASQAL_LITERAL_VARIABLE:
        if (ct->q) {
            return fs_binding_get_val(ct->q->bb[0], l->value.variable, row, NULL);
        } else {
            fs_error(LOG_ERR, "no variables bound");
        }

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
            add_message(uc, g_strdup_printf("Imported <%s> into <%s>\n",
                resuri, graphuri), 1);
        } else {
            add_message(uc, g_strdup_printf("Imported <%s>\n", resuri), 1);
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
        add_message(uc, g_strdup_printf("Deleted <%s>\n", graphuri), 1);
    }
    fs_rid_vector_free(mvec);

    return errors;
}

/* vi:set expandtab sts=4 sw=4: */
