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
#include "common/4store.h"
#include "common/hash.h"
#include "common/error.h"
#include "common/rdf-constants.h"

enum update_op {
    OP_LOAD, OP_CLEAR
};

typedef struct _update_operation {
    enum update_op op;
    char *arg1;
    char *arg2;
    struct _update_operation *next;
} update_operation;

struct update_context {
    fsp_link *link;
    int segments;
    rasqal_world *rw;
    rasqal_query *rq;
    rasqal_query_verb verb;
    rasqal_literal *graph;
};

static int inited = 0;
static pcre *re_ws = NULL;
static pcre *re_load = NULL;
static pcre *re_clear = NULL;

fs_rid fs_hash_rasqal_literal(rasqal_literal *l);
void fs_resource_from_rasqal_literal(struct update_context *uctxt,
                                     rasqal_literal *l, fs_resource *res);

static void re_error(int rc)
{
    if (rc != PCRE_ERROR_NOMATCH) {
        fs_error(LOG_ERR, "PCRE error %d\n", rc);
    }
}

static update_operation *add_op(update_operation *head, enum update_op op, char *arg1, char *arg2)
{
    update_operation *tail = head;
    while (tail && tail->next) {
        tail = tail->next;
    }

    update_operation *newop = calloc(1, sizeof(update_operation));
    newop->op = op;
    newop->arg1 = g_strdup(arg1);
    if (arg2) {
        newop->arg2 = g_strdup(arg2);
    }
    if (tail) {
        tail->next = newop;
    
        return head;
    }

    return newop;
}

static void free_ops(update_operation *head)
{
    update_operation *ptr = head;
    while (ptr) {
        g_free(ptr->arg1);
        update_operation *next = ptr->next;
        free(ptr);
        ptr = next;
    }
}

static void update_walk(struct update_context *uctxt, rasqal_graph_pattern *node)
{
    rasqal_graph_pattern_operator op = rasqal_graph_pattern_get_operator(node);
    if (op == RASQAL_GRAPH_PATTERN_OPERATOR_BASIC ||
        op == RASQAL_GRAPH_PATTERN_OPERATOR_GROUP) {
        /* do nothing */
    } else if (op == RASQAL_GRAPH_PATTERN_OPERATOR_GRAPH) {
        uctxt->graph = rasqal_graph_pattern_get_origin(node);
    } else {
        fs_error(LOG_ERR, "enocuntered unexpected graph operator “%s”", rasqal_graph_pattern_operator_as_string(op));

        return;
    }
    for (int t=0; rasqal_graph_pattern_get_triple(node, t); t++) {
        rasqal_triple *triple = rasqal_graph_pattern_get_triple(node, t);
        fs_rid quad_buf[1][4];
        quad_buf[0][0] = fs_hash_rasqal_literal(uctxt->graph);
        quad_buf[0][1] = fs_hash_rasqal_literal(triple->subject);
        quad_buf[0][2] = fs_hash_rasqal_literal(triple->predicate);
        quad_buf[0][3] = fs_hash_rasqal_literal(triple->object);

        if (uctxt->verb == RASQAL_QUERY_VERB_INSERT) {
            if (quad_buf[0][0] == FS_RID_NULL) {
                quad_buf[0][0] = fs_c.default_graph;
            }

            fs_resource res;
            res.rid = quad_buf[0][0];
            if (uctxt->graph) {
                fs_resource_from_rasqal_literal(uctxt, uctxt->graph, &res);
            } else {
                res.lex = FS_DEFAULT_GRAPH;
                res.attr = FS_RID_NULL;
            }
            fsp_res_import(uctxt->link, FS_RID_SEGMENT(quad_buf[0][0], uctxt->segments), 1, &res);
            res.rid = quad_buf[0][1];
            fs_resource_from_rasqal_literal(uctxt, triple->subject, &res);
            fsp_res_import(uctxt->link, FS_RID_SEGMENT(quad_buf[0][1], uctxt->segments), 1, &res);
            res.rid = quad_buf[0][2];
            fs_resource_from_rasqal_literal(uctxt, triple->predicate, &res);
            fsp_res_import(uctxt->link, FS_RID_SEGMENT(quad_buf[0][2], uctxt->segments), 1, &res);
            res.rid = quad_buf[0][3];
            fs_resource_from_rasqal_literal(uctxt, triple->object, &res);
            fsp_res_import(uctxt->link, FS_RID_SEGMENT(quad_buf[0][3], uctxt->segments), 1, &res);

            fsp_quad_import(uctxt->link, FS_RID_SEGMENT(quad_buf[0][1], uctxt->segments),
                            FS_BIND_BY_SUBJECT, 1, quad_buf);
        } else {
            fs_error(LOG_ERR, "unhandled verb");
        }
    }
    for (int i=0; rasqal_graph_pattern_get_sub_graph_pattern(node, i); i++) {
        update_walk(uctxt, rasqal_graph_pattern_get_sub_graph_pattern(node, i));
    }
}

int fs_rasqal_update(fsp_link *l, char *update, char **message, int unsafe)
{
    rasqal_world *rworld = rasqal_new_world();
    rasqal_world_open(rworld);
    rasqal_query *rq = rasqal_new_query(rworld, "laqrs", NULL);
    if (!rq) {
        *message = g_strdup_printf("Unable to initialise update parser");
        rasqal_free_world(rworld);

        return 1;
    }
    rasqal_query_prepare(rq, (unsigned char *)update, NULL);
    rasqal_query_verb verb = rasqal_query_get_verb(rq);
    int ok = 0;
    switch (verb) {
    case RASQAL_QUERY_VERB_UNKNOWN:
        *message = g_strdup("Unknown update verb");
        break;
    case RASQAL_QUERY_VERB_SELECT:
    case RASQAL_QUERY_VERB_CONSTRUCT:
    case RASQAL_QUERY_VERB_DESCRIBE:
    case RASQAL_QUERY_VERB_ASK:
        *message = g_strdup("Query verb sent in update request");
        break;
    case RASQAL_QUERY_VERB_DELETE:
    case RASQAL_QUERY_VERB_INSERT:
        ok = 1;
        break;
    }

    if (!ok) {
        rasqal_free_query(rq);
        rasqal_free_world(rworld);

        return 1;
    }

    struct update_context uctxt;
    memset(&uctxt, 0, sizeof(uctxt));
    uctxt.link = l;
    uctxt.segments = fsp_link_segments(l);
    uctxt.rw = rworld;
    uctxt.rq = rq;
    uctxt.verb = verb;
    update_walk(&uctxt, rasqal_query_get_query_graph_pattern(rq));
    if (verb == RASQAL_QUERY_VERB_INSERT) {
        fsp_res_import_commit_all(l);
        fsp_quad_import_commit_all(l, FS_BIND_BY_SUBJECT);
    }

    rasqal_free_query(rq);
    rasqal_free_world(rworld);

    return 0;
}

int fs_update(fsp_link *l, char *update, char **message, int unsafe)
{
    *message = NULL;

    if (!inited) {
        const char *errstr = NULL;
        int erroffset = 0;

        if ((re_ws = pcre_compile("^\\s+", PCRE_UTF8, &errstr, &erroffset,
                                  NULL)) == NULL) {
            fs_error(LOG_ERR, "pcre compile failed: %s", errstr);
        }
        if ((re_load = pcre_compile("^\\s*LOAD\\s*<(.*?)>(?:\\s+INTO\\s<(.*?)>)?",
             PCRE_CASELESS | PCRE_UTF8, &errstr, &erroffset, NULL)) == NULL) {
            fs_error(LOG_ERR, "pcre compile failed: %s", errstr);
        }
        if ((re_clear = pcre_compile("^\\s*(?:CLEAR|DROP)\\s*GRAPH\\s*<(.*?)>",
             PCRE_CASELESS | PCRE_UTF8, &errstr, &erroffset, NULL)) == NULL) {
            fs_error(LOG_ERR, "pcre compile failed: %s", errstr);
        }
    }

    char tmpa[4096];
    char tmpb[4096];
    int rc;
    int ovector[30];
    int length = strlen(update);
    char *scan = update;
    int match;
    update_operation *ops = NULL;
    GString *mstr = g_string_new("");

    do {
        match = 0;

        rc = pcre_exec(re_ws, NULL, scan, length, 0, 0, ovector, 30);
        if (rc > 0) {
            match = 1;
            scan += ovector[1];
            length -= ovector[1];
        }

        rc = pcre_exec(re_load, NULL, scan, length, 0, 0, ovector, 30);
        if (rc > 0) {
            match = 1;
            pcre_copy_substring(scan, ovector, rc, 1, tmpa, sizeof(tmpa));
            if (rc == 2) {
                ops = add_op(ops, OP_LOAD, tmpa, NULL);
            } else {
                pcre_copy_substring(scan, ovector, rc, 2, tmpb, sizeof(tmpb));
                ops = add_op(ops, OP_LOAD, tmpa, tmpb);
            }
            scan += ovector[1];
            length -= ovector[1];
        } else {
            re_error(rc);
        }

        rc = pcre_exec(re_clear, NULL, scan, length, 0, 0, ovector, 30);
        if (rc > 0) {
            match = 1;
            pcre_copy_substring(scan, ovector, rc, 1, tmpa, sizeof(tmpa));
            ops = add_op(ops, OP_CLEAR, tmpa, NULL);
            scan += ovector[1];
            length -= ovector[1];
        } else {
            re_error(rc);
        }
    } while (match);

    if (length > 0) {
        free_ops(ops);
        g_string_free(mstr, TRUE);
        if (fs_rasqal_update(l, update, message, unsafe)) {
            if (!*message) {
                *message = g_strdup_printf("Syntax error in SPARUL command "
                                           "near “%.40s”\n", scan);
            }

            return 1;
        }

        return 0;
    }

    int errors = 0;

    for (update_operation *ptr = ops; ptr; ptr = ptr->next) {
        switch (ptr->op) {
        case OP_LOAD:;
            FILE *errout = NULL;
            errout = tmpfile();
            int count = 0;
            if (fsp_start_import_all(l)) {
                errors++;
                g_string_append(mstr, "aborting import\n");
                fclose(errout);
                free_ops(ops);
                *message = mstr->str;
                g_string_free(mstr, FALSE);

                break;
            }

            char *model = ptr->arg2 ? ptr->arg2 : ptr->arg1;
            fs_import(l, model, ptr->arg1, "auto", 0, 0, 0, errout, &count);
            fs_import_commit(l, 0, 0, 0, errout, &count);
            fsp_stop_import_all(l);
            rewind(errout);
            char tmp[1024];
            if (fgets(tmp, 1024, errout)) {
                errors++;
                g_string_append(mstr, tmp);
            } else {
                if (ptr->arg2) {
                    g_string_append_printf(mstr, "Imported <%s> into <%s>\n",
                                           ptr->arg1, ptr->arg2);
                } else {
                    g_string_append_printf(mstr, "Imported <%s>\n", ptr->arg1);
                }
            }
            fclose(errout);
            break;
        case OP_CLEAR:;
            fs_rid_vector *mvec = fs_rid_vector_new(0);
            fs_rid muri = fs_hash_uri(ptr->arg1);
            fs_rid_vector_append(mvec, muri);

            if (fsp_delete_model_all(l, mvec)) {
                errors++;
                g_string_append_printf(mstr, "error while trying to delete %s\n", ptr->arg1);
            } else {
                g_string_append_printf(mstr, "Deleted <%s>\n", ptr->arg1);
            }
            fs_rid_vector_free(mvec);
            break;
        }
    }

    free_ops(ops);
    *message = mstr->str;
    g_string_free(mstr, FALSE);

    return errors;
}

fs_rid fs_hash_rasqal_literal(rasqal_literal *l)
{
    if (!l) return FS_RID_NULL;

    rasqal_literal_type type = rasqal_literal_get_rdf_term_type(l);
    if (type == RASQAL_LITERAL_URI) {
	return fs_hash_uri((char *)raptor_uri_as_string(l->value.uri));
    } else if (type == RASQAL_LITERAL_STRING) {
        fs_rid attr = 0;
        if (l->datatype) {
            attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
        } else if (l->language) {
            attr = fs_hash_literal((char *)l->language, 0);
        }

        return fs_hash_literal((char *)rasqal_literal_as_string(l), attr);
    } else {
	fs_error(LOG_ERR, "bad rasqal literal in hash");
    }

    return FS_RID_NULL;
}

void fs_resource_from_rasqal_literal(struct update_context *uctxt, rasqal_literal *l, fs_resource *res)
{
    if (!l) {
        res->lex = "(null)";
        res->attr = FS_RID_NULL;

        return;
    }
    rasqal_literal_type type = rasqal_literal_get_rdf_term_type(l);
    if (type == RASQAL_LITERAL_URI) {
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

/* vi:set expandtab sts=4 sw=4: */
