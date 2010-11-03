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
 *  Copyright 2006 Nick Lamb for Garlik.com
 */

#include <libxml/parser.h>
#include <string.h>
#include <stdlib.h>
#include <glib.h>

#include "../common/4store.h"
#include "../common/error.h"
#include "../common/hash.h"

enum xmlstate {
  WANT_TRIX,
  WANT_GRAPH,
  WANT_MODEL,
  WANT_MODEL_URI,
  WANT_TRIPLE,
  WANT_SUBJECT,
  WANT_SUBJECT_BNODE,
  WANT_SUBJECT_PLAIN_LITERAL,
  WANT_SUBJECT_TYPED_LITERAL,
  WANT_SUBJECT_URI,
  WANT_PREDICATE,
  WANT_PREDICATE_URI,
  WANT_OBJECT,
  WANT_OBJECT_BNODE,
  WANT_OBJECT_PLAIN_LITERAL,
  WANT_OBJECT_TYPED_LITERAL,
  WANT_OBJECT_URI,
  DONE_TRIPLE,
  DONE_TRIX,
};

typedef struct {
  fsp_link *link;
  int segments;
  fs_rid bnodemax;
  fs_rid m, s, p, o;
  gchar *resource;
  gchar *attr;
  enum xmlstate state;
} xmlctxt;

#define RES_BUF_SIZE 256

static fs_resource resources[FS_MAX_SEGMENTS][RES_BUF_SIZE];
static int res_count[FS_MAX_SEGMENTS];

static void insert_resource(xmlctxt *ctxt,
                            fs_rid rid, fs_rid attr, char* lex)
{
  /* TODO a cache might speed things up */
  int segment = FS_RID_SEGMENT(rid, ctxt->segments);

  resources[segment][res_count[segment]].rid = rid;
  resources[segment][res_count[segment]].attr = attr;
  resources[segment][res_count[segment]].lex = g_strdup(lex);

  if (++res_count[segment] == RES_BUF_SIZE) {
    fsp_res_import(ctxt->link, segment, RES_BUF_SIZE, resources[segment]);
    for (int k = 0; k < RES_BUF_SIZE; ++k) {
      g_free(resources[segment][k].lex);
    }
    res_count[segment] = 0;
  }
}

static fs_rid insert_bnode(xmlctxt *ctxt)
{
  fs_rid bnode = atoll(ctxt->resource);
  /* TODO catch other people's non-numeric bNode IDs */

  while (ctxt->bnodemax < bnode) {
    fs_rid bnodenext;
    fsp_bnode_alloc(ctxt->link, 1000000, &bnodenext, &(ctxt->bnodemax));
printf("(allocated bNode %lld to %lld)\n", bnodenext, ctxt->bnodemax);
  }

  return bnode | 0x8000000000000000LL;
}

static fs_rid insert_uri(xmlctxt *ctxt)
{
  char *uri = ctxt->resource;
  if (!uri || uri[0] == '\0') {
    fs_error(LOG_ERR, "NULL URI inserted");
    return 0;
  }
  fs_rid r = fs_hash_uri(uri);
  insert_resource(ctxt, r, fs_c.empty, uri);

  return r;
}

static fs_rid insert_plain(xmlctxt *ctxt)
{
  char *text = ctxt->resource;
  if (!text) {
    text = ""; /* this case is actually the empty string */
  }
  fs_rid lang = fs_c.empty;
  if (ctxt->attr) {
    lang = fs_hash_literal(ctxt->attr, fs_c.empty);
    insert_resource(ctxt, lang, fs_c.empty, ctxt->attr);
  }
  fs_rid r = fs_hash_literal(text, lang);
  insert_resource(ctxt, r, lang, text);

  return r;
}

static fs_rid insert_typed(xmlctxt *ctxt)
{
  char *text = ctxt->resource;
  fs_rid dt = fs_c.empty;
  if (ctxt->attr) {
    dt = fs_hash_uri(ctxt->attr);
    insert_resource(ctxt, dt, fs_c.empty, ctxt->attr);
  } else {
    fs_error(LOG_ERR, "NULL type URI inserted");
  }
  fs_rid r = fs_hash_literal(text, dt);
  insert_resource(ctxt, r, dt, text);

  return r;
}

static void insert_quad(xmlctxt *ctxt)
{
  fs_rid buffer[1][4];
  int subj = FS_RID_SEGMENT(ctxt->s, ctxt->segments);
  int obj = FS_RID_SEGMENT(ctxt->o, ctxt->segments);

  buffer[0][0] = ctxt->m;
  buffer[0][1] = ctxt->s;
  buffer[0][2] = ctxt->p;
  buffer[0][3] = ctxt->o;

  fsp_quad_import(ctxt->link, subj, FS_BIND_BY_SUBJECT, 1, buffer);
  fsp_quad_import(ctxt->link, obj, FS_BIND_BY_OBJECT, 1, buffer);
}

static void xml_start_document(void *user_data)
{
  xmlctxt *ctxt = (xmlctxt *) user_data;
  ctxt->state = WANT_TRIX;
}

static void xml_end_document(void *user_data)
{
  xmlctxt *ctxt = (xmlctxt *) user_data;
  if (ctxt->state != DONE_TRIX) {
    fs_error(LOG_ERR, "TriX document ends abruptly");
  }
  g_free(ctxt->resource);
  ctxt->resource = NULL;
  g_free(ctxt->attr);
  ctxt->attr = NULL;
}
void xml_start_element(void *user_data,
                       const xmlChar *name, const xmlChar **attrs)
{
  xmlctxt *ctxt = (xmlctxt *) user_data;
  switch  (ctxt->state) {
    case WANT_TRIX:
      if (strcmp((char *) name, "TriX")) {
        fs_error(LOG_ERR, "Not a TriX document");
        exit(1);
      } else {
        ctxt->state = WANT_GRAPH;
      }
      break;

    case WANT_GRAPH:
      if (strcmp((char *) name, "graph")) {
        fs_error(LOG_WARNING, "expected <graph> found <%s>", name);
      } else {
        ctxt->state = WANT_MODEL;
      }
      break;

    case WANT_MODEL:
      g_free(ctxt->resource);
      ctxt->resource = NULL;
      g_free(ctxt->attr);
      ctxt->attr = NULL;
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected <uri> found <%s>", name);
      } else {
        ctxt->state = WANT_MODEL_URI;
      }
      break;

    case WANT_TRIPLE:
      if (strcmp((char *) name, "triple")) {
        fs_error(LOG_WARNING, "expected <triple> found <%s>", name);
      } else {
        ctxt->state = WANT_SUBJECT;
      }
      break;

    case WANT_SUBJECT:
      g_free(ctxt->resource);
      ctxt->resource = NULL;
      g_free(ctxt->attr);
      ctxt->attr = NULL;
      if (!strcmp((char *) name, "uri")) {
        ctxt->state = WANT_SUBJECT_URI;
      } else if (!strcmp((char *) name, "id")) {
        ctxt->state = WANT_SUBJECT_BNODE;
      } else if (!strcmp((char *) name, "plainLiteral")) {
        if (attrs && !strcmp((char *) attrs[0], "xml:lang")) {
          ctxt->attr = g_strdup((char *) attrs[1]);
        }
        ctxt->state = WANT_SUBJECT_PLAIN_LITERAL;
      } else if (!strcmp((char *) name, "typedLiteral")) {
        if (attrs && !strcmp((char *) attrs[0], "datatype")) {
          ctxt->attr = g_strdup((char *) attrs[1]);
        } else {
          fs_error(LOG_WARNING, "missing datatype on typed literal");
        }
        ctxt->state = WANT_SUBJECT_TYPED_LITERAL;
      } else {
        fs_error(LOG_WARNING, "expected subject found <%s>", name);
      }
      break;

    case WANT_PREDICATE:
      g_free(ctxt->resource);
      ctxt->resource = NULL;
      g_free(ctxt->attr);
      ctxt->attr = NULL;
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected <uri> found <%s>", name);
      } else {
        ctxt->state = WANT_PREDICATE_URI;
      }
      break;

    case WANT_OBJECT:
      g_free(ctxt->resource);
      ctxt->resource = NULL;
      g_free(ctxt->attr);
      ctxt->attr = NULL;
      if (!strcmp((char *) name, "uri")) {
        ctxt->state = WANT_OBJECT_URI;
      } else if (!strcmp((char *) name, "id")) {
        ctxt->state = WANT_OBJECT_BNODE;
      } else if (!strcmp((char *) name, "plainLiteral")) {
        if (attrs && !strcmp((char *) attrs[0], "xml:lang")) {
          ctxt->attr = g_strdup((char *) attrs[1]);
        }
        ctxt->state = WANT_OBJECT_PLAIN_LITERAL;
      } else if (!strcmp((char *) name, "typedLiteral")) {
        if (attrs && !strcmp((char *) attrs[0], "datatype")) {
          ctxt->attr = g_strdup((char *) attrs[1]);
        } else {
          fs_error(LOG_WARNING, "missing datatype on typed literal");
        }
        ctxt->state = WANT_OBJECT_TYPED_LITERAL;
      } else {
        fs_error(LOG_WARNING, "expected object found <%s>", name);
      }
      break;

    case WANT_MODEL_URI:
    case WANT_SUBJECT_BNODE:
    case WANT_SUBJECT_PLAIN_LITERAL:
    case WANT_SUBJECT_TYPED_LITERAL:
    case WANT_SUBJECT_URI:
    case WANT_PREDICATE_URI:
    case WANT_OBJECT_BNODE:
    case WANT_OBJECT_PLAIN_LITERAL:
    case WANT_OBJECT_TYPED_LITERAL:
    case WANT_OBJECT_URI:
    case DONE_TRIPLE:
    case DONE_TRIX:
      fs_error(LOG_WARNING, "impossible document structure");
      break;
  }
}

void xml_end_element(void *user_data, const xmlChar *name)
{
  xmlctxt *ctxt = (xmlctxt *) user_data;
  switch  (ctxt->state) {
    case WANT_TRIX:
      fs_error(LOG_WARNING, "impossible document structure");
      break;

    case WANT_GRAPH:
      if (strcmp((char *) name, "TriX")) {
        fs_error(LOG_WARNING, "expected </TriX> found </%s>", name);
      } else {
        ctxt->state = DONE_TRIX;
      }
      break;

    case WANT_TRIPLE:
      if (strcmp((char *) name, "graph")) {
        fs_error(LOG_WARNING, "expected </graph> found </%s>", name);
      } else {
        ctxt->state = WANT_GRAPH;
      }
      break;

    case WANT_MODEL_URI:
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected </uri> found </%s>", name);
      } else {
        ctxt->m = insert_uri(ctxt);
        ctxt->state = WANT_TRIPLE;
      }
      break;

    case WANT_SUBJECT:
    case WANT_PREDICATE:
    case WANT_OBJECT:
      fs_error(LOG_ERR, "missing part of triple");
      ctxt->state = WANT_TRIPLE;
      break;

    case WANT_SUBJECT_BNODE:
      if (strcmp((char *) name, "id")) {
        fs_error(LOG_WARNING, "expected </id> found </%s>", name);
      } else {
        ctxt->s = insert_bnode(ctxt);
        ctxt->state = WANT_PREDICATE;
      }
      break;

    case WANT_SUBJECT_PLAIN_LITERAL:
      if (strcmp((char *) name, "plainLiteral")) {
        fs_error(LOG_WARNING, "expected </plainLiteral> found </%s>", name);
      } else {
        ctxt->s = insert_plain(ctxt);
        ctxt->state = WANT_PREDICATE;
      }
      break;

    case WANT_SUBJECT_TYPED_LITERAL:
      if (strcmp((char *) name, "typedLiteral")) {
        fs_error(LOG_WARNING, "expected </typedLiteral> found </%s>", name);
      } else {
        ctxt->s = insert_typed(ctxt);
        ctxt->state = WANT_PREDICATE;
      }
      break;

    case WANT_SUBJECT_URI:
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected </uri> found </%s>", name);
      } else {
        ctxt->s = insert_uri(ctxt);
        ctxt->state = WANT_PREDICATE;
      }
      break;

    case WANT_PREDICATE_URI:
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected </uri> found </%s>", name);
      } else {
        ctxt->p = insert_uri(ctxt);
        ctxt->state = WANT_OBJECT;
      }
      break;

    case WANT_OBJECT_BNODE:
      if (strcmp((char *) name, "id")) {
        fs_error(LOG_WARNING, "expected </id> found </%s>", name);
      } else {
        ctxt->o = insert_bnode(ctxt);
        ctxt->state = DONE_TRIPLE;
      }
      break;

    case WANT_OBJECT_PLAIN_LITERAL:
      if (strcmp((char *) name, "plainLiteral")) {
        fs_error(LOG_WARNING, "expected </plainLiteral> found </%s>", name);
      } else {
        ctxt->o = insert_plain(ctxt);
        ctxt->state = DONE_TRIPLE;
      }
      break;

    case WANT_OBJECT_TYPED_LITERAL:
      if (strcmp((char *) name, "typedLiteral")) {
        fs_error(LOG_WARNING, "expected </typedLiteral> found </%s>", name);
      } else {
        ctxt->o = insert_typed(ctxt);
        ctxt->state = DONE_TRIPLE;
      }
      break;

    case WANT_OBJECT_URI:
      if (strcmp((char *) name, "uri")) {
        fs_error(LOG_WARNING, "expected </uri> found </%s>", name);
      } else {
        ctxt->o = insert_uri(ctxt);
        ctxt->state = DONE_TRIPLE;
      }
      break;

    case DONE_TRIPLE:
      if (strcmp((char *) name, "triple")) {
        fs_error(LOG_WARNING, "expected </triple> found </%s>", name);
      } else {
        insert_quad(ctxt);
        ctxt->state = WANT_TRIPLE;
      }
      break;

    case WANT_MODEL:
    case DONE_TRIX:
      fs_error(LOG_WARNING, "impossible document structure");
      break;
  }
}

void xml_characters(void *user_data, const xmlChar *ch, int len)
{
  xmlctxt *ctxt = (xmlctxt *) user_data;
  switch  (ctxt->state) {
    case WANT_SUBJECT_BNODE:
    case WANT_SUBJECT_PLAIN_LITERAL:
    case WANT_SUBJECT_TYPED_LITERAL:
    case WANT_SUBJECT_URI:
    case WANT_PREDICATE_URI:
    case WANT_OBJECT_BNODE:
    case WANT_OBJECT_PLAIN_LITERAL:
    case WANT_OBJECT_TYPED_LITERAL:
    case WANT_OBJECT_URI:
    case WANT_MODEL_URI:
      if (ctxt->resource) {
        gchar *tmp = ctxt->resource;
        ctxt->resource = g_strdup_printf("%s%.*s", tmp, len, ch);
        g_free(tmp);
      } else {
        ctxt->resource = g_strndup((gchar *) ch, len); 
      }
      break;

    default:
      /* not important */
      break;
  }
  
}

xmlEntityPtr xml_get_entity(void * user_data, const xmlChar *name)
{
  return xmlGetPredefinedEntity(name);
}

xmlSAXHandler sax = {
  .startDocument = xml_start_document,
  .endDocument = xml_end_document,
  .startElement = xml_start_element,
  .endElement = xml_end_element,
  .characters = xml_characters,
};

static void flush_resources(xmlctxt *ctxt)
{
  for (int s = 0; s < ctxt->segments; ++s) {
    if (res_count[s] > 0) {
      fsp_res_import(ctxt->link, s, res_count[s], resources[s]);
      for (int k = 0; k < res_count[s]; ++k) {
        g_free(resources[s][k].lex);
      }
      res_count[s] = 0;
    }
    if (fsp_res_import_commit(ctxt->link, s)) {
      fs_error(LOG_ERR, "error during commit of resources");
    }
  }
}

static void flush_quads(xmlctxt *ctxt)
{
  for (int s = 0; s < ctxt->segments; ++s) {
//    quad_count[s] = 0;
    if (fsp_quad_import_commit(ctxt->link, s, FS_BIND_BY_SUBJECT)) {
      fs_error(LOG_ERR, "error during commit of quads by-subject");
    }
    if (fsp_quad_import_commit(ctxt->link, s, FS_BIND_BY_OBJECT)) {
      fs_error(LOG_ERR, "error during commit of quads by-object");
    }
  }
}

void restore_file(fsp_link *link, char *filename)
{
  xmlctxt *ctxt = calloc(sizeof(xmlctxt), 1);
  ctxt->link = link;
  ctxt->segments = fsp_link_segments(link);

  for (int s = 0; s < ctxt->segments; ++s) {
    res_count[s] = 0;
  }

  xmlSAXUserParseFile(&sax, (void *) ctxt, filename);
  flush_resources(ctxt);
  flush_quads(ctxt);
  free(ctxt);
}

