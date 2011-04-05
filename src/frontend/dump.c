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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libxml/xmlwriter.h>
#include <glib.h>

#include "../common/4store.h"
#include "../common/server.h"
#include "../common/error.h"

#define QUAD_LIMIT 2000

#define BIND_SPO (FS_BIND_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT | FS_BIND_BY_SUBJECT)

#define CACHE_SIZE (65536 * 128)
#define CACHE_MASK (CACHE_SIZE-1)

#define ATTR_CACHE_SIZE (256)
#define ATTR_CACHE_MASK (ATTR_CACHE_SIZE-1)

static fs_resource cache[CACHE_SIZE];
static fs_resource attr_cache[CACHE_SIZE];

static double time_resolving = 0.0;
static double time_bind_first = 0.0;
static double time_bind_next = 0.0;
static double time_write_out = 0.0;

static int segments = 0;

xmlChar *get_uri(fsp_link *link, fs_rid rid)
{
  if (cache[rid & CACHE_MASK].rid == rid) {
    return (xmlChar *) cache[rid & CACHE_MASK].lex;
  }

  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;
  fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource);

  return (xmlChar *) resource.lex;
}

xmlChar *get_attr(fsp_link *link, fs_rid rid)
{
  if (attr_cache[rid & CACHE_MASK].rid == rid) {
    return (xmlChar *) attr_cache[rid & CACHE_MASK].lex;
  }

  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;

  fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource);
  memcpy(&attr_cache[rid & ATTR_CACHE_MASK], &resource, sizeof(fs_resource));

  return (xmlChar *) resource.lex;
}

xmlChar *get_literal(fsp_link *link, fs_rid rid, fs_rid *attr)
{
  if (cache[rid & CACHE_MASK].rid == rid) {
    *attr = cache[rid & CACHE_MASK].attr;
    return (xmlChar *) cache[rid & CACHE_MASK].lex;
  }

  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;

  fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource);
  *attr = resource.attr;

  return (xmlChar *) resource.lex;
}

void resolve_triples(fsp_link *link, fs_rid_vector **rids)
{
  int quads = rids[0]->length;
  fs_rid_vector *todo[segments];
  fs_segment segment;

  for (segment = 0; segment < segments; ++segment) {
    todo[segment] = fs_rid_vector_new(0);
  }
  for (int c = 0; c < 3; ++c) {
    for (int k = 0; k < quads; ++k) {
      const fs_rid rid = rids[c]->data[k];
      if (FS_IS_BNODE(rid) || cache[rid & CACHE_MASK].rid == rid) continue;
      fs_rid_vector_append(todo[FS_RID_SEGMENT(rid, segments)], rid);
      cache[rid & CACHE_MASK].rid = rid; /* well, it will be soon */
    }
  } 

  int length[segments];
  fs_resource *resources[segments];
  for (segment = 0; segment < segments; ++segment) {
    length[segment] = todo[segment]->length;
    resources[segment] = calloc(length[segment], sizeof(fs_resource));
  }

  fsp_resolve_all(link, todo, resources);

  for (segment = 0; segment < segments; ++segment) {
    fs_resource *res = resources[segment];
    for (int k = 0; k < length[segment]; ++k) {
      free(cache[res[k].rid & CACHE_MASK].lex);
      memcpy(&cache[res[k].rid & CACHE_MASK], &res[k], sizeof(fs_resource));
    }

    fs_rid_vector_free(todo[segment]);
    free(resources[segment]);
  }
}

void dump_model(fsp_link *link, fs_rid model, xmlTextWriterPtr xml)
{
  fs_rid_vector none = { .length = 0, .size = 0, .data = 0 };
  fs_rid_vector one = { .length = 1, .size = 1, .data = &model };

  fs_rid_vector **results;

  double then; /* for time keeping */

  then = fs_time();
  fsp_bind_first_all(link, BIND_SPO, &one, &none, &none, &none, &results, QUAD_LIMIT);
  time_bind_first += (fs_time() - then);

  while (results != NULL) {

    long length = results[0]->length;

    if (length == 0) break;

    then = fs_time();
    resolve_triples(link, results);
    time_resolving += (fs_time() - then);

    then = fs_time();
    for (int k = 0; k < length; ++k) {
      xmlTextWriterStartElement(xml, (xmlChar *) "triple");

      for (int r = 0; r < 3; ++r) {
        fs_rid rid = results[r]->data[k];
        if (FS_IS_BNODE(rid)) {
          unsigned long long node = FS_BNODE_NUM(rid);
          xmlTextWriterWriteFormatElement(xml, (xmlChar *) "id", "%llu", node);
        } else if (FS_IS_URI(rid)) {
          xmlChar *uri = get_uri(link, rid);
          xmlTextWriterWriteElement(xml, (xmlChar *) "uri", uri);
        } else if (FS_IS_LITERAL(rid)) {
          fs_rid attr;
          xmlChar *lex = get_literal(link, rid, &attr);
          if (attr == fs_c.empty) {
            xmlTextWriterWriteElement(xml, (xmlChar *) "plainLiteral", lex);
          } else if (FS_IS_URI(attr)) {
            xmlChar *type = get_uri(link, attr);
            xmlTextWriterStartElement(xml, (xmlChar *) "typedLiteral");
            xmlTextWriterWriteString(xml, (xmlChar *) lex);
            xmlTextWriterWriteAttribute(xml, (xmlChar *) "datatype", type);
            xmlTextWriterEndElement(xml);
          } else if (FS_IS_LITERAL(attr)) {
            xmlChar *lang = get_attr(link, attr);
            xmlTextWriterStartElement(xml, (xmlChar *) "plainLiteral");
            xmlTextWriterWriteAttribute(xml, (xmlChar *) "xml:lang", lang);
            xmlTextWriterWriteString(xml, (xmlChar *) lex);
            xmlTextWriterEndElement(xml);
          }
        }
      }
      xmlTextWriterEndElement(xml);
      xmlTextWriterWriteString(xml, (xmlChar *) "\n");

    }
    time_write_out += (fs_time() - then);

    fs_rid_vector_free(results[0]);
    fs_rid_vector_free(results[1]);
    fs_rid_vector_free(results[2]);
    free(results);

    then = fs_time();
    fsp_bind_next_all(link, BIND_SPO, &results, QUAD_LIMIT);
    time_bind_next += (fs_time() - then);
  }

  fsp_bind_done_all(link);
}

void dump_trix(fsp_link *link, xmlTextWriterPtr xml)
{
  fs_rid_vector **models;
  fs_rid_vector none = { .length = 0, .size = 0, .data = 0 };

  fsp_bind_all(link, FS_BIND_DISTINCT | FS_BIND_MODEL | FS_BIND_BY_SUBJECT, &none, &none, &none, &none, &models);

  fs_rid_vector_sort(models[0]);
  fs_rid_vector_uniq(models[0], 1);

  long length = models[0]->length;

  for (int k = 0; k < length; ++k) {
    fs_rid model = models[0]->data[k];
    xmlChar *model_uri = get_uri(link, model);
    xmlTextWriterStartElement(xml, (xmlChar *) "graph");
    if (FS_IS_URI(model)) {
      xmlTextWriterWriteElement(xml, (xmlChar *) "uri", model_uri);
    } else {
      fs_error(LOG_WARNING, "model %lld is not a URI", model);
    }

    dump_model(link, model, xml);
    xmlTextWriterEndElement(xml);
    xmlTextWriterWriteString(xml, (xmlChar *) "\n");
printf("%5d/%ld: %4.5f %4.5f %4.5f %4.5f\n", k + 1, length, time_resolving, time_bind_first, time_bind_next, time_write_out);
  }
}

void dump_file(fsp_link *link, char *filename)
{
  xmlTextWriterPtr xml  = xmlNewTextWriterFilename(filename, TRUE);

  if (!xml) {
    fs_error(LOG_ERR, "Couldn't write output file, giving up");
    exit(4);
  }

  xmlTextWriterStartDocument(xml, NULL, NULL, NULL);
  xmlTextWriterStartElement(xml, (xmlChar *) "TriX");
  dump_trix(link, xml);
  xmlTextWriterEndDocument(xml); /* also closes TriX */
  xmlFreeTextWriter(xml);
}

int main(int argc, char *argv[])
{
  char *password = fsp_argv_password(&argc, argv);

  if (argc != 3) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
    fprintf(stderr, "Usage: %s <kbname> <uri>\n", argv[0]);
    exit(1);
  }

  fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RO);

  if (!link) {
    fs_error (LOG_ERR, "couldn't connect to “%s”", argv[1]);
    exit(2);
  }

  fs_hash_init(fsp_hash_type(link));
  segments = fsp_link_segments(link);
  dump_file(link, argv[2]);

  fsp_close_link(link);
}

