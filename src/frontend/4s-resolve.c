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

#include "../common/4store.h"
#include "../common/hash.h"
#include "../common/error.h"

static int  segments;

static char *get_uri(fsp_link *link, fs_rid rid)
{
  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;

  if (fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource)) {
    return "#error URI#";
  }

  return (char *) resource.lex;
}

static char *get_attr(fsp_link *link, fs_rid rid)
{
  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;
  if (fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource)) {
    return "#error attr#";
  }

  return (char *) resource.lex;
}

static char *get_literal(fsp_link *link, fs_rid rid, fs_rid *attr)
{
  fs_rid_vector onerid = { .length = 1, .size = 1, .data = &rid };
  fs_resource resource;

  if (fsp_resolve(link, FS_RID_SEGMENT(rid, segments), &onerid, &resource)) {
    *attr = 0;
    return "#error literal#";
  }

  *attr = resource.attr;

  return (char *) resource.lex;
}

int main(int argc, char *argv[])
{
  char *password = fsp_argv_password(&argc, argv);

  if (argc < 3) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
    fprintf(stderr, "Usage: %s <kbname> <RID>\n", argv[0]);
    exit(1);
  }

  fsp_link *link = fsp_open_link(argv[1], password, FS_OPEN_HINT_RO);

  if (!link) {
    fs_error (LOG_ERR, "couldn't connect to “%s”", argv[1]);
    exit(2);
  }

  segments = fsp_link_segments(link);

  for (int v = 2; v < argc; ++v) {
    fs_rid rid, attr;

    rid = strtoull(argv[v], NULL, 16);

    if (FS_RID_NULL == rid) {
      printf("%016llX: RID NULL\n", rid);
    } else if (FS_IS_URI(rid)) {
      char *uri = get_uri(link, rid);
      printf("%016llX: <%s>\n", rid, uri);
    } else if (FS_IS_LITERAL(rid)) {
      char *lex = get_literal(link, rid, &attr);
      if (attr == 0) {
        printf("%016llX: %s\n", rid, lex);
      } else if (FS_IS_URI(attr)) {
        char *uri = get_uri(link, attr);
        printf("%016llX: %s^^<%s>\n", rid, lex, uri);
      } else if (FS_IS_LITERAL(attr)) {
        char *lang = get_attr(link, attr);
        printf("%016llX: %s@%s\n", rid, lex, lang);
      } else {
	printf("ERROR: Some sort of irregular literal\n");
        printf("%016llX: %s\n", rid, lex);
      }
    } else if (FS_IS_BNODE(rid)) {
      printf("%016llX: _:b%llu\n", rid, FS_BNODE_NUM(rid));
    } else {
      printf("ERROR: Unknown resource type\n");
    }
  }

  fsp_close_link(link);
}

