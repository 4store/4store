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

/* Sample usage
 *
 * 4s-bind sparql_test many FS_BIND_SUBJECT FS_BIND_PREDICATE FS_BIND_OBJECT FS_BIND_BY_SUBJECT /dev/null subjects /dev/null /dev/null
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "../common/4store.h"
#include "../common/hash.h"
#include "../common/error.h"

static int  segments;

static char *flag_name[] = {
  "FS_BIND_MODEL",
  "FS_BIND_SUBJECT",
  "FS_BIND_PREDICATE",
  "FS_BIND_OBJECT",
  "FS_BIND_DISTINCT",
  "FS_BIND_OPTIONAL",
  "FS_BIND_UNION",
  "FS_BIND_SAME_XXXX",
  "FS_BIND_SAME_XXAA",
  "FS_BIND_SAME_XAXA",
  "FS_BIND_SAME_XAAX",
  "FS_BIND_SAME_XAAA",
  "FS_BIND_SAME_AXXA",
  "FS_BIND_SAME_AXAX",
  "FS_BIND_SAME_AXAA",
  "FS_BIND_SAME_AAXX",
  "FS_BIND_SAME_AAXA",
  "FS_BIND_SAME_AAAX",
  "FS_BIND_SAME_AAAA",
  "FS_BIND_SAME_AABB",
  "FS_BIND_SAME_ABAB",
  "FS_BIND_SAME_ABBA",

  "FS_BIND_BY_SUBJECT",
  "FS_BIND_BY_OBJECT",
};

static int flag_value[] = {
  FS_BIND_MODEL,
  FS_BIND_SUBJECT,
  FS_BIND_PREDICATE,
  FS_BIND_OBJECT,
  FS_BIND_DISTINCT,
  FS_BIND_OPTIONAL,
  FS_BIND_UNION,
  FS_BIND_SAME_XXXX,
  FS_BIND_SAME_XXAA,
  FS_BIND_SAME_XAXA,
  FS_BIND_SAME_XAAX,
  FS_BIND_SAME_XAAA,
  FS_BIND_SAME_AXXA,
  FS_BIND_SAME_AXAX,
  FS_BIND_SAME_AXAA,
  FS_BIND_SAME_AAXX,
  FS_BIND_SAME_AAXA,
  FS_BIND_SAME_AAAX,
  FS_BIND_SAME_AAAA,
  FS_BIND_SAME_AABB,
  FS_BIND_SAME_ABAB,
  FS_BIND_SAME_ABBA,

  FS_BIND_BY_SUBJECT,
  FS_BIND_BY_OBJECT,
};

static fs_rid_vector *rid_file(char *filename)
{
  fs_rid_vector *rids = fs_rid_vector_new(0);
  FILE *fp = fopen(filename, "r");

  if (!fp) {
    fs_error(LOG_ERR, "could not open “%s”: %s", filename, strerror(errno));
    return rids;
  }

  while (!feof(fp) && !ferror(fp)) {
    char ridstr[21];
    fs_rid rid;
    if (fscanf(fp, "%20s", ridstr) < 1) break;

    rid = strtoull(ridstr, NULL, 16);
    fs_rid_vector_append(rids, rid);
  }

  fclose(fp);

  return rids;
}

int main(int argc, char *argv[])
{
  char *password = fsp_argv_password(&argc, argv);

  int flags = 0, many = 0, all = 0;
  int seg = 0; /* deliberately using signed type */
  fs_rid_vector *mrids= NULL, *srids= NULL, *prids= NULL, *orids= NULL;
  fs_rid_vector **result = NULL;

  if (argc < 7) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
    fprintf(stderr, "Usage: %s <kbname> { many | all | seg# } <flags>\n", argv[0]);
    fprintf(stderr, " mrid-file srid-file prid-file orid-file [offset limit]\n");
    fprintf(stderr, "For flags use FS_BIND_... symbols or a numeric value\n");
    fprintf(stderr, "RID files are one RID per line\n");
    exit(1);
  }

  char *kbname = argv[1];

  if (!strcasecmp(argv[2], "many")) {
    many = 1;
  } else if (!strcasecmp(argv[2], "all")) {
    all = 1;
  } else {
    seg = atoi(argv[2]);
  }

  int param = 3;

  flags = strtol(argv[param], NULL, 0);

  if (flags == 0) { /* symbolic flags, hopefully */
    while (param < argc) {
      const int len = sizeof(flag_name) / sizeof(char *);
      int k;
      for (k = 0; k < len; ++k) {
        if (!strcmp(flag_name[k], argv[param])) {
          flags |= flag_value[k];
          break;
        }
      }
      if (k == len) break;
      param ++;
    }
  } else {
    param ++; /* done with the numeric flags then */
  }

  if (argc < param + 4) {
    fprintf(stderr, "Wrong number of arguments\n");
    exit(1);
  }

  mrids = rid_file(argv[param++]);
  srids = rid_file(argv[param++]);
  prids = rid_file(argv[param++]);
  orids = rid_file(argv[param++]);

  int limit, offset;

  if (argc == param ) {
    /* defaults */
    limit = -1;
    offset = -1;
  } else if (argc > param + 2) {
    fprintf(stderr, "Wrong number of arguments\n");
    exit(1);
  } else if (argc < param + 2) {
    fprintf(stderr, "Wrong number of arguments\n");
    exit(1);
  } else {
    offset = atoi(argv[param]);
    limit = atoi(argv[param + 1]);
  }

  fsp_link *link = fsp_open_link(kbname, password, FS_OPEN_HINT_RO);

  if (!link) {
    fs_error (LOG_ERR, "couldn't connect to “%s”", argv[1]);
    exit(2);
  }

  segments = fsp_link_segments(link);

  if (seg < 0 || seg > segments) {
    fs_error (LOG_ERR, "Segment %d out of range (0-%u)", seg, segments);
    exit(1);
  }

  double then = fs_time();
  int ans = 0;

  if (all) {
    ans = fsp_bind_limit_all(link, flags, mrids, srids, prids, orids, &result, offset, limit);
  } else if (many) {
    ans = fsp_bind_limit_many(link, flags, mrids, srids, prids, orids, &result, offset, limit);
  } else {
    ans = fsp_bind_limit(link, seg, flags, mrids, srids, prids, orids, &result, offset, limit);
  }

  double time_binding = fs_time() - then;
  if (ans != 0) exit(1);

  /* print results */

  int cols = 0;
  for (int k = 0; k < 4; ++k) {
    if (flags & (1 << k)) cols++;
  }

  if (!result) {
    printf("NO MATCH found.\n");
  } else if (cols == 0) {
    printf("MATCH found.\n");
  } else if (result[0]) {
    int length = result[0]->length;

    if (flags & FS_BIND_MODEL) printf("-----Model------  ");
    if (flags & FS_BIND_SUBJECT) printf("----Subject-----  ");
    if (flags & FS_BIND_PREDICATE) printf("----Predicate---  ");
    if (flags & FS_BIND_OBJECT) printf("-----Object-----");
    putchar('\n');

    for (int k = 0; k < length; ++k) {
      for (int c = 0; c < cols; ++c) {
        printf("%016llX  ", result[c]->data[k]);
      }
      putchar('\n');
    }
  }

  fprintf(stderr, "bind took %f seconds on client\n", time_binding);

  fs_query_timing times;
  if (all || many) {
    fprintf(stderr, "binding on all or many segments, times in seconds...\n");
    for (int s = 0; s < segments; ++s) {
      fsp_get_query_times(link, s, &times);
      if (times.bind > 0.0f) {
        fprintf(stderr, "%d: %f\n", s, times.bind);
      }
    }
    fputc('\n', stderr);
  } else {
    fsp_get_query_times(link, seg, &times);
    fprintf(stderr, "binding segment %d took %f seconds\n", seg, times.bind);
  }

  fsp_close_link(link);
}
