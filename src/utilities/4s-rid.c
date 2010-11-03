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
 *  Copyright 2006-7 Nick Lamb for Garlik.com
 */

/* Sample usage: 4s-rid 'uri'
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "../common/4store.h"
#include "../common/hash.h"
#include "../common/error.h"
#include "../common/params.h"

int main(int argc, char *argv[])
{
  if (argc != 2) {
    fprintf(stderr, "%s revision %s\n", argv[0], FS_BACKEND_VER);
    fprintf(stderr, "Usage: %s <uri> | \"literal\"\n", argv[0]);
    exit(1);
  }

  char *string = argv[1];
  char lex[128], lang[128], type[128], uri[128];
  fs_rid rid;

#ifdef FS_MD5
  fs_hash_init(FS_HASH_MD5);
#endif
#ifdef FS_CRC64
  fs_hash_init(FS_HASH_CRC64);
#endif
#ifdef FS_UMAC
  fs_hash_init(FS_HASH_UMAC);
#endif

  if (sscanf(string, "\"%127[^\"]\"@%127s", lex, lang) == 2) {
    rid = fs_hash_literal(lex,fs_hash_literal(lang, 0));
  } else if (sscanf(string, "\"%127[^\"]\"^^%127s", lex, type) == 2) {
    rid = fs_hash_literal(lex,fs_hash_uri(type));
  } else if (sscanf(string, "\"%127[^\"]\"", lex) == 1) {
    rid = fs_hash_literal(lex, 0);
  } else if (sscanf(string, "<%127[^>]>", uri) == 1) {
    rid = fs_hash_uri(uri);
  } else {
    fprintf(stderr, "Couldn't recognise a URI or literal in string '%s'\n", string);
    exit(1);
  }

  printf("%016llX\n", rid);
}
