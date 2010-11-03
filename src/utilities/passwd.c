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
#include <time.h>
#include <glib.h>
#include <libgen.h>

#include "../common/params.h"
#include "../common/md5.h"
#include "../common/gnu-options.h"

#include "../backend/metadata.h"

int main (int argc, char *argv[])
{
  fs_gnu_options(argc, argv, "<kbname> <password>\n");

  if (argc != 3) {
    printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
    fprintf(stderr, "Usage: %s <kbname> <password>\n", basename(argv[0]));
    exit(1);
  }

  char *kbname = argv[1];
  char *password = argv[2];

  fs_metadata *md = fs_metadata_open(kbname);
  if (!md) {
    fprintf(stderr, "Couldn't open metadata for KB “%s”\n", kbname);
    exit(2);
  }

  md5_state_t md5;
  unsigned char stage1[20], stage2[16];
  char hash[33] = "none";
  int now = 0;
  char *pw = g_strdup_printf("%s:%s", kbname, password);

  /* stage1 will contain the 4 byte Unix time_t value as a salt ... */
  now = time(NULL);
  memcpy(stage1, &now, sizeof(now));

  /* ... followed by the on-wire 16 byte MD5 auth string */
  md5_init(&md5);
  md5_append(&md5, (md5_byte_t *) pw, strlen(pw));
  md5_finish(&md5, stage1 + 4);

  /* now use MD5 on all 20 bytes and store both the salt and the hash */
  md5_init(&md5);
  md5_append(&md5, stage1, sizeof(stage1));
  md5_finish(&md5, stage2);

  sprintf(hash, "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
          stage2[0], stage2[1], stage2[2], stage2[3], stage2[4], stage2[5],
          stage2[6], stage2[7], stage2[8], stage2[9], stage2[10], stage2[11],
          stage2[12], stage2[13], stage2[14], stage2[15]);

  g_free(pw);

  fs_metadata_set_int(md, FS_MD_SALT, now);
  fs_metadata_set(md, FS_MD_HASH, hash);

  fs_metadata_flush(md);
  fs_metadata_close(md);

  return 0;
}
