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
 *  Copyright (C) 2006 Steve Harris for Garlik.com
 *  Copyright 2006-7 Nick Lamb for Garlik.com
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <locale.h>
#include <libgen.h>
#include <ctype.h>
#include <errno.h>
#include <glib.h>
#include <unistd.h>
#include <time.h>
#include <libgen.h>
#include <sys/param.h>
#include <sys/stat.h>

#include "../common/md5.h"
#include "../common/4store.h"
#include "../common/error.h"
#include "../common/params.h"
#include "../common/4s-store-root.h"

#include "../backend/metadata.h"

static int verbosity = 0;

int main(int argc, char *argv[])
{
    char *optstring = "vP:";
    int help = 0;
    int c, opt_index = 0;
    char *fromkb = NULL, *tokb = NULL;
    char *password = NULL;

    static struct option long_options[] = {
        { "version", 0, 0, 'V' },
        { "help", 0, 0, 'h' },
        { "verbose", 0, 0, 'v' },
        { "password", 1, 0, 'P' },
        { 0, 0, 0, 0 }
    };

    setlocale(LC_ALL, NULL);
    int help_return = 1;

    while ((c = getopt_long (argc, argv, optstring, long_options, &opt_index)) != -1) {
	if (c == 'v') {
	    verbosity++;
	} else if (c == 'V') {
	    printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
	    exit(0);
	} else if (c == 'h') {
	    help = 1;
	    help_return = 0;
	} else if (c == 'P') {
	    password = optarg;
	} else {
	    help++;
	}
    }

    if (optind == argc - 2) {
	fromkb = argv[optind];
	tokb = argv[optind+1];
    } else {
        help = 1;
    }

    if (help) {
	printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
        fprintf(stdout, "Usage: %s [-v] [--password <pw>] <from> <to>\n", basename(argv[0]));
        fprintf(stdout, "   -v, --verbose    increase verbosity\n");
        fprintf(stdout, "   -P, --password   set password for new KB\n");
        fprintf(stdout, "This command copies the contents of an existing KB to a new KB\n");
        fprintf(stdout, "NB. The password is not copied, specify a new password if one is needed\n");
        return help_return;
    }

    fsp_syslog_enable();

    const int len = strlen(tokb);
    for (int k= 0; k < len; ++k) {
      if (!index(FS_LEGAL_KB_CHARS, tokb[k])) {
        fs_error(LOG_ERR, "character '%c' forbidden in a KB name", tokb[k]);
        return 2;
      }
    }

    char lexf[PATH_MAX + 1];
    /* this has no trailing slash, unlike fs_get_kb_dir_format() */
    gchar *lexf_format = g_strconcat(fs_get_store_root(), "/%s", NULL);
    lexf[PATH_MAX] = '\0';
    snprintf(lexf, PATH_MAX, lexf_format, tokb);
    g_free(lexf_format);

    int result = mkdir(lexf, 0777);
    if (result) {
      if (errno == EEXIST) {
        fs_error(LOG_ERR, "there is already a KB named “%s”", tokb);
      } else {
        fs_error(LOG_ERR, "problem while creating new KB “%s”: %s", tokb, strerror(errno));
      }
      return 3;
    }

    lexf_format = g_strconcat("cp -a ",
			      fs_get_kb_dir_format(),
			      "* ",
			      fs_get_store_root(),
			      "/%s",
			      NULL);
    snprintf(lexf, PATH_MAX, lexf_format, fromkb, tokb);
    if (verbosity) printf("executing %s\n", lexf);
    if (system(lexf)) {
      fs_error(LOG_ERR, "problem while copying data files from “%s” to “%s”", fromkb, tokb);
      g_free(lexf_format);
      return 4;
    } else {
	g_free(lexf_format);
    }

    /* fix up */

    fs_metadata *meta = fs_metadata_open(tokb);
    fs_metadata_set(meta, FS_MD_NAME, tokb);

    /* password */

    if (password) {
        md5_state_t md5;
        unsigned char stage1[20], stage2[16];
        char hash[33] = "none";
        int now = 0;
        char *pw = g_strdup_printf("%s:%s", tokb, password);

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

        fs_metadata_set(meta, FS_MD_HASH, hash);
        fs_metadata_set_int(meta, FS_MD_SALT, now);
        g_free(pw);

    } else {
        fs_metadata_set(meta, FS_MD_HASH, "none");
        fs_metadata_set_int(meta, FS_MD_SALT, 0);
    }

    fs_metadata_flush(meta);
    fs_metadata_close(meta);

    fs_error(LOG_INFO, "KB %s created", tokb);

    return 0;
}

/* vi:set ts=8 sts=4 sw=4: */
