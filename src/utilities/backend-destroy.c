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
 *  Copyright 2006 Nick Lamb for Garlik.com
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <getopt.h>
#include <locale.h>
#include <libgen.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <sys/param.h>

#include "../common/4store.h"
#include "../common/error.h"
#include "../common/params.h"

static int dummy = 0;

int delete_it(char *filename, int *err_count)
{
    if (dummy) {
	return 0;
    } else if (remove(filename)) {
        if (errno != ENOENT) {
            fs_error(LOG_ERR, "could not remove '%s': %s", filename, strerror(errno));
            (*err_count)++;
        }
    } else {
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[])
{
    char *optstring = "n";
    int help = 0;
    int c, opt_index = 0;
    char *name = NULL;

    static struct option long_options[] = {
        { "version", 0, 0, 'V' },
        { "help", 0, 0, 'h' },
        { "print-only", 0, 0, 'n' },
        { 0, 0, 0, 0 }
    };

    setlocale(LC_ALL, NULL);
    int help_return = 1;

    while ((c = getopt_long (argc, argv, optstring, long_options, &opt_index)) != -1) {
	if (c == 'n') {
	    dummy = 1;
	} else if (c == 'V') {
            printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
	    exit(0);
	} else if (c == 'h') {
            help = 1;
	    help_return = 0;
	} else {
	    help++;
	}
    }

    if (optind == argc - 1) {
	name = argv[optind];
    }

    if (name == NULL) {
        help = 1;
    }

    if (help) {
	printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
        fprintf(stdout, "Usage: %s [-n] <kbname>\n", basename(argv[0]));
        fprintf(stdout, "   -n, --print-only  don't execute just show what would be done\n");
        fprintf(stdout, "This command destroys the KB and all its data.\n");
        return help_return;
    }

    fsp_syslog_enable();

    const int len = strlen(name);
    for (int k= 0; k < len; ++k) {
      if (!index(FS_LEGAL_KB_CHARS, name[k])) {
        fs_error(LOG_ERR, "character '%c' forbidden in a KB name", name[k]);
        return 2;
      }
    }

    char lexf[PATH_MAX + 1];
    lexf[PATH_MAX] = '\0';
    int unlinked = 0, errs = 0;
    for (fs_segment segment = 0; segment < FS_MAX_SEGMENTS; ++segment)  {
        snprintf(lexf, PATH_MAX, "rm -rf " FS_SEG_DIR "*", name, segment);
        system(lexf);
        snprintf(lexf, PATH_MAX, FS_SEG_DIR, name, segment);
        unlinked += delete_it(lexf, &errs);
    }
    snprintf(lexf, PATH_MAX, FS_KB_DIR "metadata.nt", name);
    unlinked += delete_it(lexf, &errs);
    snprintf(lexf, PATH_MAX, FS_KB_DIR "backup", name);
    unlinked += delete_it(lexf, &errs);
    snprintf(lexf, PATH_MAX, FS_KB_DIR, name);
    unlinked += delete_it(lexf, &errs);

    if (unlinked > 0 && !errs) fs_error(LOG_INFO, "deleted data files for KB %s", name);

    return 0;
}

/* vi:set ts=8 sts=4 sw=4: */
