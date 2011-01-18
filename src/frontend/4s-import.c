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
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <raptor.h>
#include <getopt.h>
#include <glib.h>
#include <fcntl.h>
#include <errno.h>

#include "import.h"
#include "../common/datatypes.h"
#include "../common/error.h"
#include "../common/hash.h"
#include "../common/params.h"

#include "../common/4store.h"

static fsp_link *fsplink; /* link to remote server(s) */

struct timeval then, then_last, now;

int main(int argc, char *argv[])
{
    int verbosity = 0;
    int dryrun = 0;
    char *password = NULL;
    char *format = "auto";
    FILE *msg = stderr;
    char *optstring = "am:M:vnf:";
    int c, opt_index = 0, help = 0;
    int files = 0, adding = 0;
    char *kb_name = NULL;
    char *model[argc], *uri[argc];
    char *model_default = NULL;

    password = fsp_argv_password(&argc, argv);

    static struct option long_options[] = {
        { "add", 0, 0, 'a' },
        { "model", 1, 0, 'm' },
        { "model-default", 1, 0, 'M' },
        { "verbose", 0, 0, 'v' },
        { "dryrun", 0, 0, 'n' },
        { "no-resources", 0, 0, 'R' },
        { "no-quads", 0, 0, 'Q' },
        { "format", 1, 0, 'f' },
        { "help", 0, 0, 'h' },
        { "version", 0, 0, 'V' },
        { 0, 0, 0, 0 }
    };

    for (int i= 0; i < argc; ++i) {
      model[i] = NULL;
    }

    int help_return = 1;

    while ((c = getopt_long (argc, argv, optstring, long_options, &opt_index)) != -1) {
        if (c == 'm') {
	    model[files++] = optarg;
        } else if (c == 'M') {
            model_default = optarg;
        } else if (c == 'v') {
	    verbosity++;
        } else if (c == 'a') {
	    adding = 1;
        } else if (c == 'n') {
	    dryrun |= FS_DRYRUN_DELETE | FS_DRYRUN_RESOURCES | FS_DRYRUN_QUADS;
	} else if (c == 'R') {
	    dryrun |= FS_DRYRUN_RESOURCES;
	} else if (c == 'Q') {
	    dryrun |= FS_DRYRUN_QUADS;
        } else if (c == 'f') {
            format = optarg;
        } else if (c == 'h') {
	    help = 1;
            help_return = 0;
        } else if (c == 'V') {
            printf("%s, built for 4store %s\n", argv[0], GIT_REV);
            exit(0);
        } else {
	    help = 1;
        }
    }

    if (verbosity > 0) {
	if (dryrun & FS_DRYRUN_DELETE) {
	    printf("warning: not deleting old model\n");
	}
	if (dryrun & FS_DRYRUN_RESOURCES) {
	    printf("warning: not importing resource nodes\n");
	}
	if (dryrun & FS_DRYRUN_QUADS) {
	    printf("warning: not importing quad graph\n");
	}
    }

    files = 0;
    for (int k = optind; k < argc; ++k) {
        if (!kb_name) {
            kb_name = argv[k];
        } else {
	    if (strchr(argv[k], ':')) {
		uri[files] = g_strdup(argv[k]);
	    } else {
		uri[files] = (char *)raptor_uri_filename_to_uri_string(argv[k]);
	    }
            if (!model[files]) {
                if (!model_default) {
                    model[files] = uri[files];
                } else {
                    model[files] = model_default;
                }
            }
            files++;
        }
    }

    raptor_world *rw = raptor_new_world();
    if (help || !kb_name || files == 0) {
        fprintf(stdout, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
        fprintf(stdout, "Usage: %s <kbname> <rdf file/URI> ...\n", argv[0]);
        fprintf(stdout, " -v --verbose   increase verbosity (can repeat)\n");
        fprintf(stdout, " -a --add       add data to models instead of replacing\n");
        fprintf(stdout, " -m --model     specify a model URI for the next RDF file\n");
        fprintf(stdout, " -M --model-default specify a model URI for all RDF files\n");
        fprintf(stdout, " -f --format    specify an RDF syntax for the import\n");
        fprintf(stdout, "\n   available formats are:\n");

        for (unsigned int i=0; 1; i++) {
            const raptor_syntax_description *desc =
                    raptor_world_get_parser_description(rw, i);
            if (!desc) {
                break;
            }
            fprintf(stdout, "    %12s - %s\n", desc->names[0], desc->label);
        }
        exit(help_return);
    }

    fsp_syslog_enable();

    fsplink = fsp_open_link(kb_name, password, FS_OPEN_HINT_RW);

    if (!fsplink) {
      fs_error (LOG_ERR, "couldn't connect to “%s”", kb_name);
      exit(2);
    }

    const char *features = fsp_link_features(fsplink);
    int has_o_index = !(strstr(features, "no-o-index")); /* tweak */

    fs_hash_init(fsp_hash_type(fsplink));
    const int segments = fsp_link_segments(fsplink);
    int total_triples = 0;

    fs_import_timing timing[segments];

    for (int seg = 0; seg < segments; seg++) {
        fsp_get_import_times(fsplink, seg, &timing[seg]);
    }

    gettimeofday(&then, 0);

    if (fsp_start_import_all(fsplink)) {
	fs_error(LOG_ERR, "aborting import");

	exit(3);
    }

#if 0
printf("press enter\n");
char foo;
read(0, &foo, 1);
#endif

    fs_rid_vector *mvec = fs_rid_vector_new(0);

    for (int f= 0; f < files; ++f) {
        fs_rid muri = fs_hash_uri(model[f]);
        fs_rid_vector_append(mvec, muri);
    }
    if (!adding) {
        if (verbosity) {
	    printf("removing old data\n");
	    fflush(stdout);
        }
        if (!(dryrun & FS_DRYRUN_DELETE)) {
	    if (fsp_delete_model_all(fsplink, mvec)) {
	        fs_error(LOG_ERR, "model delete failed");
	        return 1;
	    }
	    for (int i=0; i<mvec->length; i++) {
		if (mvec->data[i] == fs_c.system_config) {
		    fs_import_reread_config();
		}
	    }
        }
        fsp_new_model_all(fsplink, mvec);
    }

    fs_rid_vector_free(mvec);

    gettimeofday(&then_last, 0);
    for (int f = 0; f < files; ++f) {
	if (verbosity) {
            printf("Reading <%s>\n", uri[f]);
            if (strcmp(uri[f], model[f])) {
                printf("   into <%s>\n", model[f]);
            }
	    fflush(stdout);
        }

        fs_import(fsplink, model[f], uri[f], format, verbosity, dryrun, has_o_index, msg, &total_triples);
	if (verbosity) {
	    fflush(stdout);
        }
    }
    double sthen = fs_time();
    int ret = fs_import_commit(fsplink, verbosity, dryrun, has_o_index, msg, &total_triples);

    if (verbosity > 0) {
	printf("Updating index\n");
        fflush(stdout);
    }
    fsp_stop_import_all(fsplink);
    if (verbosity > 0) {
        printf("Index update took %f seconds\n", fs_time()-sthen);
    }

    if (!ret) {
        gettimeofday(&now, 0);
        double diff = (now.tv_sec - then.tv_sec) +
                        (now.tv_usec - then.tv_usec) * 0.000001;
        if (verbosity && total_triples > 0) {
	    printf("Imported %d triples, average %d triples/s\n", total_triples,
		     (int)((double)total_triples/diff));
            fflush(stdout);
        }
    }

    if (verbosity > 1) {
        printf("seg add_q\tadd_r\t\tcommit_q\tcommit_r\tremove\t\trebuild\t\twrite\n");
        long long *tics = fsp_profile_write(fsplink);

        for (int seg = 0; seg < segments; seg++) {
            fs_import_timing newtimes;
            fsp_get_import_times(fsplink, seg, &newtimes);

	    printf("%2d: %f\t%f\t%f\t%f\t%f\t%f\t%f\n", seg,
                   newtimes.add_s - timing[seg].add_s,
	           newtimes.add_r - timing[seg].add_r,
	           newtimes.commit_q - timing[seg].commit_q,
                   newtimes.commit_r - timing[seg].commit_r,
                   newtimes.remove - timing[seg].remove,
		   newtimes.rebuild - timing[seg].rebuild,
		   tics[seg] * 0.001);
	}
    }

    fsp_close_link(fsplink);
    raptor_free_world(rw);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
