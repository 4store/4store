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
 *  Copyright (C) 2006-7 Nick Lamb and Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <glib.h>
#include <rasqal.h>
#include <getopt.h>
#include <sys/time.h>

#ifdef USE_LIBEDIT
#include <editline/readline.h>
#else
#include <readline/readline.h>
#include <readline/history.h>
#endif

#include "query.h"
#include "query-cache.h"
#include "results.h"
#include "query-datatypes.h"
#include "common/4store.h"
#include "common/datatypes.h"
#include "common/params.h"
#include "common/hash.h"
#include "common/error.h"

static void interactive(fsp_link *link, raptor_uri *bu, const char *result_format, int verbosity, int opt_levelo, int result_flags, int soft_limit);
static void programatic_io(fsp_link *link, raptor_uri *bu, const char *query_lang, const char *result_format, fs_query_timing *timing, int verbosity, int opt_level, int result_flags, int soft_limit);

static int show_timing;

static double ftime()
{
    struct timeval now;

    gettimeofday(&now, 0);

    return (double)now.tv_sec + (now.tv_usec * 0.000001);
}

int main(int argc, char *argv[])
{
    char *password = fsp_argv_password(&argc, argv);

    static char *optstring = "hvf:PO:Ib:rs:d";
    char *format = getenv("FORMAT");
    char *kb_name = NULL, *query = NULL;
    int programatic = 0, help = 0;
    int c, opt_index = 0;
    int verbosity = 0;
    int opt_level = 3;
    int insert_mode = 0;
    int restricted = 0;
    int soft_limit = 0;
    int default_graph = 0;
    char *base_uri = "local:";

    static struct option long_options[] = {
        { "help", 0, 0, 'h' },
        { "verbose", 0, 0, 'v' },
        { "format", 1, 0, 'f' },
        { "programatic", 0, 0, 'P' },
        { "opt-level", 1, 0, 'O' },
        { "insert", 0, 0, 'I' },
        { "restricted", 0, 0, 'r' },
        { "soft-limit", 1, 0, 's' },
        { "default-graph", 0, 0, 'd' },
        { "base", 1, 0, 'b' },
        { 0, 0, 0, 0 }
    };

    while ((c = getopt_long (argc, argv, optstring, long_options, &opt_index)) != -1) {
        if (c == 'h') {
            help = 1;
        } else if (c == 'f') {
            format = optarg;
        } else if (c == 'P') {
            programatic = TRUE;
        } else if (c == 'v') {
            verbosity++;
        } else if (c == 'O') {
            opt_level = atoi(optarg);
        } else if (c == 'I') {
            insert_mode = 1;
        } else if (c == 'r') {
            restricted = 1;
        } else if (c == 's') {
            soft_limit = atoi(optarg);
        } else if (c == 'd') {
            default_graph = 1;
        } else if (c == 'b') {
            base_uri = optarg;
        } else {
            help = 1;
        }
    }

    for (int k = optind; k < argc; ++k) {
        if (!kb_name) {
            kb_name = argv[k];
        } else if (!query && !programatic) {
            query = argv[k];
        } else {
            help = 1;
        }
    }

    if (help || !kb_name) {
      char *langs = "";
      if (fq_query_have_laqrs()) {
        langs = "/LAQRS";
      }
      fprintf(stderr, "%s revision %s\n", argv[0], FS_FRONTEND_VER);
      fprintf(stderr, "Usage: %s <kbname> [-f format] [-O opt-level] [-I] [-b uri] [query]\n", argv[0]);
      fprintf(stderr, "   or: %s <kbname> -P\n", argv[0]);
      fprintf(stderr, " query is a SPARQL%s query, remember to use"
                      " shell quoting if necessary\n", langs);
      fprintf(stderr, " -f              Output format one of, sparql, text, json, or testcase\n");
      fprintf(stderr, " -O, --opt-level Set optimisation level, range 0-3\n");
      fprintf(stderr, " -I, --insert    Interpret CONSTRUCT statements as inserts\n");
      fprintf(stderr, " -r, --restricted  Enable query complexity restriction\n");
      fprintf(stderr, " -s, --soft-limit  Override default soft limit on search breadth\n");
      fprintf(stderr, " -d, --default-graph  Enable SPARQL default graph support\n");
      fprintf(stderr, " -b, --base      Set base URI for query\n");
      return 1;
    }

    if (programatic || query) {
        /* don't syslog interactive errors */
        fsp_syslog_enable();
    }

    double then = ftime();
    /* if query does UPDATE or DELETE operations this needs a re-think */
    fsp_link *link = fsp_open_link(kb_name, password, FS_OPEN_HINT_RO);

    double now = ftime();
    show_timing = (getenv("SHOW_TIMING") != NULL) | (verbosity > 1);

    if (show_timing) {
        printf("Link open time: %f\n", now-then);
    }

    if (!link) {
      fs_error(LOG_ERR, "couldn't connect to “%s”", kb_name);
      return 2;
    }

    const int segments = fsp_link_segments(link);

    fs_query_timing timing[segments];
    if (show_timing) {
	for (int seg = 0; seg < segments; seg++) {
	    fsp_get_query_times(link, seg, timing+seg);
	}
    }

    if (fsp_no_op(link, 0)) {
      fs_error(LOG_ERR, "NO-OP failed for “%s”", kb_name);
      return 2;
    }

    raptor_init();
#ifndef HAVE_RASQAL_WORLD
    rasqal_init();
#endif /* ! HAVE_RASQAL_WORLD */
    fs_hash_init(fsp_hash_type(link));

    raptor_uri *bu = raptor_new_uri((unsigned char *)base_uri);

    int flags = 0;
    flags |= insert_mode ? FS_RESULT_FLAG_CONSTRUCT_AS_INSERT : 0;
    flags |= restricted ? FS_QUERY_RESTRICTED : 0;
    flags |= default_graph ? FS_QUERY_DEFAULT_GRAPH : 0;

    if (programatic) {
	programatic_io(link, bu, "sparql", format, timing, verbosity, opt_level,
            FS_RESULT_FLAG_HEADERS | flags, soft_limit);
    } else if (!query) {
        if (!format) format = "text";
        interactive(link, bu, format, verbosity, opt_level,
            insert_mode ? FS_RESULT_FLAG_CONSTRUCT_AS_INSERT : flags, soft_limit);
    }

    int ret = 0;

    fs_query_state *qs = fs_query_init(link);
    fs_query *qr = fs_query_execute(qs, link, bu, query, flags, opt_level, soft_limit);
    if (fs_query_errors(qr)) {
        ret = 1;
    }
    fs_query_results_output(qr, format, 0, stdout);
    fs_query_free(qr);

    if (show_timing) {
	printf("seg bind\t(secs)\t\tprice\t(secs)\t\tresolve\t(secs)\t\twait (secs)\n");
	long long *tics = fsp_profile_write(link);

	for (int seg = 0; seg < segments; seg++) {
	    fs_query_timing newtimes;
	    fsp_get_query_times(link, seg, &newtimes);

	    printf("%2d: %4d\t%f\t%4d\t%f\t%4d\t%f\t%f\n", seg,
		   newtimes.bind_count - timing[seg].bind_count,
		   newtimes.bind - timing[seg].bind,
		   newtimes.price_count - timing[seg].price_count,
		   newtimes.price - timing[seg].price,
		   newtimes.resolve_count - timing[seg].resolve_count,
		   newtimes.resolve - timing[seg].resolve,
		   tics[seg] * 0.001);
	}
    }

    raptor_free_uri(bu);
    raptor_finish();
#ifndef HAVE_RASQAL_WORLD
    rasqal_finish();
#endif /* ! HAVE_RASQAL_WORLD */

    fs_query_cache_flush(qs, verbosity);
    fs_query_fini(qs);

    fsp_close_link(link);

    return ret;
}

#define MAX_Q_SIZE 1000000

static void programatic_io(fsp_link *link, raptor_uri *bu, const char *query_lang, const char *result_format, fs_query_timing *timing, int verbosity, int opt_level, int result_flags, int soft_limit)
{
    char query[MAX_Q_SIZE];
    char *pos;
    char *newl;

    const int segments = fsp_link_segments(link);
    fs_query_state *qs = fs_query_init(link);

    do {
	pos = query;
	*query = '\0';
	/* assemble query string */
	do {
	    newl = fgets(pos, query + MAX_Q_SIZE - pos - 1, stdin);
	    if (newl) {
		    pos += strlen(newl);
	    }
	} while (newl && strcmp(newl, "#EOQ\n") && strcmp(newl, "#END\n"));

	/* process query string */
	if (*query && strcmp(query, "#EOQ\n") && strcmp(query, "#END\n")) {
            if (show_timing) {
                printf("Q: %s\n", query);
            }
	    fs_query *tq = fs_query_execute(qs, link, bu, query,
		    result_flags, opt_level, soft_limit);
	    fs_query_results_output(tq, result_format, 0, stdout);
            if (show_timing) {
                printf("# time: %f s\n", fs_time() - fs_query_start_time(tq));
                printf("seg bind\t(secs)\t\tprice\t(secs)\t\tresolve\t(secs)\t\twait (secs)\n");
                long long *tics = fsp_profile_write(link);
                fs_query_timing total_time = {0, 0, 0, 0, 0, 0};

                for (int seg = 0; seg < segments; seg++) {
                    fs_query_timing newtimes;
                    fsp_get_query_times(link, seg, &newtimes);

                    printf("%2d: %4d\t%f\t%4d\t%f\t%4d\t%f\t%f\n", seg,
                           newtimes.bind_count - timing[seg].bind_count,
                           newtimes.bind - timing[seg].bind,
                           newtimes.price_count - timing[seg].price_count,
                           newtimes.price - timing[seg].price,
                           newtimes.resolve_count - timing[seg].resolve_count,
                           newtimes.resolve - timing[seg].resolve,
                           tics[seg] * 0.001);

                    total_time.bind_count += newtimes.bind_count - timing[seg].bind_count;
                    total_time.bind += newtimes.bind- timing[seg].bind;
                    total_time.price_count += newtimes.price_count - timing[seg].price_count;
                    total_time.price += newtimes.price - timing[seg].price;
                    total_time.resolve_count += newtimes.resolve_count - timing[seg].resolve_count;
                    total_time.resolve += newtimes.resolve - timing[seg].resolve;
                }
                printf("TT: %4d\t%f\t%4d\t%f\t%4d\t%f\n", 
                       total_time.bind_count, total_time.bind,
                       total_time.price_count, total_time.price,
                       total_time.resolve_count, total_time.resolve);
            }
	    fs_query_free(tq);
	    if (result_format && !strcmp(result_format, "sparql")) {
                printf("<!-- EOR -->\n");
	    } else {
                printf("#EOR\n");
	    }
	    fflush(stdout);
	}
    } while (newl && strcmp(newl, "#END\n"));

    raptor_free_uri(bu);
    fsp_close_link(link);
    raptor_finish();
#ifndef HAVE_RASQAL_WORLD
    rasqal_finish();
#endif /* ! HAVE_RASQAL_WORLD */

    fs_query_cache_flush(qs, verbosity);
    fs_query_fini(qs);

    exit(0);
}


static char **resource_completion (const char *text, int start, int end)
{
    char **matches = NULL;

    /* TODO figure out the right way to plumb this into the resource cache */
    return (matches);
}

static void load_history_dotfile(void)
{
    char *dotfile = g_strconcat(g_get_home_dir(), "/.fsp_history", NULL);
    read_history(dotfile);
    g_free(dotfile);
}

static void save_history_dotfile(void)
{
    char *dotfile = g_strconcat(g_get_home_dir(), "/.fsp_history", NULL);
    stifle_history(100); /* arbitrarily restrict history file to 100 entries */
    write_history(dotfile);
    g_free(dotfile);
}

static void interactive(fsp_link *link, raptor_uri *bu, const char *result_format, int verbosity, int opt_level, int result_flags, int soft_limit)
{
    char *query = NULL;

    /* fill out readline functions */
    load_history_dotfile();
    rl_attempted_completion_function = resource_completion;

    fs_query_state *qs = fs_query_init(link);

    do {
	/* assemble query string */
        char *line = readline("4store>");
        if (!line) break; /* EOF */

        g_free(query);
        query = g_strdup(line);

        if (*line == '\0') {
            free(line);
            continue;
        }

        while (line && !g_str_has_suffix(line, "#EOQ")) {
            free(line);
            line = readline("   >");
	    if (line) {
                    char *old = query;
                    query = g_strjoin("\n", old, line, NULL);
		    g_free(old);
	    }
	}
        free(line);
        add_history(query);
        char *old = query;
        query = g_strconcat(old, "\n", NULL);
        g_free(old);

	/* process query string */
        double then = 0.0;
	if (query && strcmp(query, "#EOQ")) {
            if (show_timing) {
                then = fs_time();
            }
	    fs_query *tq = fs_query_execute(qs, link, bu, query,
		    result_flags, opt_level, soft_limit);
	    fs_query_results_output(tq, result_format, 0, stdout);
	    fs_query_free(tq);
	    if (result_format && !strcmp(result_format, "sparql")) {
                if (show_timing) {
                    double now = fs_time();
                    printf("<!-- EOR execution time %.3fs -->\n", now-then);
                } else {
                    printf("<!-- EOR -->\n");
                }
	    } else {
                printf("#EOR\n");
                if (show_timing) {
                    double now = fs_time();
                    printf("# execution time %.3fs\n", now-then);
                }
	    }
	    fflush(stdout);
	}
    } while (query);

    raptor_free_uri(bu);
    fsp_close_link(link);
    raptor_finish();
#ifndef HAVE_RASQAL_WORLD
    rasqal_finish();
#endif /* ! HAVE_RASQAL_WORLD */

    save_history_dotfile();

    fs_query_cache_flush(qs, verbosity);
    fs_query_fini(qs);

    exit(0);
}

/* vi:set expandtab sts=4 sw=4: */
