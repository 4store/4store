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
#include <ctype.h>
#include <sys/types.h>
#include <sys/time.h>
#include <raptor.h>
#include <glib.h>
#include <fcntl.h>
#include <errno.h>

#include "import.h"
#include "common/datatypes.h"
#include "common/error.h"
#include "common/hash.h"
#include "common/params.h"

#include "common/4store.h"

#define RES_BUF_SIZE 256
#define QUAD_BUF_SIZE 10000
#define FS_CHUNK_SIZE 5000000

#define MEMBER_PREFIX "http://www.w3.org/1999/02/22-rdf-syntax-ns#_"

typedef struct {
    fsp_link *link;
    int verbosity;
    int dryrun;
    int count_err;
    int count_warn;
    int count_trip;
    int last_count;
    int *ext_count;
    int tax_rebuild;
    char *model;
    fs_rid model_hash;
    int quad_fd;
    char *quad_fn;
    int segments;
    int has_o_index;
    raptor_uri muri;
    raptor_parser *parser;
} fs_parse_stuff;

static long res_pos[FS_MAX_SEGMENTS];

static fs_resource res_buffer[FS_MAX_SEGMENTS][RES_BUF_SIZE];
static char *lex_tmp[FS_MAX_SEGMENTS][RES_BUF_SIZE];

static fs_rid quad_buf[QUAD_BUF_SIZE][4];
static fs_rid quad_buf_s[QUAD_BUF_SIZE][4];

static void store_stmt(void *user_data,
		       const raptor_statement * statement);

static int process_quads(fs_parse_stuff *data);

#define CACHE_SIZE 32768
#define CACHE_MASK (CACHE_SIZE-1)
static fs_rid nodecache[CACHE_SIZE];

static int buffer_res(fsp_link *link, const int segments, fs_rid r, char *lex, fs_rid attr, int dryrun) {
    int seg = FS_RID_SEGMENT(r, segments);

    if (FS_IS_BNODE(r)) {
	return 1;
    }
    if (nodecache[r & CACHE_MASK] == r) {
	return 1;
    }
    if (!lex) {
        return 1;
    }
    nodecache[r & CACHE_MASK] = r;
    res_buffer[seg][res_pos[seg]].rid = r;
    res_buffer[seg][res_pos[seg]].attr = attr;
    if (strlen(lex) < RES_BUF_SIZE) {
	strcpy(lex_tmp[seg][res_pos[seg]], lex);
	res_buffer[seg][res_pos[seg]].lex = lex_tmp[seg][res_pos[seg]];
    } else {
	res_buffer[seg][res_pos[seg]].lex = g_strdup(lex);
    }
    if (++res_pos[seg] == RES_BUF_SIZE) {
	if (!(dryrun & FS_DRYRUN_RESOURCES) &&
                fsp_res_import(link, seg, res_pos[seg], res_buffer[seg])) {
	    fs_error(LOG_ERR, "resource import failed");
	    return 1;
	}
	for (int i=0; i<res_pos[seg]; i++) {
	    if (res_buffer[seg][i].lex != lex_tmp[seg][i]) {
		free(res_buffer[seg][i].lex);
		res_buffer[seg][i].lex = NULL;
	    }
	}
	res_pos[seg] = 0;
    }

    return 0;
}

int total_triples_parsed = 0;
static struct timeval then_last;

static int inited = 0;

static fs_parse_stuff parse_data;

void rdf_parser_warning(void *user_data, raptor_locator* locator, const char *message)
{
    parse_data.count_warn++;
    fs_error(LOG_ERR, "raptor parser warning: %s line %d", message,
             raptor_locator_line(locator));
}

void rdf_parser_error(void *user_data, raptor_locator* locator, const char *message)
{
    parse_data.count_err++;
    fs_error(LOG_ERR, "raptor parser error: %s line %d", message,
             raptor_locator_line(locator));
}

void fatal_rdf_parser_error(void *user_data, raptor_locator* locator, const char *message)
{
    parse_data.count_err++;
    fs_error(LOG_ERR, "fatal raptor parser error: %s line %d", message,
             raptor_locator_line(locator));
}

void graph_handler(void *user_data, raptor_uri *graph)
{
    g_free(parse_data.model);
    if (graph == NULL) {
        parse_data.model = g_strdup((char *) raptor_uri_as_string(parse_data.muri));
    } else {
        parse_data.model = g_strdup((char *) raptor_uri_as_string(graph));
    }

    parse_data.model_hash = fs_hash_uri(parse_data.model);
    buffer_res(parse_data.link, parse_data.segments, parse_data.model_hash, parse_data.model, FS_RID_NULL, parse_data.dryrun);
}

/* ..._start and ..._finish share an int * count parameter
 * the same variable should be passed by reference both times */
int fs_import_stream_start(fsp_link *link, const char *model_uri, const char *mimetype, int has_o_index, int *count)
{
    if (inited == 0) {
        memset(&parse_data, 0, sizeof(parse_data));
        inited = 1;
    }

    parse_data.link = link;
    parse_data.segments = fsp_link_segments(link);
    parse_data.ext_count = count;

    for (int i=0; i<parse_data.segments; i++) {
        for (int j=0; j<RES_BUF_SIZE; j++) {
            lex_tmp[i][j] = malloc(RES_BUF_SIZE);
        }
    }

    memset(nodecache, 0, sizeof(nodecache));

    parse_data.quad_fn = g_strdup(FS_TMP_PATH "/importXXXXXX");
    parse_data.quad_fd = mkstemp(parse_data.quad_fn);
    if (parse_data.quad_fd < 0) {
        fs_error(LOG_ERR, "Cannot create tmp file “%s”", parse_data.quad_fn);
        return 1;
    }

    parse_data.muri = raptor_new_uri((unsigned char *) model_uri);

    parse_data.model = g_strdup(model_uri);
    parse_data.model_hash = fs_hash_uri(model_uri);
    parse_data.count_trip = 0;
    parse_data.count_warn = 0;
    parse_data.count_err = 0;
    parse_data.last_count = 0;
    parse_data.has_o_index = has_o_index;

    /* store the model uri */
    buffer_res(link, parse_data.segments, parse_data.model_hash, parse_data.model, FS_RID_NULL, parse_data.dryrun);

    parse_data.parser = raptor_new_parser_for_content(NULL, mimetype, NULL, 0, (unsigned char *) parse_data.model);

    if (!parse_data.parser) {
        return 1;
    }

    /* use us as a vector for an indirect attack? no thanks */
    raptor_set_feature(parse_data.parser, RAPTOR_FEATURE_NO_NET, 0);

    raptor_set_warning_handler(parse_data.parser, link, rdf_parser_warning);
    raptor_set_error_handler(parse_data.parser, link, rdf_parser_error);
    raptor_set_fatal_error_handler(parse_data.parser, link, fatal_rdf_parser_error);

    raptor_set_statement_handler(parse_data.parser, &parse_data, store_stmt);
    raptor_set_graph_handler(parse_data.parser, &parse_data, graph_handler);

    raptor_start_parse(parse_data.parser, parse_data.muri);

    fs_hash_freshen(); /* blank nodes are unique per file */

    return 0;
}

int fs_import_stream_data(fsp_link *link, unsigned char *data, size_t count)
{
    return raptor_parse_chunk(parse_data.parser, data, count, 0);
}

int fs_import_stream_finish(fsp_link *link, int *count, int *errors)
{
    raptor_parse_chunk(parse_data.parser, NULL, 0, 1); /* finish */
    raptor_free_parser(parse_data.parser);
    raptor_free_uri(parse_data.muri);
    g_free(parse_data.model);

    const int segments = fsp_link_segments(link);

    *count += process_quads(&parse_data);

    close(parse_data.quad_fd);
    unlink(parse_data.quad_fn);
    free(parse_data.quad_fn);
    parse_data.quad_fd = 0xdeadbeef;
    parse_data.quad_fn = NULL;

    /* make sure buffers are flushed */
    for (int seg = 0; seg < segments; seg++) {
        if (res_pos[seg] > 0 && fsp_res_import(link, seg, res_pos[seg], res_buffer[seg])) {
	    fs_error(LOG_ERR, "resource import failed");
            return 1;
        }
    }
    if (fsp_res_import_commit_all(link)) {
        fs_error(LOG_ERR, "resource commit failed");
        return 2;
    }
    for (int seg = 0; seg < segments; seg++) {
        for (int i=0; i<res_pos[seg]; i++) {
            if (res_buffer[seg][i].lex != lex_tmp[seg][i]) {
                free(res_buffer[seg][i].lex);
            }
        }
        res_pos[seg] = 0;
    }
    if (fsp_quad_import_commit_all(link, FS_BIND_BY_SUBJECT)) {
        fs_error(LOG_ERR, "quad commit failed");
        return 3;
    }

    for (int i=0; i<segments; i++) {
        for (int j=0; j<RES_BUF_SIZE; j++) {
            free(lex_tmp[i][j]);
            lex_tmp[i][j] = NULL;
        }
    }

    *errors = parse_data.count_err;

    return 0;
}


int fs_import(fsp_link *link, const char *model_uri, char *resource_uri,
	      const char *format, int verbosity, int dryrun, int has_o_index,
              FILE *msg, int *count)
{
    raptor_parser *rdf_parser = NULL;
    raptor_uri ruri = NULL;
    int ret = 0;

    const int segments = fsp_link_segments(link);

    parse_data.ext_count = count;
    if (!inited) {
        inited = 1;
        parse_data.link = link;
        parse_data.segments = fsp_link_segments(link);

        for (int i=0; i<parse_data.segments; i++) {
            for (int j=0; j<RES_BUF_SIZE; j++) {
                lex_tmp[i][j] = malloc(RES_BUF_SIZE);
            }
        }

        memset(nodecache, 0, sizeof(nodecache));

        parse_data.quad_fn = g_strdup(FS_TMP_PATH "/importXXXXXX");
        parse_data.quad_fd = mkstemp(parse_data.quad_fn);
        if (parse_data.quad_fd < 0) {
            fs_error(LOG_ERR, "Cannot create tmp file “%s”", parse_data.quad_fn);
            return 1;
        }
        gettimeofday(&then_last, 0);
    }

    parse_data.verbosity = verbosity;
    parse_data.model = g_strdup(model_uri);
    parse_data.model_hash = fs_hash_uri(model_uri);
    parse_data.count_trip = 0;
    parse_data.last_count = 0;
    parse_data.dryrun = dryrun;
    parse_data.has_o_index = has_o_index;

    /* store the model uri */
    buffer_res(link, segments, parse_data.model_hash, parse_data.model, FS_RID_NULL, dryrun);

    if (strcmp(format, "auto")) {
       rdf_parser = raptor_new_parser(format);
    } else if (strstr(resource_uri, ".n3") || strstr(resource_uri, ".ttl")) {
        rdf_parser = raptor_new_parser("turtle");
    } else if (strstr(resource_uri, ".nt")) {
        rdf_parser = raptor_new_parser("ntriples");
    } else {
        rdf_parser = raptor_new_parser("rdfxml");
    }
    if (!rdf_parser) {
        fs_error(LOG_ERR, "failed to create RDF parser");
        return 1;
    }

    raptor_set_statement_handler(rdf_parser, &parse_data, store_stmt);
    raptor_set_graph_handler(rdf_parser, &parse_data, graph_handler);
    ruri = raptor_new_uri((unsigned char *) resource_uri);
    parse_data.muri = raptor_new_uri((unsigned char *) model_uri);

    if (raptor_parse_uri(rdf_parser, ruri, parse_data.muri)) {
        fs_error(LOG_ERR, "failed to parse file “%s”", resource_uri);
        ret++;
    }
    if (verbosity) {
        printf("Pass 1, processed %d triples (%d)\n", total_triples_parsed, parse_data.count_trip);
    }

    raptor_free_parser(rdf_parser);
    raptor_free_uri(ruri);
    raptor_free_uri(parse_data.muri);
    g_free(parse_data.model);
    fs_hash_freshen(); /* blank nodes are unique per file */

    return ret;
}

int fs_import_commit(fsp_link *link, int verbosity, int dryrun, int has_o_index, FILE *msg, int *count)
{
    const int segments = fsp_link_segments(link);

    *count += process_quads(&parse_data);

    close(parse_data.quad_fd);
    unlink(parse_data.quad_fn);
    free(parse_data.quad_fn);
    parse_data.quad_fd = 0xdeadbeef;
    parse_data.quad_fn = NULL;

    /* make sure buffers are flushed */
    for (int seg = 0; seg < segments; seg++) {
	if (!(dryrun & FS_DRYRUN_RESOURCES) && res_pos[seg] > 0 && fsp_res_import(link, seg, res_pos[seg], res_buffer[seg])) {
	    fs_error(LOG_ERR, "resource import failed");

	    return 1;
	}
    }
    if (!(dryrun & FS_DRYRUN_RESOURCES) && fsp_res_import_commit_all(link)) {
        fs_error(LOG_ERR, "resource commit failed");
        return 2;
    }
    for (int seg = 0; seg < segments; seg++) {
	for (int i=0; i<res_pos[seg]; i++) {
	    if (res_buffer[seg][i].lex != lex_tmp[seg][i]) {
		free(res_buffer[seg][i].lex);
	    }
	}
	res_pos[seg] = 0;
    }

    if (!(dryrun & FS_DRYRUN_QUADS) && fsp_quad_import_commit_all(link, FS_BIND_BY_SUBJECT)) {
        fs_error(LOG_ERR, "quad commit failed");
        return 3;
    }

    for (int i=0; i<segments; i++) {
	for (int j=0; j<RES_BUF_SIZE; j++) {
	    free(lex_tmp[i][j]);
            lex_tmp[i][j] = NULL;
	}
    }

    inited = 0;

    return 0;
}

static int process_quads(fs_parse_stuff *data)
{
    fsp_link *link = data->link;
    const int segments = data->segments;
    int tfd = data->quad_fd;
    int verbosity = data->verbosity;
    int dryrun = data->dryrun;
    int total = 0;
    int ret;
    struct timeval now;

    if (lseek(tfd, 0, SEEK_SET) == -1) {
        fs_error(LOG_ERR, "error seeking in triple buffer file (fd %d): %s", tfd, strerror(errno));

        return -1;
    }
    do {
	ret = read(tfd, quad_buf, sizeof(quad_buf));
	int count = ret / (sizeof(fs_rid) * 4);
	if (ret < 0) {
	    fs_error(LOG_ERR, "error reading triple buffer file (fd %d): %s", tfd, strerror(errno));
	    return -1;
	}
	if (ret % sizeof(fs_rid) * 4 != 0) {
	    fs_error(LOG_ERR, "bad read size, %d - not multiple of 4 RIDs", ret);
	    return -1;
	}
	total += count;
	for (int seg=0; seg < segments; seg++) {
	    int i=0, scnt = 0;
	    for (i=0; i<count; i++) {
		if (FS_RID_SEGMENT(quad_buf[i][1], segments) == seg) {
		    quad_buf_s[scnt][0] = quad_buf[i][0];
		    quad_buf_s[scnt][1] = quad_buf[i][1];
		    quad_buf_s[scnt][2] = quad_buf[i][2];
		    quad_buf_s[scnt][3] = quad_buf[i][3];
		    scnt++;
		}
            }
	    if (!(dryrun & FS_DRYRUN_QUADS) && scnt > 0 && fsp_quad_import(link, seg, FS_BIND_BY_SUBJECT, scnt, quad_buf_s)) {
		fs_error(LOG_ERR, "quad import failed");

		return 1;
	    }
	}
	if (verbosity) printf("Pass 2, processed %d triples\r", total);
	fflush(stdout);
    } while (ret == sizeof(quad_buf));
    if (verbosity) {
        gettimeofday(&now, 0);
        double diff = (now.tv_sec - then_last.tv_sec) +
			(now.tv_usec - then_last.tv_usec) * 0.000001;
        printf("Pass 2, processed %d triples", total);
        if (total > 0) {
	    printf(", %d triples/s\n", (int)((double)total/diff));
        } else {
	    printf("\n");
        }
    }
    ftruncate(tfd, 0);
    lseek(tfd, 0, SEEK_SET);

    return total;
}

/* inside this code block bNode RIDs are unswizzled */

static fs_rid bnodenext = 1, bnodemax = 0;

static fs_rid fs_bnode_id(fsp_link *link, const char *bnode)
{
    GHashTable *bnids = fs_hash_bnids();
    fs_rid bn = (fs_rid) (unsigned long) g_hash_table_lookup(bnids, bnode);
    if (!bn) {
        if (bnodenext > bnodemax) {
            fsp_bnode_alloc(link, 1000000, &bnodenext, &bnodemax);
            if (sizeof(gpointer) < sizeof(fs_rid) && bnodemax >= 0x100000000LL) {
                    fs_error(LOG_CRIT, "bNode RID %lld exceeds safe size on 32-bit arch", bnodemax);
                abort();
            }
        }
        bn = bnodenext++;
        g_hash_table_insert(bnids, g_strdup(bnode), (gpointer) (unsigned long) bn);
    }
    union {
        fs_rid rid;
        char bytes[8];
    } node;


    /* bNode IDs up to 2 ** 56 or only */
    node.rid = bn;
    node.bytes[7] = (node.bytes[0] & 0x7c) >> 2;
    node.bytes[1] ^= node.bytes[6];
    node.bytes[6] ^= node.bytes[1];
    node.bytes[1] ^= node.bytes[6];
    node.bytes[2] ^= node.bytes[5];
    node.bytes[5] ^= node.bytes[2];
    node.bytes[2] ^= node.bytes[5];
    node.bytes[3] ^= node.bytes[4];
    node.bytes[4] ^= node.bytes[3];
    node.bytes[3] ^= node.bytes[4];

    return node.rid | 0x8000000000000000LL;
}

/* remainder of code uses swizzled bNode RIDs */

static void store_stmt(void *user_data, const raptor_statement * statement)
{
    fs_parse_stuff *data = (fs_parse_stuff *) user_data;
    char *subj = (char *) raptor_uri_as_string((raptor_uri *)
					       statement->subject);
    char *pred;
    char *obj;
    fs_rid m, s, p, o;
    char tmpp[512];

    m = data->model_hash;

    if (statement->subject_type == RAPTOR_IDENTIFIER_TYPE_ANONYMOUS) {
        s = fs_bnode_id(data->link, statement->subject);
        subj = (char *) statement->subject;
    } else {
	s = fs_hash_uri(subj);
    }

    if (statement->predicate_type == RAPTOR_IDENTIFIER_TYPE_ORDINAL) {
        sprintf(tmpp, MEMBER_PREFIX "%d", *((int *)statement->predicate));
        pred = tmpp;
    } else {
        pred = (char *) raptor_uri_as_string((raptor_uri *)
					       statement->predicate);
    }
    p = fs_hash_uri(pred);

    fs_rid attr = fs_c.empty;
    if (statement->object_type == RAPTOR_IDENTIFIER_TYPE_LITERAL ||
	statement->object_type == RAPTOR_IDENTIFIER_TYPE_XML_LITERAL) {
	obj = (char *) statement->object;
	if (statement->object_literal_language) {
	    char *langtag = (char *)statement->object_literal_language;
            for (char *pos = langtag; *pos; pos++) {
                if (islower(*pos)) {
                    *pos = toupper(*pos);
                }
            }
	    attr = fs_hash_literal(langtag, 0);
	    buffer_res(data->link, data->segments, attr, langtag, fs_c.empty, data->dryrun);
	} else if (raptor_uri_as_string(statement->object_literal_datatype)) {
	    char *dt = (char *)raptor_uri_as_string(statement->object_literal_datatype);
	    
	    attr = fs_hash_uri(dt);
	    buffer_res(data->link, data->segments, attr, dt, FS_RID_NULL, data->dryrun);
	}
	o = fs_hash_literal(obj, attr);
    } else if (statement->object_type == RAPTOR_IDENTIFIER_TYPE_ANONYMOUS) {
	o = fs_bnode_id(data->link, statement->object);
	obj = (char *) statement->object;
    } else {
	obj = (char *) raptor_uri_as_string((raptor_uri *) statement->
					    object);
        attr = FS_RID_NULL;
	o = fs_hash_uri(obj);
    }

    buffer_res(data->link, data->segments, s, subj, FS_RID_NULL, data->dryrun);
    buffer_res(data->link, data->segments, p, pred, FS_RID_NULL, data->dryrun);
    buffer_res(data->link, data->segments, o, obj, attr, data->dryrun);

    fs_rid tbuf[4] = { m, s, p, o };
retry_write:
    if (write(data->quad_fd, tbuf, sizeof(tbuf))  == -1) {
        fs_error(LOG_ERR, "failed to buffer quad to fd %d (0x%x): %s", data->quad_fd, data->quad_fd, strerror(errno));
        if (errno == EAGAIN || errno == EINTR || errno == ENOSPC) {
            sleep(5);
            goto retry_write;
        }
    }
    if (data->verbosity > 2) {
        fprintf(stderr, "%016llx %016llx %016llx %016llx\n", m, s, p, o);
    }

    data->count_trip++;
    total_triples_parsed++;
    if (data->verbosity && total_triples_parsed % 10000 == 0) {
	printf("Pass 1, processed %d triples\r", total_triples_parsed);
	fflush(stdout);
    }
    if (total_triples_parsed == FS_CHUNK_SIZE) {
	if (data->verbosity) printf("Pass 1, processed %d triples (%d)\n", FS_CHUNK_SIZE, data->count_trip);
	*(data->ext_count) += process_quads(data);
	data->last_count = data->count_trip;
	total_triples_parsed = 0;
	gettimeofday(&then_last, 0);
    }
}

/* vi:set expandtab sts=4 sw=4: */
