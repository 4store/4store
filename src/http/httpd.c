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

#define _GNU_SOURCE
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <syslog.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <libgen.h>
#include <glib.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <rasqal.h>

#include "common/4store.h"
#include "common/error.h"
#include "common/hash.h"

#include "frontend/query.h"
#include "frontend/import.h"
#include "frontend/update.h"

#include "httpd.h"

#define WATCHDOG_RATE 16000 /* bytes per second */

/* file globals */

static raptor_uri *bu;
static fsp_link *fsplink;
static int has_o_index = 0;

static long all_time_import_count = 0;
static int global_import_count = 0;
static int unsafe = 0;
static int default_graph = 0;
static int soft_limit = 0; /* default value for soft limit */

static fs_query_state *query_state;

static GThreadPool* pool;
#define QUERY_THREAD_POOL_SIZE 16

static gboolean recv_fn (GIOChannel *source, GIOCondition condition, gpointer data);
static void http_import_queue_remove(client_ctxt *ctxt);
static void http_put_finished(client_ctxt *ctxt, const char *msg);

static FILE *ql_file = NULL;

static void query_log_open (const char *kb_name)
{
  char *filename = g_strdup_printf("/var/log/4store/query-%s.log", kb_name);

  ql_file= fopen(filename, "a");
  if (ql_file) {
    fprintf(ql_file, "\n# 4s-httpd for KB=%s, pid=%d #####\n", kb_name, getpid());
    fflush(ql_file);
  } else {
    fs_error(LOG_WARNING, "couldn't open query log '%s' for appending: %s", filename, strerror(errno));
  }
  g_free(filename);
}

static void query_log_close ()
{
  if (ql_file) {
    fclose(ql_file);
    ql_file = NULL;
  }
}

static void query_log_reopen ()
{
  query_log_close();
  query_log_open(fsp_kb_name(fsplink));
}

static void query_log (const char *query)
{
  if (ql_file) {
    fprintf(ql_file, "#####\n%s\n", query);
    fflush(ql_file);
  }
}

static void daemonize (void)
{
  /* fork once, we don't want to be process leader */
  switch(fork()) {
    case 0:
      break;
    case -1:
      fs_error(LOG_ERR, "fork() error starting daemon: %s", strerror(errno));
      exit(1);
    default:
      _exit(0);
  }

  /* new session / process group */
  if (setsid() == -1) {
    fs_error(LOG_ERR, "setsid() failed starting daemon: %s", strerror(errno));
    exit(1);
  }

  /* fork again, separating ourselves from our parent permanently */

  switch(fork()) {
    case 0:
      break;
    case -1:
      fs_error(LOG_ERR, "fork() error starting daemon: %s", strerror(errno));
      exit(1);
    default:
      _exit(0);
  }

  /* close stdin, stdout, stderr */
  close(0); close(1); close(2);

  /* use up some fds as a precaution against printf() getting
     written to the wire */
  open("/dev/null", 0);
  open("/dev/null", 0);
  open("/dev/null", 0);

  /* move somewhere safe and known */
  if (chdir("/")) {
    fs_error(LOG_ERR, "chdir failed: %s", strerror(errno));
  }
}

static char hexdigit[256] =
{
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 
  0, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 
};

static void url_decode (char *encoded)
{
  unsigned char *chase = (unsigned char *) encoded;

  while (*encoded) {
    switch (*encoded) {
    case '%': {
      unsigned char first = encoded[1];
      unsigned char second = encoded[2];
      *chase = hexdigit[first] * 16 + hexdigit[second];
      chase++;
      encoded += 3;
      break;
    }
    case '+':
      *(chase++) = ' ';
      encoded++;
      break;
    default:
      /* no processing */
      *(chase++) = *encoded;
      encoded++;
      break;
    }
  }
  *chase = '\0'; /* terminate string */
}

static char *just_content_type(client_ctxt *ctxt)
{
  char *result = NULL;
  const char *content_type = g_hash_table_lookup(ctxt->headers, "content-type");
  if (content_type) {
    char **vector = g_strsplit(content_type, ";", 2);
    result = g_strdup(vector[0]);
    g_strfreev(vector);
  }

  return result;
}

static void http_send(client_ctxt *ctxt, const char *msg)
{
  send(ctxt->sock, msg, strlen(msg), 0 /* flags */);
}

static void http_header(client_ctxt *ctxt, const char *code, const char *mimetype)
{
  http_send(ctxt, "HTTP/1.0 "); http_send(ctxt, code); http_send(ctxt, "\r\n");
  http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
  if (mimetype) {
    http_send(ctxt, "Content-Type: "); http_send(ctxt, mimetype); http_send(ctxt, "\r\n");
  }
  http_send(ctxt, "X-Endpoint-Description: /description/"); http_send(ctxt, "\r\n");
  http_send(ctxt, "\r\n");
}

static void http_code(client_ctxt *ctxt, const char *code)
{
  http_send(ctxt, "HTTP/1.0 "); http_send(ctxt, code); http_send(ctxt, "\r\n");
  http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
  http_send(ctxt, "Content-Type: text/html; charset=UTF-8\r\n");
  http_send(ctxt, "\r\n");
  http_send(ctxt, "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n");
  http_send(ctxt, "<html><head><title>");
  http_send(ctxt, code); http_send(ctxt, "</title></head>\n");
  http_send(ctxt, "<body><h1>");
  http_send(ctxt, code);
  http_send(ctxt, "</h1>\n<p>This is a 4store SPARQL server.</p>");
  http_send(ctxt, "<p>4store " GIT_REV "</p>");
  http_send(ctxt, "</body></html>\n");
}

static void http_404(client_ctxt *ctxt, const char *url)
{
  fs_error(LOG_INFO, "HTTP 404 for %s", url);
  http_send(ctxt, "HTTP/1.0 404 Not found\r\n");
  http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
  http_send(ctxt, "Content-Type: text/html; charset=UTF-8\r\n");
  http_send(ctxt, "\r\n");
  http_send(ctxt, "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n");
  http_send(ctxt, "<html><head><title>Not found</title></head>\n");
  http_send(ctxt, "<body><h1>Not found</h1>\n");
  http_send(ctxt, "<p>This is a 4store SPARQL server.</p>");
  http_send(ctxt, "<p>Check <a href=\"/status/\">the status page</a>.</p>");
  http_send(ctxt, "<p>4store " GIT_REV "</p>");
  http_send(ctxt, "</body></html>\n");
}

static void http_error(client_ctxt *ctxt, const char *error)
{
  fs_error(LOG_INFO, "HTTP error, returning status %s", error);
  http_code(ctxt, error);
}

static void http_redirect(client_ctxt *ctxt, const char *to)
{
  http_send(ctxt, "HTTP/1.0 301 Moved permanently\r\n");
  http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
  http_send(ctxt, "Location: "); http_send(ctxt, to); http_send(ctxt, "\r\n");
  http_send(ctxt, "Content-Type: text/html; charset=UTF-8\r\n");
  http_send(ctxt, "\r\n");
  http_send(ctxt, "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n");
  http_send(ctxt, "<html><body><p>See <a href=\"");
  http_send(ctxt, to);
  http_send(ctxt, "\">"); http_send(ctxt, to); http_send(ctxt, "</a>\n");
  http_send(ctxt, "<p>4store " GIT_REV "</p>");
  http_send(ctxt, "</body></html>\n");
}

static void client_free(client_ctxt *ctxt)
{
  if (ctxt->watchdog) {
    g_source_remove(ctxt->watchdog);
  }
  if (ctxt->partial) {
    g_byte_array_free(ctxt->partial, TRUE);
  }
  g_hash_table_destroy(ctxt->headers);
  free(ctxt->request);
  g_free(ctxt);
}

static void http_close(client_ctxt *ctxt)
{
  GSource *s =
    g_main_context_find_source_by_user_data(g_main_context_default(), ctxt);
  if (s) g_source_destroy(s);
  g_io_channel_shutdown(ctxt->ioch, TRUE, NULL);
  g_io_channel_unref(ctxt->ioch);
  client_free(ctxt);
}

static void http_query_worker(gpointer data, gpointer user_data)
{
  client_ctxt *ctxt = (client_ctxt *) data;

  ctxt->qr = fs_query_execute(query_state, fsplink, bu, ctxt->query_string, ctxt->query_flags, 3 /* opt_level */, ctxt->soft_limit);

  http_send(ctxt, "HTTP/1.0 200 OK\r\n");
  http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
  const char *accept = g_hash_table_lookup(ctxt->headers, "accept");

  fcntl(ctxt->sock, F_SETFL, 0 /* not O_NONBLOCK */); /* blocking */
  FILE *fp = fdopen(dup(ctxt->sock), "a+");
  if (fp != NULL) {
    const char *type = "sparql"; /* default */
    int flags = FS_RESULT_FLAG_HEADERS;

    if (ctxt->output) {
      type = ctxt->output;
    } else if (ctxt->qr->construct && accept && strstr(accept, "text/turtle")) {
      type = "text";
      fprintf(fp, "Content-Type: text/turtle\r\n\r\n");
      flags = 0;
    } else if (ctxt->qr->construct && accept && strstr(accept, "application/rdf+xml")) {
      type = "sparql";
    } else if (accept && strstr(accept, "application/sparql-results+xml")) {
      type = "sparql";
    } else if (accept && strstr(accept, "application/sparql-results+json")) {
      type = "json";
    } else if (accept && strstr(accept, "application/json")) {
      type = "json";
    } else if (accept && strstr(accept, "text/tab-separated-values")) {
      type = "text";
    } else if (accept && strstr(accept, "text/plain")) {
      type = "text";
      fprintf(fp, "Content-Type: text/plain; charset=utf-8\r\n\r\n");
      flags = 0;
    }
    fs_query_results_output(ctxt->qr, type, flags, fp);
    fs_query_free(ctxt->qr);
    ctxt->qr = NULL;
    free(ctxt->query_string);
    ctxt->query_string = NULL;
    if (ctxt->output) {
      g_free(ctxt->output);
      ctxt->output = NULL;
    }

    fclose(fp);
  }
  http_close(ctxt);
}

static void http_answer_query(client_ctxt *ctxt, const char *query)
{
  query_log(query);
  ctxt->query_string = g_strdup(query);
  ctxt->update_string = NULL;
  g_source_remove_by_user_data(ctxt);
  g_thread_pool_push(pool, ctxt, NULL);
}

static GSList *import_queue = NULL;

static gboolean import_watchdog (gpointer data)
{
  client_ctxt *ctxt = (client_ctxt *) data;
  /* something has gone wrong, we need to recover */
  fs_error(LOG_ERR, "import watchdog fired");

  http_put_finished(ctxt, "500 watchdog timeout fired");
  return FALSE;
}

static void http_import_start(client_ctxt *ctxt)
{
  /* If it's an update operation, we have a different path */
  if (ctxt->update_string) {
    char *message = NULL;
    int ret = fs_update(fsplink, ctxt->update_string, &message, unsafe);
    fs_query_cache_flush(query_state, 0);
    http_import_queue_remove(ctxt);
    if (ret == 0) {
      http_send(ctxt, "HTTP/1.0 200 OK\r\n");
    } else {
      http_send(ctxt, "HTTP/1.0 400 Bad argument\r\n");
    }
    http_send(ctxt, "Server: 4s-httpd/" GIT_REV "\r\n");
    http_send(ctxt, "Content-Type: text/plain; charset=utf-8\r\n\r\n");
    http_send(ctxt, message);
    http_send(ctxt, "\n");
    http_close(ctxt);

    return;
  }

  const char *length = g_hash_table_lookup(ctxt->headers, "content-length");
  ctxt->bytes_left = atol(length);

  ctxt->importing = 1;
  fs_error(LOG_INFO, "starting import %s (%ld bytes)", ctxt->import_uri, ctxt->bytes_left);

  g_io_add_watch(ctxt->ioch, G_IO_IN, recv_fn, ctxt);

  const char *expect = g_hash_table_lookup(ctxt->headers, "expect");
  if (expect && !strcasecmp(expect, "100-continue")) {
    http_send(ctxt, "HTTP/1.0 100 Continue\r\n\r\n");
  }

  if (fsp_start_import_all(fsplink)) {
    fs_error(LOG_ERR, "fsp_start_import_all failed");
    http_import_queue_remove(ctxt);
    http_error(ctxt, "500 failed during import-start");
    http_close(ctxt);
    return;
  }

  fs_rid_vector *mvec = fs_rid_vector_new(0);
  fs_rid muri = fs_hash_uri(ctxt->import_uri);
  fs_rid_vector_append(mvec, muri);

  if (fsp_delete_model_all(fsplink, mvec) || fsp_new_model_all(fsplink, mvec)) {
    fs_error(LOG_ERR, "fsp_{delete,new}_model_all failed");
    fs_rid_vector_free(mvec);
    fsp_stop_import_all(fsplink);
    http_import_queue_remove(ctxt);
    http_error(ctxt, "500 failed during import-start");
    http_close(ctxt);
    return;
  }

  fs_rid_vector_free(mvec);
  char *type = just_content_type(ctxt);
  fs_import_stream_start(fsplink, ctxt->import_uri, type, has_o_index, &global_import_count);
  g_free(type);

  guint timeout = 30 + (ctxt->bytes_left / WATCHDOG_RATE);
  ctxt->watchdog = g_timeout_add(1000 * timeout, import_watchdog, ctxt);
  if (ctxt->bytes_left == 0) {
    http_put_finished(ctxt, NULL);
  }
}

static void http_post_data(client_ctxt *ctxt, char *model, const char *content_type, char *data)
{
  ctxt->importing = 1;
  long int length = strlen(data);
  fs_error(LOG_INFO, "starting add to %s (%ld bytes)", model, length);

  if (fsp_start_import_all(fsplink)) {
    fs_error(LOG_ERR, "fsp_start_import_all failed");
    http_import_queue_remove(ctxt);
    http_error(ctxt, "500 failed during import-start");
    http_close(ctxt);
    return;
  }

  fs_import_stream_start(fsplink, model, content_type, has_o_index, &global_import_count);

  guint timeout = 30 + (ctxt->bytes_left / WATCHDOG_RATE);
  ctxt->watchdog = g_timeout_add(1000 * timeout, import_watchdog, ctxt);

  fs_import_stream_data(fsplink, (unsigned char *)data, length);

  if (ctxt->watchdog) {
    g_source_remove(ctxt->watchdog);
    ctxt->watchdog = 0;
  }

  int error_count = -1;
  fs_import_stream_finish(fsplink, &global_import_count, &error_count);
  all_time_import_count += global_import_count;
  global_import_count = 0;
  fsp_stop_import_all(fsplink);

  fs_query_cache_flush(query_state, 0);

  ctxt->importing = 0;
  fs_error(LOG_INFO, "finished add to %s", model);

  http_import_queue_remove(ctxt);

  if (error_count == -1) {
    fs_error(LOG_ERR, "import_stream_finish didn't complete successfully");
    http_error(ctxt, "500 server problem while importing");
  } else if (error_count > 0) {
    http_error(ctxt, "400 RDF parser reported errors");
  } else {
    http_code(ctxt, "200 added successfully");
  }
  http_close(ctxt);
}

static void http_import_queue_remove(client_ctxt *ctxt)
{
  import_queue = g_slist_remove(import_queue, ctxt);
  if (import_queue) {
    http_import_start((client_ctxt *) import_queue->data);
  }
}

static void http_put_finished(client_ctxt *ctxt, const char *msg)
{
  if (ctxt->watchdog) {
    g_source_remove(ctxt->watchdog);
    ctxt->watchdog = 0;
  }

  int error_count = -1;
  fs_import_stream_finish(fsplink, &global_import_count, &error_count);
  all_time_import_count += global_import_count;
  global_import_count = 0;
  fsp_stop_import_all(fsplink);

  fs_query_cache_flush(query_state, 0);

  ctxt->importing = 0;
  if (ctxt->bytes_left) {
    fs_error(LOG_INFO, "finished import %s (%ld bytes left)", ctxt->import_uri, ctxt->bytes_left);
  } else {
    fs_error(LOG_INFO, "finished import %s", ctxt->import_uri);
  }
  g_free(ctxt->import_uri);
  ctxt->import_uri = NULL;

  http_import_queue_remove(ctxt);

  if (msg) {
    http_error(ctxt, msg);
  } else if (error_count == -1) {
    fs_error(LOG_ERR, "import_stream_finish didn't complete successfully");
    http_error(ctxt, "500 server problem while importing");
  } else if (error_count > 0) {
    http_error(ctxt, "400 RDF parser reported errors");
  } else {
    http_code(ctxt, "201 imported successfully");
  }
  http_close(ctxt);
}

static void http_put_request(client_ctxt *ctxt, gchar *url, gchar *protocol)
{
  /* special case, allow /sparql/http://example.com/ as an alias for
                         http://example.com/ */
  if (!strncmp(url, "/sparql/", 8)) {
    url += 8;
    url_decode(url);
  } else if (!strncmp(url, "/data/", 6)) { /* we want to move people towards /data/ */
    url += 6;
    url_decode(url);
  } else if (!strncmp(url, "/", 1)) {
    http_error(ctxt, "403 forbidden - invalid URI");
    http_close(ctxt);
    return;
  }

  if (!g_hash_table_lookup(ctxt->headers, "content-length")) {
    http_error(ctxt, "411 content length required");
    http_close(ctxt);
    return;
  }

  ctxt->import_uri = g_strdup(url);
  ctxt->update_string = NULL;

  g_source_remove_by_user_data(ctxt);
  if (import_queue) {
    import_queue = g_slist_append(import_queue, ctxt);
  } else {
    import_queue = g_slist_append(import_queue, ctxt);
    http_import_start(ctxt);
  }
}

static void http_delete_request(client_ctxt *ctxt, gchar *url, gchar *protocol)
{
  /* special case, allow /sparql/http://example.com/ as an alias for
                         http://example.com/ */
  if (!strncmp(url, "/sparql/", 8)) {
    url += 8;
    url_decode(url);
  } else if (!strncmp(url, "/data/", 6)) { /* we want to move people towards /data/ */
    url += 6;
    url_decode(url);
  } else if (!strncmp(url, "/", 1)) {
    http_error(ctxt, "403 forbidden - invalid URI");
    http_close(ctxt);
    return;
  }

  /* now we have a UTF-8 raw string */

  fs_rid_vector *mvec = fs_rid_vector_new(0);
  fs_rid muri = fs_hash_uri(url);
  fs_rid_vector_append(mvec, muri);

  if (fsp_delete_model_all(fsplink, mvec)) {
    fs_error(LOG_ERR, "error while trying to delete model <%s>", url);
    http_error(ctxt, "500 failed while adding new model");
  } else {
    fs_query_cache_flush(query_state, 0);
    fs_error(LOG_INFO, "deleted model <%s>", url);
    http_error(ctxt, "200 deleted successfully");
  }
  fs_rid_vector_free(mvec);

  http_close(ctxt);
}

static void http_status_report(client_ctxt *ctxt)
{
  http_send(ctxt, "HTTP/1.0 200 OK\r\n"
  "Server: 4s-httpd/" GIT_REV "\r\n"
  "Content-Type: text/html; charset=UTF-8\r\n"
  "\r\n"
  "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n"
  "<html><head><title>SPARQL httpd server status</title></head>\n"
  "<body><h1>SPARQL httpd server " GIT_REV " status</h1>\n"
  "<h2>KB ");
  http_send(ctxt, fsp_kb_name(fsplink));
  http_send(ctxt, "</h2>\n");

  char *running = g_strdup_printf("%u", g_thread_pool_get_num_threads(pool));
  char *outstanding = g_strdup_printf("%u", g_thread_pool_unprocessed(pool));

  http_send(ctxt, "<table><tr><th>Running queries</th><td>");
  http_send(ctxt, running); http_send(ctxt, "</td></tr>\n");
  http_send(ctxt, "<tr><th>Outstanding queries</th><td>");
  http_send(ctxt, outstanding); http_send(ctxt, "</td></tr>\n");
  http_send(ctxt, "</table>\n");

  g_free(running);
  g_free(outstanding);


  http_send(ctxt, "<p><a href=\"/status/size/\">4store backend size info</a></p>\n");
  http_send(ctxt, "<p><a href=\"/test/\">Execute a test query</a></p>\n");

  if (import_queue) { /* this must be synchronous for now */
    http_send(ctxt, "<h3>Imports in progress</h3>\n");
    GSList *queue;
    for (queue = import_queue; queue; queue = queue->next) {
      client_ctxt *import_ctxt = (client_ctxt *) queue->data;
      http_send(ctxt, import_ctxt->import_uri);
      http_send(ctxt, "<br>\n");
    }
    char *flushed = g_strdup_printf("<p># triples from this import so far: %d</p>\n", global_import_count);
    http_send(ctxt, flushed);
    g_free(flushed);
  }

  char *triples = g_strdup_printf("<p>Total # triples imported: %ld</p>\n", all_time_import_count);
  http_send(ctxt, triples);
  g_free(triples);

  http_send(ctxt, "</body></html>\n");
  http_close(ctxt);
}

static void http_size_report(client_ctxt *ctxt)
{
  http_send(ctxt, "HTTP/1.0 200 OK\r\n"
  "Server: 4s-httpd/" GIT_REV "\r\n"
  "Content-Type: text/html; charset=UTF-8\r\n"
  "\r\n"
  "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n"
  "<html><head><title>SPARQL httpd server status - size</title></head>\n"
  "<body><h1>SPARQL httpd server " GIT_REV " status - size</h1>\n"
  "<h2>KB ");
  http_send(ctxt, fsp_kb_name(fsplink));
  http_send(ctxt, "</h2>\n");

  fs_data_size sz, total = {0, 0, 0, 0, 0, 0};

  http_send(ctxt,"<table><tr><th>Segment #</th><th>quads (s)</th><th>quads (sr)</th>\
                  <th>models</th><th>resources</th></tr>\n");

  const int segments = fsp_link_segments(fsplink);
  for (fs_segment seg = 0; seg < segments; ++seg) {
    if (fsp_get_data_size(fsplink, seg, &sz)) {
      http_send(ctxt, "<tr><td>Unexpected error fetching size info from segment</td></tr>\n");
    } else {
      char *line = g_strdup_printf("<tr><td>%d</td><td>%lld</td><td>%+lld</td><td>%lld</td><td>%lld</td></tr>\n",
                                   seg, sz.quads_s, sz.quads_sr - sz.quads_s, sz.models_s, sz.resources);
      http_send(ctxt, line);
      g_free(line);
    }
    total.quads_s += sz.quads_s;
    total.quads_sr += sz.quads_sr;
    total.quads_o += sz.quads_o;
    if (sz.models_s > total.models_s) total.models_s = sz.models_s;
    total.resources += sz.resources;
  }

  char *final = g_strdup_printf("<tr><th>Total</th><td>%lld</td><td>%+lld</td><td>%lld</td><td>%lld</td></tr>\n",
                               total.quads_s, total.quads_sr - total.quads_s , total.models_s, total.resources);
  http_send(ctxt, final);
  g_free(final);
 
  http_send(ctxt, "</body></html>\n");
  http_close(ctxt);
}

static void http_service_description(client_ctxt *ctxt)
{
  http_send(ctxt, "HTTP/1.0 200 OK\r\n"
  "Server: 4s-httpd/" GIT_REV "\r\n"
  "Content-Type: application/x-turtle\r\n"
  "\r\n"
  "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
  "@prefix sd: <http://darq.sf.net/dose/0.1#> .\n"
  "@prefix saddle: <http://www.w3.org/2005/03/saddle/#> .\n"
  "@prefix sparql: <http://kasei.us/2008/04/sparql#> .\n"
  "@prefix void: <http://rdfs.org/ns/void#> .\n"
  "[] a sd:Service ;\n"
  "  saddle:queryLanguage [\n"
  "    rdfs:label \"SPARQL\" ;\n"
  "    saddle:spec <http://www.w3.org/TR/rdf-sparql-query/>\n"
  "  ] ;\n"
  "  saddle:queryLanguage [\n"
  "    rdfs:label \"RDQL\" ;\n"
  "    saddle:spec <http://www.w3.org/Submission/RDQL/>\n"
  "  ] ;\n"
  "  saddle:resultFormat [\n"
  "    rdfs:label \"SPARQL Query Results XML\" ;\n"
  "    saddle:mediaType \"application/sparql-results+xml\" ;\n"
  "    saddle:spec <http://www.w3.org/TR/rdf-sparql-XMLres/>\n"
  "  ] ;\n"
  "  saddle:resultFormat [\n"
  "    rdfs:label \"RDF/XML\" ;\n"
  "    saddle:mediaType \"application/rdf+xml\" ;\n"
  "    saddle:spec <http://www.w3.org/TR/rdf-syntax/>\n"
  "  ] ;\n"
  "  saddle:resultFormat [\n"
  "    rdfs:label \"SPARQL Query Results JSON\" ;\n"
  "    saddle:mediaType \"application/sparql-results+json\" ;\n"
  "    saddle:spec <http://www.w3.org/TR/rdf-sparql-json-res/>\n"
  "  ] ;\n"
  "  saddle:resultFormat [\n"
  "    rdfs:label \"SPARQL Query Results UTF-8 text\" ;\n"
  "    saddle:mediaType \"text/plain\" ;\n"
  "    saddle:spec <http://example.org/sparql/text-format/>\n"
  "  ] ;\n"
  "  sparql:sparqlExtension <http://kasei.us/2008/04/sparql-extension/service> ;\n");

  http_send(ctxt, "  rdfs:label \"4store SPARQL Endpoint for ");
  http_send(ctxt, fsp_kb_name(fsplink));
  http_send(ctxt, " (" GIT_REV ")\" ;\n");

  fs_data_size sz, total = {0, 0, 0, 0, 0, 0};
  const int segments = fsp_link_segments(fsplink);
  for (fs_segment seg = 0; seg < segments; ++seg) {
    if (fsp_get_data_size(fsplink, seg, &sz)) {
      http_send(ctxt, "# Unexpected error fetching size info from segment\n");
    }
    total.quads_s += sz.quads_s;
    total.quads_sr += sz.quads_sr;
    total.quads_o += sz.quads_o;
    if (sz.models_s > total.models_s) total.models_s = sz.models_s;
    total.resources += sz.resources;
  }

  char *size = g_strdup_printf("  sd:totalTriples %llu ;\n  sd:totalResources %llu ;\n", total.quads_s, total.resources);
  http_send(ctxt, size);
  g_free(size);
 
  http_send(ctxt, ".\n");
  http_close(ctxt);
}

static void http_query_widget(client_ctxt *ctxt)
{
  http_send(ctxt, "HTTP/1.0 200 OK\r\n"
  "Server: 4s-httpd/" GIT_REV "\r\n"
  "Content-Type: text/html; charset=UTF-8\r\n"
  "\r\n"
  "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n"
  "<html><head><title>SPARQL httpd test query</title></head>\n"
  "<body><h1>SPARQL httpd server " GIT_REV " test query</h1>\n"
  "<h2>KB ");
  http_send(ctxt, fsp_kb_name(fsplink));
  http_send(ctxt, "</h2>\n");

  http_send(ctxt, "<form action=\"../sparql/\" method=\"post\">\n"
   "<textarea name=\"query\" cols=\"80\" rows=\"18\">\n"
   "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
   "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
   "\nSELECT * WHERE {\n ?s ?p ?o\n} LIMIT 10\n"
   "</textarea><br>\n"
   "<em>Soft limit</em> <input type=\"text\" name=\"soft-limit\">\n"
   "<input type=\"submit\" value=\"Execute\"><input type=\"reset\">\n"
   "</form>\n");

  http_send(ctxt, "</body></html>\n");
  http_close(ctxt);
}

static void http_get_request(client_ctxt *ctxt, gchar *url, gchar *protocol)
{
  char *default_graph = NULL; /* ignored for now */

  char *qm = strchr(url, '?');
  char *qs = qm ? qm + 1 : NULL;
  if (qs) {
    *qm = '\0';
  }
  char *path = url;
  url_decode(path);
  if (!strcmp(path, "/sparql/")) {
    char *query = NULL;
    while (qs) {
      char *ampersand = strchr(qs, '&');
      char *next = ampersand ? ampersand + 1 : NULL;
      if (next) {
        *ampersand = '\0';
      }
      char *key = qs;
      char *equals = strchr(qs, '=');
      char *value = equals ? equals + 1 : NULL;
      if (equals) {
        *equals = '\0';
      }
      url_decode(key);
      if (!strcmp(key, "query") && value) {
        url_decode(value);
        query = value;
      } else if (!strcmp(key, "restricted")) {
        ctxt->query_flags |= FS_QUERY_RESTRICTED;
      } else if (!strcmp(key, "soft-limit") && value) {
        url_decode(value);
        if (strlen(value)) { /* ignore empty string, default form value */
          ctxt->soft_limit = atoi(value);
        }
      } else if (!strcmp(key, "output") && value) {
        url_decode(value);
        ctxt->output = g_strdup(value);
      } else if (!strcmp(key, "default-graph-uri") && value) {
        url_decode(value);
        default_graph = value;
      }
      qs = next;
    }
    if (query) {
      http_answer_query(ctxt, query);
    } else {
      http_error(ctxt, "500 SPARQL protocol error");
      http_close(ctxt);
    }
  } else if (!strcmp(path, "/sparql")) {
    http_redirect(ctxt, "sparql/");
    http_close(ctxt);
  } else if (!strcmp(path, "/status/")) {
    http_status_report(ctxt);
  } else if (!strcmp(path, "/status")) {
    http_redirect(ctxt, "status/");
    http_close(ctxt);
  } else if (!strcmp(path, "/status/size/")) {
    http_size_report(ctxt);
  } else if (!strcmp(path, "/description/")) {
    http_service_description(ctxt);
  } else if (!strcmp(path, "/test/")) {
    http_query_widget(ctxt);
  } else if (!strcmp(path, "/test")) {
    http_redirect(ctxt, "test/");
    http_close(ctxt);
  } else {
    http_404(ctxt, url);
    http_close(ctxt);
  }
}

static void http_head_request(client_ctxt *ctxt, gchar *url, gchar *protocol)
{
  char *qm = strchr(url, '?');
  char *qs = qm ? qm + 1 : NULL;
  if (qs) {
    *qm = '\0';
  }
  char *path = url;
  url_decode(path);
  if (!strcmp(path, "/sparql/")) {
    http_header(ctxt, "200", "application/sparql-results+xml");
  } else if (!strcmp(path, "/status/")) {
    http_header(ctxt, "200", "text/html; charset=UTF-8");
  } else if (!strcmp(path, "/status/size/")) {
    http_header(ctxt, "200", "text/html; charset=UTF-8");
  } else if (!strcmp(path, "/test/")) {
    http_header(ctxt, "200", "text/html; charset=UTF-8");
  } else {
    http_header(ctxt, "404", "text/html; charset=UTF-8");
  }

  http_close(ctxt);
}

static void http_post_request(client_ctxt *ctxt, gchar *url, gchar *protocol)
{
  char *default_graph = NULL; /* ignored for now */

  url_decode(url);
  if (!strcmp(url, "/sparql/")) {
    char *form_type = just_content_type(ctxt);
    if (!form_type || strcasecmp(form_type, "application/x-www-form-urlencoded")) {
      http_error(ctxt, "400 4store only implements application/x-www-form-urlencoded");
      http_close(ctxt);
      g_free(form_type);
      return;
    }
    g_free(form_type);

    const char *length = g_hash_table_lookup(ctxt->headers, "content-length");
    ctxt->bytes_left = length ? atol(length) : 0;

    if (ctxt->bytes_left == 0) {
      http_error(ctxt, "500 SPARQL protocol error, empty");
      http_close(ctxt);
      return;
    }

    /* FIXME this could block almost indefinitely */
    gchar *form = g_malloc0(ctxt->bytes_left + 1);
    gchar *buffer = form;

    for (gsize read = 0; ctxt->bytes_left > 0; buffer += read, ctxt->bytes_left -= read) {
      GIOStatus result;
      do {
        result = g_io_channel_read_chars(ctxt->ioch, buffer, ctxt->bytes_left, &read, NULL);
      } while (result == G_IO_STATUS_AGAIN);
      if (result !=  G_IO_STATUS_NORMAL) {
        fs_error(LOG_ERR, "unexpected IO status %u during POST request", result);
        g_free(form);
        http_error(ctxt, "500 SPARQL server error");
        http_close(ctxt);
        return;
      }
    }

    char *query = NULL;
    char *qs = form;
    while (qs) {
      char *ampersand = strchr(qs, '&');
      char *next = ampersand ? ampersand + 1 : NULL;
      if (next) {
        *ampersand = '\0';
      }
      char *key = qs;
      char *equals = strchr(qs, '=');
      char *value = equals ? equals + 1 : NULL;
      if (equals) {
        *equals = '\0';
      }
      url_decode(key);
      if (!strcmp(key, "query") && value) {
        url_decode(value);
        query = value;
      } else if (!strcmp(key, "restricted")) {
        ctxt->query_flags= FS_QUERY_RESTRICTED;
      } else if (!strcmp(key, "soft-limit") && value) {
        url_decode(value);
        if (strlen(value)) { /* ignore empty string, default form value */
          ctxt->soft_limit = atoi(value);
        }
      } else if (!strcmp(key, "default-graph-uri") && value) {
        url_decode(value);
        default_graph = value;
      }
      qs = next;
    }
    if (query) {
      http_answer_query(ctxt, query);
    } else {
      http_error(ctxt, "500 SPARQL protocol error");
      http_close(ctxt);
    }
    g_free(form);

  } else if (!strcmp(url, "/update/")) {
    const char *form_type = g_hash_table_lookup(ctxt->headers, "content-type");
    if (!form_type || strcasecmp(form_type, "application/x-www-form-urlencoded")) {
      http_error(ctxt, "400 4store only implements application/x-www-form-urlencoded");
      http_close(ctxt);
      return;
    }

    const char *length = g_hash_table_lookup(ctxt->headers, "content-length");
    ctxt->bytes_left = length ? atol(length) : 0;

    if (ctxt->bytes_left == 0) {
      http_error(ctxt, "500 SPARQL protocol error, empty");
      http_close(ctxt);
      return;
    }

    /* FIXME this could block almost indefinitely */
    gchar *form = g_malloc0(ctxt->bytes_left + 1);
    gchar *buffer = form;

    for (gsize read = 0; ctxt->bytes_left > 0; buffer += read, ctxt->bytes_left -= read) {
      GIOStatus result;
      do {
        result = g_io_channel_read_chars(ctxt->ioch, buffer, ctxt->bytes_left, &read, NULL);
      } while (result == G_IO_STATUS_AGAIN);
      if (result !=  G_IO_STATUS_NORMAL) {
        fs_error(LOG_ERR, "unexpected IO status %u during POST request", result);
        g_free(form);
        http_error(ctxt, "500 SPARQL server error");
        http_close(ctxt);
        return;
      }
    }

    char *update = NULL;
    char *qs = form;
    while (qs) {
      char *ampersand = strchr(qs, '&');
      char *next = ampersand ? ampersand + 1 : NULL;
      if (next) {
        *ampersand = '\0';
      }
      char *key = qs;
      char *equals = strchr(qs, '=');
      char *value = equals ? equals + 1 : NULL;
      if (equals) {
        *equals = '\0';
      }
      url_decode(key);
      if (!strcmp(key, "update") && value) {
        url_decode(value);
        update = value;
      }
      qs = next;
    }
    if (update) {
      ctxt->update_string = update;
      g_source_remove_by_user_data(ctxt);
      if (import_queue) {
        import_queue = g_slist_append(import_queue, ctxt);
      } else {
        import_queue = g_slist_append(import_queue, ctxt);
        http_import_start(ctxt);
      }
    } else {
      http_error(ctxt, "500 SPARQL protocol error");
      http_close(ctxt);
    }
    g_free(form);

  } else if (!strcmp(url, "/data/")) {
    char *form_type = just_content_type(ctxt);
    if (!form_type || strcasecmp(form_type, "application/x-www-form-urlencoded")) {
      http_error(ctxt, "400 4store only implements application/x-www-form-urlencoded");
      http_close(ctxt);
      g_free(form_type);
      return;
    }
    g_free(form_type);

    const char *length = g_hash_table_lookup(ctxt->headers, "content-length");
    ctxt->bytes_left = length ? atol(length) : 0;

    if (ctxt->bytes_left == 0) {
      http_error(ctxt, "500 SPARQL REST protocol error, empty");
      http_close(ctxt);
      return;
    }

    /* FIXME this could block almost indefinitely */
    gchar *form = g_malloc0(ctxt->bytes_left + 1);
    gchar *buffer = form;

    for (gsize read = 0; ctxt->bytes_left > 0; buffer += read, ctxt->bytes_left -= read) {
      GIOStatus result;
      do {
        result = g_io_channel_read_chars(ctxt->ioch, buffer, ctxt->bytes_left, &read, NULL);
      } while (result == G_IO_STATUS_AGAIN);
      if (result !=  G_IO_STATUS_NORMAL) {
        fs_error(LOG_ERR, "unexpected IO status %u during POST request", result);
        g_free(form);
        http_error(ctxt, "500 SPARQL REST server error");
        http_close(ctxt);
        return;
      }
    }

    char *graph = NULL;
    char *mime_type = NULL;
    char *data = NULL;
    char *qs = form;
    while (qs) {
      char *ampersand = strchr(qs, '&');
      char *next = ampersand ? ampersand + 1 : NULL;
      if (next) {
        *ampersand = '\0';
      }
      char *key = qs;
      char *equals = strchr(qs, '=');
      char *value = equals ? equals + 1 : NULL;
      if (equals) {
        *equals = '\0';
      }
      url_decode(key);
      if (!strcmp(key, "graph") && value) {
        url_decode(value);
        graph = value;
      } else if (!strcmp(key, "data") && value) {
        url_decode(value);
        data = value;
      } else if (!strcmp(key, "mime-type") && value) {
        url_decode(value);
        mime_type = value;
      }
      qs = next;
    }
    if (graph && data) {
      http_post_data(ctxt, graph, mime_type, data);
    } else {
      http_error(ctxt, "500 SPARQL REST protocol error");
      http_close(ctxt);
    }
    g_free(form);
  } else {
    http_404(ctxt, url);
    http_close(ctxt);
  }
}

static void http_request(client_ctxt *ctxt, gchar *request)
{
  if (!strncasecmp(request, "POST ", 5)) {
    /* POST request */
    char *url = strdup(request + 5);
    char *space = strrchr(url, ' ');
    char *protocol = NULL;
    if (space) {
      protocol = space + 1;
      *space = '\0';
    }
    http_post_request(ctxt, url, protocol);
    free(url);
  } else if (!strncasecmp(request, "HEAD ", 5)) {
    /* HEAD request */
    char *url = strdup(request + 5);
    char *space = strrchr(url, ' ');
    char *protocol = NULL;
    if (space) {
      protocol = space + 1;
      *space = '\0';
    }
    http_head_request(ctxt, url, protocol);
    free(url);
  } else if (!strncasecmp(request, "GET ", 4)) {
    /* GET request */
    char *url = strdup(request + 4);
    char *space = strrchr(url, ' ');
    char *protocol = NULL;
    if (space) {
      protocol = space + 1;
      *space = '\0';
    }
    http_get_request(ctxt, url, protocol);
    free(url);
  } else if (!strncasecmp(request, "PUT ", 4)) {
    /* PUT request */
    char *url = strdup(request + 4);
    char *space = strrchr(url, ' ');
    char *protocol = NULL;
    if (space) {
      protocol = space + 1;
      *space = '\0';
    }
    http_put_request(ctxt, url, protocol);
    free(url);
  } else if (!strncasecmp(request, "DELETE ", 7)) {
    /* DELETE request */
    char *url = strdup(request + 7);
    char *space = strrchr(url, ' ');
    char *protocol = NULL;
    if (space) {
      protocol = space + 1;
      *space = '\0';
    }
    http_delete_request(ctxt, url, protocol);
    free(url);
  } else {
    /* 400 */
    http_error(ctxt, "400 Bad Request");
    http_close(ctxt);
  }
}

static void http_line(client_ctxt *ctxt, gchar *line)
{
  if (!ctxt->request) {
    /* FIXME handle HTTP/0.9 */
    ctxt->request = g_strchomp(line);
  } else if (!strcmp(line, "\r\n")) {
    http_request(ctxt, ctxt->request);
    free(line);
  } else {
    /* header */
    char *name = line;
    char *colon = strchr(line, ':');
    char *value;
    if (colon) {
      value = colon + 1;
      g_hash_table_insert(ctxt->headers, g_ascii_strdown(name, colon - name), g_strdup(g_strstrip(value)));
    }
    free(line);
  }
}

static gboolean recv_fn (GIOChannel *source, GIOCondition condition, gpointer data)
{
  client_ctxt *ctxt = (client_ctxt *) data;
  GError *err = NULL;

  if (ctxt->importing) {
    gchar buffer[2048];
    gsize max = sizeof(buffer), read = 0;
    if (ctxt->bytes_left < max) max = ctxt->bytes_left;
    GIOStatus result = g_io_channel_read_chars(source, buffer, max, &read, &err);
    switch (result) {
      case G_IO_STATUS_NORMAL:
        ctxt->bytes_left -= read;
        fs_import_stream_data(fsplink, (unsigned char *) buffer, read);
        if (ctxt->bytes_left == 0) {
          http_put_finished(ctxt, NULL);
        }
        break;
      case G_IO_STATUS_EOF:
        /* possible early EOF, but still some sort of success */
        http_put_finished(ctxt, NULL);
        break;
      case G_IO_STATUS_ERROR:
        fs_error(LOG_ERR, "I/O error: %s during import", err ? err->message : "unknown");
        http_put_finished(ctxt, "500 I/O error");
        break;
      case G_IO_STATUS_AGAIN:
        fs_error(LOG_ERR, "unexpected G_IO_STATUS_AGAIN during import");
        http_put_finished(ctxt, "500 AGAIN error");
        break;
      default:
        fs_error(LOG_ERR, "unexpected GIOStatus during import");
    }
    return TRUE;
  }

  gchar *line;
  GIOStatus result = g_io_channel_read_line(source, &line, NULL, NULL, &err);

  switch (result) {
   guchar buffer[128];
   gsize read;
   case G_IO_STATUS_NORMAL:
      /* push queries into TODO buffer */
      if (ctxt->partial) {
        guchar zero = 0;
        g_byte_array_append(ctxt->partial, &zero, 1);
        char *tmp = line;
        line = g_strdup_printf("%s%s", ctxt->partial->data, tmp);
        g_free(tmp);
        g_byte_array_free(ctxt->partial, TRUE);
        ctxt->partial = NULL;
      }
      http_line(ctxt, line);
      break;
    case G_IO_STATUS_ERROR:
      fs_error(LOG_ERR, "I/O error: %s", err ? err->message : "unknown");
      /* fall through to close */
    case G_IO_STATUS_EOF:
      g_source_remove_by_user_data(data);
      g_io_channel_shutdown(ctxt->ioch, TRUE, NULL);
      g_io_channel_unref(ctxt->ioch);
      client_free(ctxt);
      return FALSE;
    case G_IO_STATUS_AGAIN:
      if (g_io_channel_read_chars(source, (gchar *) buffer, sizeof(buffer), &read, &err) == G_IO_STATUS_NORMAL) {
        if (!ctxt->partial) {
          ctxt->partial =  g_byte_array_new();
        }
        g_byte_array_append(ctxt->partial, buffer, read);
      }
      break;

    default:
      fs_error(LOG_ERR, "unexpected GIOStatus");
  }

  return TRUE;
}

gboolean accept_fn (GIOChannel *source, GIOCondition condition, gpointer data)
{
  client_ctxt *ctxt = g_new0(client_ctxt, 1);
  ctxt->headers = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
  ctxt->sock = accept(g_io_channel_unix_get_fd(source), NULL, NULL);
  ctxt->query_flags = 0; /* FS_QUERY_RESTRICTED; default to unrestricted */
  if (default_graph) {
    ctxt->query_flags |= FS_QUERY_DEFAULT_GRAPH;
  }
  /* set default value */
  ctxt->soft_limit = soft_limit;
  fcntl(ctxt->sock, F_SETFL, O_NONBLOCK); /* non-blocking */
  GIOChannel *connector = g_io_channel_unix_new (ctxt->sock);
  g_io_channel_set_encoding(connector, NULL, NULL);
  g_io_channel_set_line_term(connector, "\r\n", -1);
  ctxt->ioch = connector;
  g_io_add_watch(connector, G_IO_IN, recv_fn, ctxt);

  return TRUE;
}

static void do_hangup(int sig)
{
  /* kill children or something ? */
  fs_error(LOG_INFO, "signal SIGHUP received, reopening logs");
  query_log_reopen();
}

static volatile sig_atomic_t fatal_error_in_progress = 0;

static void do_kill(int sig)
{
  if (fatal_error_in_progress) raise (sig);
  fatal_error_in_progress = 1;

  signal (sig, SIG_DFL);
  fs_error(LOG_INFO, "signal %s received", strsignal(sig));
  kill (0, sig);
}

static void do_sigmisc(int sig)
{
  if (fatal_error_in_progress) raise (sig);
  fatal_error_in_progress = 1;

  signal (sig, SIG_DFL);
  fs_error(LOG_INFO, "signal %s received in child", strsignal(sig));
  raise (sig);
}

static const char *explain_siginfo(siginfo_t *info)
{
  if (info->si_code == SI_USER) {
    return "killed manually e.g with kill(1)";
  }
#ifdef SI_KERNEL
  if (info->si_code == SI_KERNEL) {
    return "killed by the kernel";
  }
#endif
  if (info->si_code == ILL_ILLOPC && info->si_signo == SIGILL) {
    return "illegal opcode";
  }
  if (info->si_code == ILL_ILLOPN && info->si_signo == SIGILL) {
    return "illegal operand";
  }
  if (info->si_code == ILL_ILLADR && info->si_signo == SIGILL) {
    return "illegal addressing mode";
  }
  if (info->si_code == ILL_ILLTRP && info->si_signo == SIGILL) {
    return "illegal trap";
  }
  if (info->si_code == FPE_INTDIV && info->si_signo == SIGFPE) {
    return "integer divide by zero";
  }
  if (info->si_code == FPE_INTOVF && info->si_signo == SIGFPE) {
    return "integer overflow";
  }
  if (info->si_code == FPE_FLTDIV && info->si_signo == SIGFPE) {
    return "floating point divide by zero";
  }
  if (info->si_code == BUS_ADRALN && info->si_signo == SIGBUS) {
    return "invalid address alignment";
  }

  return "no additional information decoded";
}

static void do_backtrace(int sig, siginfo_t *info, void *blah)
{
  if (fatal_error_in_progress) raise (sig);
  fatal_error_in_progress = 1;

  signal (sig, SIG_DFL);

  const char *additional = explain_siginfo(info);

  fs_error(LOG_CRIT, "signal %s received in child, backtracing, %s", strsignal(sig), additional);
  raise (sig);
}

static void signal_actions_parent(void)
{
  struct sigaction ignore_action = {
    .sa_handler = SIG_IGN,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&ignore_action.sa_mask);

  struct sigaction kill_action = {
    .sa_handler = &do_kill,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&kill_action.sa_mask);

  sigaction(SIGHUP, &ignore_action, NULL); /* HUP ignored in parent */
  sigaction(SIGINT, &kill_action, NULL); /* ^C */
  sigaction(SIGTERM, &kill_action, NULL); /* kill */
}

static void signal_actions_child(void)
{
  struct sigaction hangup_action = {
    .sa_handler = &do_hangup,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&hangup_action.sa_mask);

  struct sigaction misc_action = {
    .sa_handler = &do_sigmisc,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&misc_action.sa_mask);

  struct sigaction backtrace_action = {
    .sa_sigaction = &do_backtrace,
    .sa_flags = (SA_RESTART | SA_SIGINFO),
  };
  sigfillset(&backtrace_action.sa_mask);

  struct sigaction ignore_action = {
    .sa_handler = SIG_IGN,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&ignore_action.sa_mask);

  sigaction(SIGHUP, &hangup_action, NULL); /* HUP re-opens logs */
  sigaction(SIGINT, &misc_action, NULL); /* ^C */
  sigaction(SIGTERM, &misc_action, NULL); /* kill */
  sigaction(SIGPIPE, &ignore_action, NULL); /* connection went away */

  sigaction(SIGFPE, &backtrace_action, NULL); /* DIV/0 or similar */
  sigaction(SIGBUS, &backtrace_action, NULL); /* address alignment etc. */
  sigaction(SIGABRT, &backtrace_action, NULL); /* abort */
  sigaction(SIGSEGV, &backtrace_action, NULL); /* segfault */
}

static int server_setup (int background, const char *host, const char *port)
{
  struct addrinfo hints, *info;
  int err, on = 1;

  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM; /* tcp */
  hints.ai_flags = AI_PASSIVE;

  if ((err = getaddrinfo(host, port, &hints, &info))) {
    fs_error(LOG_ERR, "getaddrinfo failed: %s", gai_strerror(err));
    return -1;
  }
  int srv = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
  if (srv < 0) {
    fs_error(LOG_ERR, "socket failed");
    freeaddrinfo(info);
    return -2;
  }

  if (setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
    fs_error(LOG_WARNING, "setsockopt SO_REUSEADDR failed");
  }

  if (bind(srv, info->ai_addr, info->ai_addrlen) < 0) {
    fs_error(LOG_ERR, "server socket bind failed: %s", strerror(errno));
    freeaddrinfo(info);
    return -3;
  }

  freeaddrinfo(info);

  if (listen(srv, 64) < 0) {
    fs_error(LOG_ERR, "listen failed");
    return -4;
  }

  if (background) {
    daemonize();
  }

  signal_actions_parent();
  if (host != NULL) {
    fs_error(LOG_INFO, "4store HTTP daemon " GIT_REV " started on host %s port %s", host, port);
  } else {
    fs_error(LOG_INFO, "4store HTTP daemon " GIT_REV " started on port %s", port);
  }
  return srv;
}

static void child (int srv, char *kb_name, char *password)
{
  signal_actions_child();

  fsplink = fsp_open_link(kb_name, password, FS_OPEN_HINT_RW);
  if (!fsplink) {
    fs_error(LOG_ERR, "couldn't connect to “%s”", kb_name);
    exit(3);
  }
  if (fsp_no_op(fsplink, 0)) {
    fs_error(LOG_ERR, "NO-OP failed for “%s”", kb_name);
    exit(4);
  }

  raptor_init();
#ifndef HAVE_RASQAL_WORLD
  rasqal_init();
#endif /* ! HAVE_RASQAL_WORLD */
  fs_hash_init(fsp_hash_type(fsplink));

  bu = raptor_new_uri((unsigned char *)"local:");

  const char *features = fsp_link_features(fsplink);
  has_o_index = !(strstr(features, "no-o-index")); /* tweak */

  query_log_open(kb_name);

  query_state = fs_query_init(fsplink);
  g_thread_init(NULL);
  pool = g_thread_pool_new(http_query_worker, NULL, QUERY_THREAD_POOL_SIZE, FALSE, NULL);

  GMainLoop *loop = g_main_loop_new (NULL, FALSE);
  GIOChannel *listener = g_io_channel_unix_new (srv);
  g_io_add_watch(listener, G_IO_IN, accept_fn, NULL);

  g_main_loop_run(loop);
}

static int create_child(int srv, char *kb_name, char *password)
{
  pid_t pid = fork();

  if (pid == -1) {
    fs_error(LOG_ERR, "fork: %s", strerror(errno));
    exit(5);
  } else if (pid > 0) {
    /* parent process */
    return pid;
  } else {
    /* child process */
    child(srv, kb_name, password);
    exit(0);
  }
}

static void child_exited(pid_t pid, gint status)
{
  if (WIFEXITED(status)) {
    int code = WEXITSTATUS(status);
    fs_error((code == 0) ? LOG_INFO : LOG_CRIT,
             "child %d exited with return code %d", pid, code);
  } else if (WIFSIGNALED(status)) {
    int code = WTERMSIG(status);
    fs_error((code == SIGTERM || code == SIGKILL) ? LOG_INFO : LOG_CRIT,
             "child %d terminated by signal %d", pid, code);
  } else if (WIFSTOPPED(status)) {
    fs_error(LOG_ERR, "child %d stopped by signal %d", pid, WSTOPSIG(status));
  } else {
    fs_error(LOG_CRIT, "child %d was terminated for unknown reasons", pid);
  }
}

int main(int argc, char *argv[])
{
  int daemonize = 1;
  char *password = fsp_argv_password(&argc, argv);
  char *kb_name = NULL;

  const char *host = NULL;
  const char *port = "8080";

  int o;
  while ((o = getopt(argc, argv, "DH:p:Uds:")) != -1) {
    switch (o) {
      case 'D':
        daemonize = 0;
        break;
      case 'H':
        host = optarg;
        break;
      case 'p':
        port = optarg;
        break;
      case 'U':
	unsafe = 1;
	break;
      case 'd':
	default_graph = 1;
	break;
      case 's':
	soft_limit = atoi(optarg);
	break;
    }
  }

  if (optind >= argc) {
    fprintf(stderr, "%s revision %s\n", argv[0], GIT_REV);
    fprintf(stderr, "Usage: %s [-D] [-H host] [-p port] [-U] [-s limit] <kbname>\n", basename(argv[0]));
    fprintf(stderr, "       -H   specify host to listen on\n");
    fprintf(stderr, "       -p   specify port to listen on\n");
    fprintf(stderr, "       -D   do not daemonise\n");
    fprintf(stderr, "       -U   enable unsafe operations (eg. LOAD)\n");
    fprintf(stderr, "       -d   enable SPARQL default graph support\n");
    fprintf(stderr, "       -s   default soft limit (-1 to disable)\n");

    return 1;
  }

  fsp_syslog_enable();
  kb_name = argv[optind];

  int srv = server_setup(daemonize, host, port);
  if (srv < 0) {
    return 2;
  }
  if (unsafe) {
    fs_error(LOG_INFO, "unsafe operations enabled");
  }

  pid_t cpid, wpid;
  do {
    int status;
    cpid = create_child(srv, kb_name, password);
    sleep(10); /* don't respawn faster than every ten seconds */
    wpid = waitpid(cpid, &status, 0);
    child_exited(wpid, status);
  } while(1);

  return 0;
}
