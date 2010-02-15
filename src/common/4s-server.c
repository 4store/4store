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

#include "4s-internals.h"
#include "error.h"
#include "params.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <syslog.h>
#include <netdb.h>
#include <glib.h>

static char *global_kb_name = NULL;
static float global_disk_limit = 0.0f;

#define kb_error(s, f...) (fs_error_intl(s, __FILE__, __LINE__, global_kb_name, f))

static unsigned char * fsp_handle_no_op (fs_segment segment,
                                         unsigned int length,
                                         unsigned char *content)
{
  unsigned char *reply = message_new(FS_DONE_OK, segment, 0);
  return reply;
}

static unsigned char * handle_or_fail(const char *name,
                                      fsp_backend_fn handle_fn,
                                      fs_backend *be,
                                      fs_segment segment,
                                      unsigned int length,
                                      unsigned char *content)
{
  if (handle_fn) {
    return handle_fn(be, segment, length, content);
  } else {
    kb_error(LOG_ERR, "no implementation for %s", name);
    return fsp_error_new(segment, "not implemented");
  }
}

#define handle(fn, be, segment, length, content) \
         handle_or_fail(#fn, fn, be, segment, length, content)

static void child (int conn, fsp_backend *backend, fs_backend *be)
{
  int auth = 0;

  while (1) {
    fs_segment segment;
    unsigned int length;
    unsigned char *msg = message_recv(conn, &segment, &length);
    unsigned char *reply = NULL;
    unsigned char *content = msg + FS_HEADER;

    if (!msg) {
      /* if the connection is in fact closed, this won't matter,
         but otherwise this error might help */
      reply = fsp_error_new(segment, "protocol mismatch");
      unsigned int* const l = (unsigned int *) (reply + 4);
      unsigned int length = *l;
      if (write(conn, reply, FS_HEADER + length) != (FS_HEADER+length)) {
        fs_error(LOG_ERR, "write failed: %s", strerror(errno));
      }
      break;
    }

    if (auth) {
      switch (msg[3]) {
        case FS_NO_OP:
          reply = fsp_handle_no_op(segment, length, content);
          break;
        case FS_RESOLVE:
          reply = handle(backend->resolve, be, segment, length, content);
          break;
        case FS_BIND:
          reply = handle(backend->bind, be, segment, length, content);
          break;
        case FS_PRICE_BIND:
          reply = handle(backend->price, be, segment, length, content);
          break;
        case FS_DELETE_MODEL:
          reply = handle(backend->delete_models, be, segment, length, content);
          break;
        case FS_INSERT_RESOURCE:
          reply = handle(backend->insert_resource, be, segment, length, content);
          break;
        case FS_SEGMENTS:
          reply = handle(backend->segments, be, segment, length, content);
          break;
        case FS_COMMIT_RESOURCE:
          reply = handle(backend->commit_resource, be, segment, length, content);
          break;
        case FS_START_IMPORT:
          reply = handle(backend->start_import, be, segment, length, content);
          break;
        case FS_STOP_IMPORT:
          reply = handle(backend->stop_import, be, segment, length, content);
          break;
        case FS_GET_SIZE:
          reply = handle(backend->get_data_size, be, segment, length, content);
          break;
        case FS_GET_IMPORT_TIMES:
          reply = handle(backend->get_import_times, be, segment, length, content);
          break;
        case FS_INSERT_QUAD:
          reply = handle(backend->insert_quad, be, segment, length, content);
          break;
        case FS_COMMIT_QUAD:
          reply = handle(backend->commit_quad, be, segment, length, content);
          break;
        case FS_GET_QUERY_TIMES:
          reply = handle(backend->get_query_times, be, segment, length, content);
          break;
        case FS_BIND_LIMIT:
          reply = handle(backend->bind_limit, be, segment, length, content);
          break;
        case FS_BNODE_ALLOC:
          reply = handle(backend->bnode_alloc, be, segment, length, content);
          break;
        case FS_RESOLVE_ATTR:
          reply = handle(backend->resolve_attr, be, segment, length, content);
          break;
        case FS_DELETE_MODELS:
          reply = handle(backend->delete_models, be, segment, length, content);
          break;
        case FS_NEW_MODELS:
          reply = handle(backend->new_models, be, segment, length, content);
          break;
        case FS_BIND_FIRST:
          reply = handle(backend->bind_first, be, segment, length, content);
          break;
        case FS_BIND_NEXT:
          reply = handle(backend->bind_next, be, segment, length, content);
          break;
        case FS_BIND_DONE:
          reply = handle(backend->bind_done, be, segment, length, content);
          break;
        case FS_TRANSACTION:
          reply = handle(backend->transaction, be, segment, length, content);
          break;
        case FS_NODE_SEGMENTS:
          reply = handle(backend->node_segments, be, segment, length, content);
          break;
        case FS_REVERSE_BIND:
          reply = handle(backend->reverse_bind, be, segment, length, content);
          break;
        case FS_LOCK:
          reply = handle(backend->lock, be, segment, length, content);
          break;
        case FS_UNLOCK:
          reply = handle(backend->unlock, be, segment, length, content);
          break;
        case FS_GET_SIZE_REVERSE:
          reply = handle(backend->get_size_reverse, be, segment, length, content);
          break;
        case FS_GET_QUAD_FREQ:
          reply = handle(backend->get_quad_freq, be, segment, length, content);
          break;
        case FS_CHOOSE_SEGMENT:
          reply = handle(backend->choose_segment, be, segment, length, content);
          break;
	case FS_DELETE_QUADS:
	  reply = handle(backend->delete_quads, be, segment, length, content);
	  break;
        default:
          kb_error(LOG_WARNING, "unexpected message type (%d)", msg[3]);
          reply = fsp_error_new(segment, "unexpected message type");
          break;
      }
    } else if (msg[3] == FS_AUTH) {
      if (backend->auth) {
        reply = backend->auth(be, segment, length, content);
      } else {
        reply = message_new(FS_DONE_OK, segment, 0);
      }
      if (reply[3] == FS_DONE_OK) auth = 1;
    } else  {
      reply = fsp_error_new(segment, "authenticate before continuing");
    }

    if (reply) {
      unsigned int* const l = (unsigned int *) (reply + 4);
      unsigned int length = *l;
      if (write(conn, reply, FS_HEADER + length) <= 0) {
        kb_error(LOG_WARNING, "write reply failed");
      }
      free(reply);
    }
    free(msg);
  }
}

volatile sig_atomic_t fatal_error_in_progress = 0;

static void do_sigmisc(int sig)
{
  if (fatal_error_in_progress) raise (sig);
  fatal_error_in_progress = 1;
     
  signal (sig, SIG_DFL);
  kb_error(LOG_INFO, "signal %s received", strsignal(sig));
  raise (sig);
}

static void signal_actions(void)
{
  struct sigaction misc_action = {
    .sa_handler = &do_sigmisc,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&misc_action.sa_mask);

  struct sigaction ignore_action = {
    .sa_handler = SIG_IGN,
    .sa_flags = (SA_RESTART),
  };
  sigfillset(&misc_action.sa_mask);

  sigaction(SIGHUP, &misc_action, NULL); /* HUP */
  sigaction(SIGINT, &misc_action, NULL); /* ^C */
  sigaction(SIGTERM, &misc_action, NULL); /* kill */
  sigaction(SIGUSR2, &ignore_action, NULL); /* file reread signal */
}

static void daemonize (void)
{
  /* fork once, we don't want to be process leader */
  switch(fork()) {
    case 0:
      break;
    case -1:
      kb_error(LOG_ERR, "fork() error starting daemon: %s", strerror(errno));
      exit(1);
    default:
      _exit(0);
  }

  /* new session / process group */
  if (setsid() == -1) {
    kb_error(LOG_ERR, "setsid() failed starting daemon: %s", strerror(errno));
    exit(1);
  }

  /* fork again, separating ourselves from our parent permanently */

  switch(fork()) {
    case 0:
      break;
    case -1:
      kb_error(LOG_ERR, "fork() error starting daemon: %s", strerror(errno));
      exit(1);
    default:
      _exit(0);
  }

  /* close stdin, stdout, stderr */
  close(0); close(1); close(2);

  /* Avahi sucks, we need an open fd or it gets confused -sigh */
  if (open("/dev/null", 0) == -1) {
    kb_error(LOG_ERR, "couldn't open /dev/null: %s", strerror(errno));
  }
  /* use up some more fds as a precaution against printf() getting
     written to the wire */
  open("/dev/null", 0);
  open("/dev/null", 0);

  /* move somewhere safe and known */
  if (chdir("/")) {
    kb_error(LOG_ERR, "chdir failed: %s", strerror(errno));
  }
}

static void child_exited(GPid pid, gint status, gpointer data)
{
  if (WIFEXITED(status)) {
    if (WEXITSTATUS(status)) {
      int code = WEXITSTATUS(status);
      kb_error((code == 0) ? LOG_INFO : LOG_CRIT,
               "child %d exited with return code %d", pid, code);
    }
  } else if (WIFSIGNALED(status)) {
    int code = WTERMSIG(status);
    kb_error((code == SIGTERM || code == SIGKILL) ? LOG_INFO : LOG_CRIT,
             "child %d terminated by signal %d", pid, code);
  } else if (WIFSTOPPED(status)) {
    kb_error(LOG_ERR, "child %d stopped by signal %d", pid, WSTOPSIG(status));
  } else {
    kb_error(LOG_CRIT, "child %d was terminated for unknown reasons", pid);
  }

}

gboolean accept_fn (GIOChannel *source, GIOCondition condition, gpointer data)
{
  fsp_backend *backend = (fsp_backend *) data;
  int conn = accept(g_io_channel_unix_get_fd(source), NULL, NULL);

  if (conn == -1) {
    if (errno != EINTR) kb_error(LOG_ERR, "accept: %s", strerror(errno));
    return TRUE; /* try again */
  }

  pid_t pid = fork();
  if (pid == -1) {
    kb_error(LOG_ERR, "fork: %s", strerror(errno));
    close(conn);
  } else if (pid > 0) {
    /* parent process */
    g_child_watch_add(pid, child_exited, data);
    close(conn);
  } else {
    /* child process */
    fs_backend *be = backend->open(global_kb_name, 0);
    if (be) {
      fs_backend_set_min_free(be, global_disk_limit);
      child(conn, backend, be);
      backend->close(be);
    }
    close(conn);
    exit(0);
  }

  return TRUE;
}

void fsp_serve (const char *kb_name, fsp_backend *backend, int daemon, float disk_limit)
{
  struct addrinfo hints, *info;
  uint16_t port = FS_DEFAULT_PORT;
  fs_backend *original = NULL;
  char cport[6];
  int on = 1, off = 0, srv, err;
  
  default_hints(&hints);
  /* what we'll do is set IPv6 here and turn off IPV6-only on hosts where it's the default */
  hints.ai_family = AF_INET6;
  hints.ai_flags = AI_PASSIVE;

  /* we need access to these elsewhere */
  global_kb_name = (char *)kb_name;
  global_disk_limit = disk_limit;

  if (backend->open) {
    original = backend->open(kb_name, FS_BACKEND_NO_OPEN);
    if (!original) return;
  }

  do {
    sprintf(cport, "%u", port);
    if ((err = getaddrinfo(NULL, cport, &hints, &info))) {
      kb_error(LOG_ERR, "getaddrinfo failed: %s", gai_strerror(err));
      return;
    }
    srv = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (srv < 0) {
      if (errno == EAFNOSUPPORT) {
        kb_error(LOG_INFO, "couldn't get IPv6 dual stack, trying IPv4-only");
        hints.ai_family = AF_INET;
        continue;
      }
      kb_error(LOG_ERR, "socket failed: %s", strerror(errno));
      freeaddrinfo(info);
      return;
    }

    if (hints.ai_family != AF_INET && setsockopt(srv, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off)) == -1) {
      kb_error(LOG_WARNING, "setsockopt IPV6_V6ONLY OFF failed");
    }
    if (setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
      kb_error(LOG_WARNING, "setsockopt SO_REUSEADDR failed");
    }

    if (bind(srv, info->ai_addr, info->ai_addrlen) < 0) {
      if (errno == EADDRINUSE) {
        close(srv);
        freeaddrinfo(info);
        ++port; /* try another port */
        continue;
      } else {
        kb_error(LOG_ERR, "server socket bind failed: %s", strerror(errno));
        freeaddrinfo(info);
        return;
      }
    }

    break;
  } while (1);

  freeaddrinfo(info);

  if (listen(srv, 64) < 0) {
    kb_error(LOG_ERR, "listen failed");
    return;
  }

  /* preload toplevel indexes */
  fs_backend *be = backend->open(kb_name, FS_BACKEND_PRELOAD);
  if (!be) {
    kb_error(LOG_CRIT, "failed to open backend");
    return;
  }

  GMainLoop *loop = g_main_loop_new (NULL, FALSE);
  fsp_mdns_setup_backend (port, kb_name, backend->segment_count(be));

  backend->close(be);

  if (daemon) {
    daemonize();
  }

  signal_actions();
  fs_error(LOG_INFO, "4store backend %s for kb %s on port %s", FS_BACKEND_VER, kb_name, cport);

  GIOChannel *listener = g_io_channel_unix_new (srv);
  g_io_add_watch(listener, G_IO_IN, accept_fn, backend);

  g_main_loop_run(loop);

  return;
}
