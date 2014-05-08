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
#include "4s-store-root.h"

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
#include <netinet/in.h>
#include <sys/stat.h>

static char *global_kb_name = NULL;
static float global_disk_limit = 0.0f;
static FILE *global_ri_file = NULL;

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
	case FS_GET_UUID:
	  reply = handle(backend->get_uuid, be, segment, length, content);
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

/* Store runtime information (pid+port) in locked file */
static int init_runtime_info(const char *kb_name, const char *cport)
{
    char *path;
    int len, fd, rv;
    struct flock ri_lock;

    /* alloc mem for string path to runtime.info */
    len = (strlen(fs_get_ri_file_format())-2) + strlen(kb_name) + 1;
    path = (char *)malloc(len * sizeof(char));
    if (path == NULL) {
        kb_error(LOG_CRIT, "failed to malloc %d bytes", len);
        return -1;
    }

    /* generate full path to runtime.info */
    rv = sprintf(path, fs_get_ri_file_format(), kb_name);
    if (rv < 0) {
        kb_error(LOG_ERR, "sprintf failed to write %d bytes", len);
        free(path);
        return -1;
    }

    /* Open runtime.info with mode 0644.
     * Use global so that file is locked for the lifetime of this process. */
    umask(022);
    global_ri_file = fopen(path, "w");
    if (global_ri_file == NULL) {
        kb_error(LOG_ERR, "failed to open '%s' for writing: %s",
                 path, strerror(errno));
        free(path);
        return -1;
    }

    /* Get integer file descriptor for use by fcntl */
    fd = fileno(global_ri_file);
    if (fd == -1) {
        kb_error(LOG_ERR, "failed to get file descriptor: %s", strerror(errno));
        free(path);
        return -1;
    }

    /* We want to get a write lock on the entire file */
    ri_lock.l_type = F_WRLCK;    /* write lock */
    ri_lock.l_whence = SEEK_SET; /* l_start begins at start of file */
    ri_lock.l_start = 0;         /* offset from whence */
    ri_lock.l_len = 0;           /* until EOF */

    /* Check whether file is currently locked.
     * This *should* be impossible, locks on metadata.nt mean that we should
     * never get here if a kb backend is running, but can't hurt to check. */
    rv = fcntl(fd, F_GETLK, &ri_lock);
    if (rv == -1) {
        kb_error(LOG_ERR, "failed to get lock information on '%s'", path);
        fclose(global_ri_file);
        free(path);
        return -1;
    }

    if (ri_lock.l_type == F_UNLCK) {
        /* file is unlocked, should always be the case */
        ri_lock.l_type = F_WRLCK;
        ri_lock.l_whence = SEEK_SET;
        ri_lock.l_start = 0;
        ri_lock.l_len = 0;

        /* get non-blocking write lock */
        rv = fcntl(fd, F_SETLK, &ri_lock);
        if (rv == -1) {
            kb_error(LOG_ERR, "failed to get lock on '%s'", path);
            fclose(global_ri_file);
            free(path);
            return -1;
        }
    }
    else {
        /* should never get here */
        kb_error(LOG_ERR, "file '%s' already locked with %d by pid %d",
                 path, ri_lock.l_type, ri_lock.l_pid);
        fclose(global_ri_file);
        free(path);
        return -1;
    }

    /* write port/pid to file: "%s %s",pid,port */
    rv = fprintf(global_ri_file, "%d %s", getpid(), cport);
    if (rv < 0) {
        kb_error(LOG_ERR, "failed to write to file '%s'", path);
        ri_lock.l_type = F_UNLCK;
        ri_lock.l_whence = SEEK_SET;
        ri_lock.l_start = 0;
        ri_lock.l_len = 0;
        fcntl(fd, F_SETLK, &ri_lock); /* clear lock */
        fclose(global_ri_file);
        free(path);
        return -1;
    }

    rv = fflush(global_ri_file);
    if (rv == EOF) {
        kb_error(LOG_ERR, "failed to flush file '%s'", path);
        ri_lock.l_type = F_UNLCK;
        ri_lock.l_whence = SEEK_SET;
        ri_lock.l_start = 0;
        ri_lock.l_len = 0;
        fcntl(fd, F_SETLK, &ri_lock); /* clear lock */
        fclose(global_ri_file);
        free(path);
        return -1;
    }

    kb_error(LOG_INFO, "runtime information file created");

    free(path);
    return 0;
}

#if !defined(MAXSOCK)
# define MAXSOCK 16
#endif

void fsp_serve (const char *kb_name, fsp_backend *backend, int daemon, float disk_limit)
{
    struct addrinfo hints, *info0, *info;
    uint16_t port = FS_DEFAULT_PORT;
    char cport[6];
    int sock[MAXSOCK];
    int nsock = 0;
    int err, on=1, off=0, i;

    /* we need access to these elsewhere */
    global_kb_name = (char *)kb_name;
    global_disk_limit = disk_limit;

    if (!backend->open) {
	/* no open function defined, we will eventually fail anyway, so give up early */
	return;
    }

    fs_backend *original = NULL;
    original = backend->open(kb_name, FS_BACKEND_NO_OPEN);
    if (!original)
	return;

    /* don't set any PF_INET or INET6 hints here */
    default_hints(&hints);
    hints.ai_flags = AI_PASSIVE;

    do {
	snprintf(cport, sizeof(cport), "%u", port);

	if ((err = getaddrinfo(NULL, cport, &hints, &info0))) {
	    kb_error(LOG_ERR, "getaddrinfo failed: %s", gai_strerror(err));
	    return;
	}

	/* keep track of the first addrinfo so we can free the whole chain later */
	info = info0;
	/* iterate through the list of possible addresses. on Linux we only get one: the
	   'dual stack' or ipv4 address. on some BSDs we will see two or more; often separate
	   INET6 and INET addrinfos. we want to listen on all of them. */
	do {
	    sock[nsock] = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
	    if (sock[nsock] < 0)
		continue;
#if defined(IPV6_V6ONLY)
	    if (info->ai_family == AF_INET6)
		/* don't check the return value -- some platforms (cough, OpenBSD, cough)
		have IPV6_V6ONLY defined, but refuse to let it be turned off */
		setsockopt(sock[nsock], IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
#endif
	    if (setsockopt(sock[nsock], SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1)
		kb_error(LOG_ERR, "setsockopt(SO_REUSEADDR) failed, continuing");
	    if (bind(sock[nsock], info->ai_addr, info->ai_addrlen) < 0) {
		err = errno;
		close(sock[nsock]);
		continue;
	    }
	    if (listen(sock[nsock], 64) < 0) {
		err = errno;
		close(sock[nsock]);
		continue;
	    }
	    ++nsock;
	} while ((info = info->ai_next) && nsock < MAXSOCK);

	freeaddrinfo(info0);

	/* if we go around again, try the next port */
	if (nsock == 0) ++port;
    } while (nsock == 0 && port < 65535);

    if (nsock == 0) {
    	kb_error(LOG_ERR, "fsp_serve failed to get a valid listening socket: %s", strerror(err));
    	return;
    }

    /* preload toplevel indexes */
    fs_backend *be = backend->open(kb_name, FS_BACKEND_PRELOAD);
    if (!be) {
	kb_error(LOG_CRIT, "failed to open backend");
	return;
    }

    GMainLoop *loop = g_main_loop_new(NULL, FALSE);
    fsp_mdns_setup_backend(port, kb_name, backend->segment_count(be));

    backend->close(be);

    if (daemon) {
	daemonize();
    }

    /* set up pid/port lockfile */
    err = init_runtime_info(kb_name, cport);
    if (err < 0) {
	/* error already logged so just return */
	return;
    }

    signal_actions();
    fs_error(LOG_INFO, "4store backend %s for kb %s on port %s (%d fds)", FS_BACKEND_VER, kb_name, cport, nsock);

    for (i = 0; i < nsock; ++i) {
	GIOChannel *listener = g_io_channel_unix_new(sock[i]);
	g_io_add_watch(listener, G_IO_IN, accept_fn, backend);
    }

    g_main_loop_run(loop);

    return;
}
