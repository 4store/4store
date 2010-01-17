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

#include "4s-internals.h"

#include "datatypes.h"
#include "params.h"
#include "error.h"
#include "hash.h"
#include "md5.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>

#include <glib.h>

static const char *invalid_response(unsigned char *msg)
{
  if (!msg) {
    return "no reply";
  } else if (msg[3] == FS_ERROR) {
    return (char *) msg + FS_HEADER;
  } else {
    return "incorrect reply type";
  }
}

static char fsp_pw[21]; /* erased once the password is used */

char *fsp_argv_password (int *argc, char *argv[]) {
  char *password = NULL, *filename = NULL;
  int j, k, count = *argc;

  for (j = 0; j + 1 < count; ++j) {
    if (!strcmp(argv[j], "--password")) {
      password = argv[j+1];
      break;
    }
    if (!strcmp(argv[j], "--passwordfile")) {
      filename = argv[j+1];
      break;
    }
  }

  if (filename) {
    FILE *file = fopen(filename, "r");
    if (file) {
      if (fscanf(file, "%20s", fsp_pw) > 0) {
        password = fsp_pw;
      }
      fclose(file);
    } else {
      fs_error(LOG_ERR, "Can't open password file “%s”: %s", filename, strerror(errno));
      exit(1);
    }
  }

  if (!password) return NULL;

  for (k = j; k + 2 < count; ++k) {
    argv[k] = argv[k+2];
  }
  argv[k] = NULL;
  *argc = count - 2;

  return password;
}

static int check_message(fsp_link *link, int sock, const char *message)
{
  int ret = 0;
  fs_segment segment;
  unsigned int ignored;
  unsigned char *in = message_recv(sock, &segment, &ignored);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_DONE_OK) {
    link_error(LOG_ERR, message, segment, invalid_response(in));
    ret = 1;
  }
  free(in);

  return ret;
}

static void features (fsp_link *link, const char *string)
{
  if (link->features) {
    if (strcmp(link->features, string)) {
      link_error(LOG_WARNING, "features inconsistent between segments");
    }
  } else {
    link->features = strdup(string);
  }
}

static int fsp_open_socket (fsp_link *link, const char *node, uint16_t port)
{
  struct addrinfo hints, *info, *list;
  char cport[6];
  int sock = -1, err;

  sprintf(cport, "%u", port);
  default_hints(&hints);
  if ((err = getaddrinfo(node, cport, &hints, &info))) {
    link_error(LOG_ERR, "getaddrinfo failed for “%s” with error: %s", node, gai_strerror(err));
    return -1;
  }

  for (list= info; list; list = list->ai_next) {
    sock = socket(list->ai_family, list->ai_socktype, list->ai_protocol);
    if (sock < 0) {
      link_error(LOG_ERR, "socket create failed for “%s”", node);
      continue;
    }

    if (connect(sock, list->ai_addr, list->ai_addrlen) < 0) {
      link_error(LOG_ERR, "connect to node “%s” failed", node);
      close(sock);
      sock = -1;
      continue;
    }

    break;
  }
  freeaddrinfo(info);

  if (sock == -1) return -1;

  err = fsp_ver_fixup(link, sock);
  if (err == -1) return -1;
  if (err == 1) {
    /* try again, shouldn't loop */
    close(sock);
    return fsp_open_socket (link, node, port);
  }

  unsigned int length = 16 + strlen(link->kb_name);
  unsigned char *out = message_new(FS_AUTH, 0, length);
  memcpy(out + FS_HEADER, link->hash, 16);
  memcpy(out + FS_HEADER + 16, link->kb_name, strlen(link->kb_name));
  if (write(sock, out, FS_HEADER + length) != (FS_HEADER + length)) {
    link_error(LOG_ERR, "write failed: %s", strerror(errno));
    close(sock);
    sock = -1;
  }
  free(out);

  fs_segment segment;
  unsigned char *in = message_recv(sock, &segment, &length);

  if (!in || in[3] != FS_DONE_OK) {
    link_error(LOG_ERR, "auth failed: %s", invalid_response(in));
    close(sock);
    sock = -1;
  }
  features(link, (char *) in + FS_HEADER);
  free(in);

  return sock;
}

static unsigned char *segment_list(fsp_link *link, int server, int sock)
{
  unsigned char expected = FS_NODE_SEGMENT_LIST;
  unsigned char *list;
  fs_segment segments;
  unsigned int length;
  unsigned char *in = message_recv(sock, &segments, &length);

  if (in && in[3] == FS_ERROR) {
    free(in);
    unsigned char *out = message_new(FS_SEGMENTS, 0, 0);
    if (write(sock, out, FS_HEADER) != FS_HEADER) {
      link_error(LOG_ERR, "write failed: %s", strerror(errno));
      free(out);
      return NULL;
    }
    expected = FS_SEGMENT_LIST;
    free(out);
    in = message_recv(sock, &segments, &length);
  }

  if (!in || in[3] != expected) {
    link_error(LOG_ERR, "segment list failed for %s port %d: %s",
               link->addrs[server], link->ports[server], invalid_response(in));
    free(in);
    return NULL;
  }

  if (link->segments && link->segments != segments) {
    link_error(LOG_WARNING, "%s port %d offers %d segments not %d",
               link->addrs[server], link->ports[server], segments, link->segments);
    free(in);
    return NULL;
  } else if (!link->segments) {
    link->segments = segments;
  }

  if (expected == FS_NODE_SEGMENT_LIST) {
    if (length != link->segments) {
      link_error(LOG_WARNING, "segment list wrong size for %s port %d",
               link->addrs[server], link->ports[server]);
      free(in);
      return NULL;
    }
    list = in;
  } else {
    list = calloc(link->segments + FS_HEADER, 1);
    for (int k = 0; k < length / 4; ++k) {
      fs_segment seg;
      memcpy(&seg, in + FS_HEADER + 4 * k, sizeof(seg));
      list[FS_HEADER + seg] = 'p';
    }
  }

  return list;
}

/* returns zero when things are fine
   else number of missing segments or -1 */

static int check_segments(fsp_link *link, int readonly)
{
  if (link->segments == 0) return -1;

  int count = 0;

  for (int k = 0; k < link->segments; ++k) {
    if (link->socks1[k] != -1) {
      count++;
    } else if (readonly && link->socks2[k] != -1) {
      count++;
    }
  }

  if (count > 0 && count < link->segments) {
    link_error(LOG_INFO, "waiting for more backend nodes");
    fsp_mdns_retry_frontend(link, 180000); /* up to three minutes pause */

    /* re-assess count */
    count = 0;
    for (int k = 0; k < link->segments; ++k) {
      if (link->socks1[k] != -1) {
        count++;
      } else if (readonly && link->socks2[k] != -1) {
        count++;
      }
    }
  }

  if (count < link->segments) {
    GString *buf = g_string_new ("not enough primary nodes, segments ");
    for (int k = 0; k < link->segments; ++k) {
      if (link->socks1[k] == -1) {
        g_string_append_printf (buf, "%d, ", k);
      }
    }
    g_string_append(buf, "missing");
    link_error(LOG_CRIT, "%s", buf->str);
    g_string_free(buf, TRUE);
  }

  return link->segments - count;
}

static int choose_segment (fsp_link *link, int sock, int server, fs_segment segment)
{
  unsigned char *out = message_new(FS_CHOOSE_SEGMENT, segment, 0);
  if (write(sock, out, FS_HEADER) != FS_HEADER) {
    free(out);
    return 1;
  }
  free(out);

  unsigned int ignored;

  struct pollfd pollfds[1] = { { .fd = sock, .events = POLLIN } };
  int timeout = 2500; /* 2.5 seconds */
  while (poll(pollfds, 1, timeout) == 0) {
    timeout += 2500; /* wait longer each time */
    link_error(LOG_INFO, "waiting for lock on segment %d on %s port %d",
               segment, link->addrs[server], link->ports[server]);
  }

  int ret = 0;
  unsigned char *in = message_recv(sock, &segment, &ignored);
  if (!in || in[3] != FS_DONE_OK) {
    link_error(LOG_ERR, "choose_segment failed(%d): %s", segment, invalid_response(in));
    ret = 1;
  }
  free(in);

  return ret;
}

/* returns count of new primary segments or zero */
int fsp_add_backend (fsp_link *link, const char *addr, uint16_t port, int segments)
{
  if (link->segments && link->segments != segments) {
    return 0;
  }

  int count = 0;

  int sock = fsp_open_socket(link, addr, port);
  if (sock == -1) return 0;

  unsigned char *out = message_new(FS_NODE_SEGMENTS, 0, 0);
  if (write(sock, out, FS_HEADER) != FS_HEADER) {
    link_error(LOG_ERR, "write failed: %s", strerror(errno));
    free(out);
    return 0;
  }
  free(out);

  /* post increment */
  int server = link->servers++;
  link->addrs[server] = g_strdup(addr);
  link->ports[server] = port;
  link->segments = segments; /* still zero despite this for non-Avahi case */

  unsigned char *in = segment_list(link, server, sock);
  /* now we definitely know # of seg */

  if (!in) return 0;

  for (fs_segment seg = 0; seg < link->segments; ++seg) {
    unsigned char status = in[FS_HEADER + seg];
    switch (status) {
      case 'p':
        if (link->socks1[seg] != -1) {
          continue;
        } else if (sock == -1) {
          sock = fsp_open_socket(link, link->addrs[server], link->ports[server]);
        }
        
        if (choose_segment(link, sock, server, seg)) break; /* something went wrong */
        link->groups[seg]= server;
        link->socks[seg] = link->socks1[seg] = sock;
        sock = -1; /* used this one */
        count++;
        break;
      case 'm':
        if (link->socks2[seg] != -1) {
          continue;
        } else if (sock == -1) {
          sock = fsp_open_socket(link, link->addrs[server], link->ports[server]);
        }
        if (choose_segment(link, sock, server, seg)) break; /* something went wrong */
        link->socks2[seg] = sock;
        sock = -1; /* used this one */
        break;
      case '\0':
        /* not present */
        break;
      default:
        link_error(LOG_ERR, "unknown status ignored for segment %d on %s port %d",
                 seg, link->addrs[server], link->ports[server]);
        break;
    }
  }

  free(in);
  if (sock != -1) { /* doesn't have any segments we wanted ? */
    close(sock);
  }

  return count;
}

static void fsp_write_primary(fsp_link* link, const void *data, size_t size)
{
  unsigned int * const s = (unsigned int *) (data + 8);
  fs_segment segment = *s;

#ifdef FS_PROFILE_WRITE
  struct timeval start, stop;
  
  gettimeofday(&start, NULL);
#endif
  g_static_mutex_lock(&link->mutex[segment]);
  ssize_t count = write(link->socks1[segment], data, FS_HEADER + size);
  if (count == -1) {
    link_error(LOG_ERR, "write error for primary segment %d: %s", segment, strerror(errno));
  }
#ifdef FS_PROFILE_WRITE
  gettimeofday(&stop, NULL);

  link->tics[segment] += (stop.tv_sec - start.tv_sec) * 1000;
  link->tics[segment] += (stop.tv_usec - start.tv_usec) / 1000;
#endif
}

static int fsp_write(fsp_link* link, const void *data, size_t size)
{
  unsigned int * const s = (unsigned int *) (data + 8);
  fs_segment segment = *s;

  int sock = link->socks[segment];

#ifdef FS_PROFILE_WRITE
  struct timeval start, stop;
  
  gettimeofday(&start, NULL);
#endif
  g_static_mutex_lock(&link->mutex[segment]);
  ssize_t count = write(sock, data, FS_HEADER + size);
  while (count == -1) {
    link_error(LOG_ERR, "write error for segment %d: %s", segment, strerror(errno));
    if (sock == link->socks1[segment] && link->socks2[segment] != -1) {
      close(sock);
      link->socks1[segment] = -1;
      sock = link->socks[segment] = link->socks2[segment];
      link_error(LOG_WARNING, "switching to backup segment %d for queries", segment);
    } else if (sock == link->socks2[segment] && link->socks1[segment] != -1) {
      close(sock);
      link->socks2[segment] = -1;
      sock = link->socks[segment] = link->socks1[segment];
      link_error(LOG_WARNING, "switching back to primary segment %d for queries", segment);
    } else {
      link_error(LOG_CRIT, "segment %d failed with no backup", segment);
      close(sock);
      sock = -1;
      break;
    }
    count= write(sock, data, FS_HEADER + size);
  }
#ifdef FS_PROFILE_WRITE
  gettimeofday(&stop, NULL);

  link->tics[segment] += (stop.tv_sec - start.tv_sec) * 1000;
  link->tics[segment] += (stop.tv_usec - start.tv_usec) / 1000;
#endif

  return sock;
}

static void fsp_write_replica(fsp_link* link, const void *data, size_t size)
{
  unsigned int * const s = (unsigned int *) (data + 8);
  fs_segment segment = *s;

#ifdef FS_PROFILE_WRITE
  struct timeval start, stop;

  gettimeofday(&start, NULL);
#endif
  g_static_mutex_lock(&link->mutex[segment]);
  if (write(link->socks1[segment], data, FS_HEADER + size) == -1) {
    if (errno != EPIPE) {
      link_error(LOG_ERR, "write_replica(%d) failed: %s", segment, strerror(errno));
    }
  } else if (link->socks2[segment] != -1) {
    /* success and mirror exists so try to write to that too */
    if (write(link->socks2[segment], data, FS_HEADER + size) != (FS_HEADER+size)) {
      link_error(LOG_ERR, "write failed: %s", strerror(errno));
    }
  }
#ifdef FS_PROFILE_WRITE
  gettimeofday(&stop, NULL);

  link->tics[segment] += (stop.tv_sec - start.tv_sec) * 1000;
  link->tics[segment] += (stop.tv_usec - start.tv_usec) / 1000;
#endif
}

/* You must hold the relevent lock before calling this */
static void fsp_write_replica_locked(fsp_link* link, const void *data, size_t size)
{
  unsigned int * const s = (unsigned int *) (data + 8);
  fs_segment segment = *s;

#ifdef FS_PROFILE_WRITE
  struct timeval start, stop;

  gettimeofday(&start, NULL);
#endif
  /* a lock is already held */
  if (write(link->socks1[segment], data, FS_HEADER + size) == -1) {
    if (errno != EPIPE) {
      link_error(LOG_ERR, "write_replica(%d) failed: %s", segment, strerror(errno));
    }
  } else if (link->socks2[segment] != -1) {
    /* success and mirror exists so try to write to that too */
    if (write(link->socks2[segment], data, FS_HEADER + size) != (FS_HEADER+size)) {
      link_error(LOG_ERR, "write failed: %s", strerror(errno));
    }
  }
#ifdef FS_PROFILE_WRITE
  gettimeofday(&stop, NULL);

  link->tics[segment] += (stop.tv_sec - start.tv_sec) * 1000;
  link->tics[segment] += (stop.tv_usec - start.tv_usec) / 1000;
#endif
}

static unsigned char *message_recv_replica(fsp_link *link, fs_segment segment, unsigned int *length)
{
  unsigned int unused;

  if (link->socks2[segment] == -1) {
    return message_recv(link->socks1[segment], &unused, length);
  }

  unsigned int len1, len2;
  unsigned char *msg1, *msg2;

  msg1= message_recv(link->socks1[segment], &unused, &len1);
  msg2= message_recv(link->socks2[segment], &unused, &len2);

  if (!msg1 || msg1[3] == FS_ERROR) {
    *length = len1;
    free(msg2);
    return msg1;
  } else {
    *length = len2;
    free(msg1);
    return msg2;
  }
}

fsp_link* fsp_open_link (const char *name, char *password, int readonly)
{
  if (!name) {
    return NULL;
  }

  fsp_link *link = calloc(1, sizeof(fsp_link));

  link->kb_name = name;

  /* sockets start zero'd which is no good */

  for (fs_segment s = 0; s < FS_MAX_SEGMENTS; ++s) {
    link->groups[s] = link->socks[s] = link->socks1[s] = link->socks2[s] = -1;
    g_static_mutex_init(&link->mutex[s]);
  }

  if (password) {
    md5_state_t md5;
    char *pw = g_strdup_printf("%s:%s", name, password);

    md5_init(&md5);
    md5_append(&md5, (unsigned char *) pw, strlen(pw));
    md5_finish(&md5, link->hash); /* on-wire 16 byte MD5 auth */

    /* try somewhat to erase password string */
    for (int len = strlen(password), k = 0; k < len; ++k) {
      password[k] = '\0';
    }
    for (int len = strlen(pw), k = 0; k < len; ++k) {
      pw[k] = '\0';
    }
    g_free(pw);
  }

  fsp_mdns_setup_frontend(link);
  if (link->servers < 1) {
    free(link);
    link = NULL;
  }

  if (link && check_segments(link, readonly)) {
    free(link);
    link = NULL;
  }

  return link;
}

void fsp_close_link (fsp_link *link)
{
  fsp_mdns_cleanup_frontend(link);

  for (int k= 0; k < link->servers; ++k) {
    g_free((char *) link->addrs[k]);
  }
  for (int k= 0; k < link->segments; ++k) {
    close (link->socks1[k]);
    if (link->socks2[k] != -1) close (link->socks2[k]);
  }

  free(link);
}

int fsp_link_segments (fsp_link *link)
{
  return link->segments;
}

const char *fsp_link_features (fsp_link *link)
{
  return link->features;
}

long long *fsp_profile_write(fsp_link* link)
{
   return link->tics;
}

static int check_message_replica(fsp_link *link, fs_segment segment, const char *message)
{
  int ret = 0;
  unsigned int ignored;
  unsigned char *in = message_recv_replica(link, segment, &ignored);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_DONE_OK) {
    link_error(LOG_ERR, message, segment, invalid_response(in));
    ret = 1;
  }
  free(in);

  return ret;
}

int fsp_no_op (fsp_link* link,
               fs_segment segment)
{
  unsigned char *out = message_new(FS_NO_OP, segment, 0);
  int sock = fsp_write(link, out, 0);
  free(out);

  return check_message(link, sock, "no_op(%d) failed: %s");
}

int fsp_bind_limit (fsp_link *link,
                    fs_segment segment,
                    int flags,
                    fs_rid_vector *mrids,
                    fs_rid_vector *srids,
                    fs_rid_vector *prids,
                    fs_rid_vector *orids,
                    fs_rid_vector ***result,
                    int offset,
                    int limit)
{
  unsigned char *content;
  unsigned int length, value;
  int ret = 0;

  /* fill out */
  length = 32 +
         (mrids->length + srids->length + prids->length + orids->length ) * 8;

  unsigned char *out = message_new(FS_BIND_LIMIT, segment, length);
  content = out + FS_HEADER;

  memcpy(content, &flags, sizeof(flags));
  memcpy(content + 4, &offset, sizeof(offset));
  memcpy(content + 8, &limit, sizeof(limit));
  value = mrids->length * 8;
  memcpy(content + 12, &value, sizeof(value));
  value = srids->length * 8;
  memcpy(content + 16, &value, sizeof(value));
  value = prids->length * 8;
  memcpy(content + 20, &value, sizeof(value));
  value = orids->length * 8;
  memcpy(content + 24, &value, sizeof(value));
  content += 32;

  memcpy(content, mrids->data, mrids->length * 8);
  content += mrids->length * 8;
  memcpy(content, srids->data, srids->length * 8);
  content += srids->length * 8;
  memcpy(content, prids->data, prids->length * 8);
  content += prids->length * 8;
  memcpy(content, orids->data, orids->length * 8);
  content += orids->length * 8;
  
  int sock = fsp_write(link, out, length);
  free(out);

  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);
  content = in + FS_HEADER;

  if (!in) {
    link_error(LOG_ERR, "bind(%d) failed: no reply", segment);
    return 1;
  } else if (in[3] == FS_NO_MATCH) {
    free(in);
    *result = NULL;
    return 0;
  } else if (in[3] != FS_BIND_LIST) {
    link_error(LOG_ERR, "bind(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  int k, cols = 0;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    *result = calloc(1, sizeof(fs_rid_vector *));
  } else {
    int count = length / (8 * cols);
    if (count == limit) {
      (link->hit_limits)++;
    }

    *result = calloc(cols, sizeof(fs_rid_vector *));
    for (k = 0; k < cols; ++k) {
      fs_rid_vector *v = fs_rid_vector_new(count);
      (*result)[k] = v;
      memcpy(v->data, content, count * 8);
      content += count * 8;
    }
  }

  free(in);

  return ret;
}

int fsp_reverse_bind_all (fsp_link *link,
                          int flags,
                          fs_rid_vector *mrids,
                          fs_rid_vector *srids,
                          fs_rid_vector *prids,
                          fs_rid_vector *orids,
                          fs_rid_vector ***result,
                          int offset,
                          int limit)
{
  fs_segment segment;
  unsigned char *out, *content;
  unsigned int length, value;
  int sock[link->segments], ret = 0;

  /* fill out */
  length = 32 +
         (mrids->length + srids->length + prids->length + orids->length ) * 8;

  out = message_new(FS_REVERSE_BIND, 0, length);
  content = out + FS_HEADER;

  memcpy(content, &flags, sizeof(flags));
  memcpy(content + 4, &offset, sizeof(offset));
  memcpy(content + 8, &limit, sizeof(limit));
  value = mrids->length * 8;
  memcpy(content + 12, &value, sizeof(value));
  value = srids->length * 8;
  memcpy(content + 16, &value, sizeof(value));
  value = prids->length * 8;
  memcpy(content + 20, &value, sizeof(value));
  value = orids->length * 8;
  memcpy(content + 24, &value, sizeof(value));
  content += 32;

  memcpy(content, mrids->data, mrids->length * 8);
  content += mrids->length * 8;
  memcpy(content, srids->data, srids->length * 8);
  content += srids->length * 8;
  memcpy(content, prids->data, prids->length * 8);
  content += prids->length * 8;
  memcpy(content, orids->data, orids->length * 8);
  content += orids->length * 8;

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    sock[segment] = fsp_write(link, out, length);
  }

  fs_rid_vector **vectors;
  int matches = 0, cols = 0, k;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    vectors = calloc(1, sizeof(fs_rid_vector *));
  } else {
    vectors = calloc(cols, sizeof(fs_rid_vector *));
  }

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned char *in = message_recv(sock[segment], &segment, &length);
    g_static_mutex_unlock (&link->mutex[segment]);
    content = in + FS_HEADER;

    if (!in) {
      link_error(LOG_ERR, "reverse_bind(%d) failed: no reply", segment);
      ret++ ;
      continue;
    } else if (in[3] == FS_NO_MATCH) {
      free(in);
      continue;
    } else if (in[3] != FS_BIND_LIST) {
      link_error(LOG_ERR, "reverse_bind(%d) failed: %s", segment, invalid_response(in));
      free(in);
      ret++ ;
      continue;
    }

    if (cols == 0) {
      matches++;
    } else {
      int count = length / (8 * cols);
      if (count == limit) {
        (link->hit_limits)++;
      }
      for (k = 0; k < cols; ++k) {
        fs_rid_vector *v = fs_rid_vector_new(count);
        memcpy(v->data, content, count * 8);
	if (vectors[k]) {
          fs_rid_vector_append_vector(vectors[k], v);
          fs_rid_vector_free(v);
        } else {
          vectors[k] = v;
        }
        content += count * 8;
      }
    }

    free(in);
  }

  if (cols == 0 && matches == 0) {
    free(vectors);
    *result = NULL; /* if there are no results, there's no match */
  } else {
    *result = vectors;
  }

  free(out);

  return ret;
}

int fsp_bind_limit_all (fsp_link *link,
                        int flags,
                        fs_rid_vector *mrids,
                        fs_rid_vector *srids,
                        fs_rid_vector *prids,
                        fs_rid_vector *orids,
                        fs_rid_vector ***result,
                        int offset,
                        int limit)
{
  fs_segment segment;
  unsigned char *out, *content;
  unsigned int length, value;
  int sock[link->segments], ret = 0;

  /* fill out */
  length = 32 +
         (mrids->length + srids->length + prids->length + orids->length ) * 8;

  out = message_new(FS_BIND_LIMIT, 0, length);
  content = out + FS_HEADER;

  memcpy(content, &flags, sizeof(flags));
  memcpy(content + 4, &offset, sizeof(offset));
  memcpy(content + 8, &limit, sizeof(limit));
  value = mrids->length * 8;
  memcpy(content + 12, &value, sizeof(value));
  value = srids->length * 8;
  memcpy(content + 16, &value, sizeof(value));
  value = prids->length * 8;
  memcpy(content + 20, &value, sizeof(value));
  value = orids->length * 8;
  memcpy(content + 24, &value, sizeof(value));
  content += 32;

  memcpy(content, mrids->data, mrids->length * 8);
  content += mrids->length * 8;
  memcpy(content, srids->data, srids->length * 8);
  content += srids->length * 8;
  memcpy(content, prids->data, prids->length * 8);
  content += prids->length * 8;
  memcpy(content, orids->data, orids->length * 8);
  content += orids->length * 8;

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    sock[segment] = fsp_write(link, out, length);
  }

  fs_rid_vector **vectors;
  int matches = 0, cols = 0, k;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    vectors = calloc(1, sizeof(fs_rid_vector *));
  } else {
    vectors = calloc(cols, sizeof(fs_rid_vector *));
  }

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned char *in = message_recv(sock[segment], &segment, &length);
    g_static_mutex_unlock (&link->mutex[segment]);
    content = in + FS_HEADER;

    if (!in) {
      link_error(LOG_ERR, "bind(%d) failed: no reply", segment);
      ret++ ;
      continue;
    } else if (in[3] == FS_NO_MATCH) {
      free(in);
      continue;
    } else if (in[3] != FS_BIND_LIST) {
      link_error(LOG_ERR, "bind(%d) failed: %s", segment, invalid_response(in));
      free(in);
      ret++ ;
      continue;
    }

    if (cols == 0) {
      matches++;
    } else {
      int count = length / (8 * cols);
      if (count == limit) {
        (link->hit_limits)++;
      }
      for (k = 0; k < cols; ++k) {
        fs_rid_vector *v = fs_rid_vector_new(count);
        memcpy(v->data, content, count * 8);
	if (vectors[k]) {
          fs_rid_vector_append_vector(vectors[k], v);
          fs_rid_vector_free(v);
        } else {
          vectors[k] = v;
        }
        content += count * 8;
      }
    }

    free(in);
  }

  if (cols == 0 && matches == 0) {
    free(vectors);
    *result = NULL; /* if there are no results, there's no match */
  } else {
    *result = vectors;
  }

  free(out);

  return ret;
}


int fsp_price_bind (fsp_link *link,
                    fs_segment segment,
                    int flags,
                    fs_rid_vector *mrids,
                    fs_rid_vector *srids,
                    fs_rid_vector *prids,
                    fs_rid_vector *orids,
                    unsigned long long *rows)
{
  int ret = 0;

  unsigned int length = 24 +
         (mrids->length + srids->length + prids->length + orids->length ) * 8;

  unsigned char *out = message_new(FS_PRICE_BIND, segment, length);

  int sock = fsp_write(link, out, length);
  free(out);

  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_ESTIMATED_ROWS) {
    link_error(LOG_ERR, "price_bind(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  memcpy(rows, in + FS_HEADER, sizeof(*rows));
  free(in);

  return ret;
}

int fsp_res_import (fsp_link *link, fs_segment segment,
                    int count, fs_resource buffer[])
{
  unsigned int k, serial_length = 0;

  for (k = 0; k < count; ++k) {
    serial_length+= ((28 + strlen(buffer[k].lex)) / 8);
  }

  int length = 8 + (8 * serial_length);

  unsigned char *out = message_new(FS_INSERT_RESOURCE, segment, length);
  unsigned char *record = out + FS_HEADER;

  memcpy(record, &count, sizeof(int));
  record += 8;
  
  for (k= 0; k < count; ++k) {
    unsigned int one_length = ((28 + strlen(buffer[k].lex)) / 8) * 8;

    memcpy(record, &buffer[k].rid, sizeof (fs_rid));
    memcpy(record + 8, &buffer[k].attr, sizeof (fs_rid));
    memcpy(record + 16, &one_length, sizeof(int));
    strcpy((char *) record + 20, buffer[k].lex);
    record += one_length;
  }

  fsp_write_replica(link, out, length);
  g_static_mutex_unlock (&link->mutex[segment]);

  free(out);
  return 0;
}

int fsp_quad_import (fsp_link *link, fs_segment segment,
                     int flags, int count, fs_rid buffer[][4])
{
  unsigned int length = 8 + (count * 32);
  unsigned char *out = message_new(FS_INSERT_QUAD, segment, length);
  unsigned char *output = out + FS_HEADER;

  memcpy(output, &flags, sizeof(flags));
  memcpy(output + 8, buffer, count * 32);
  fsp_write_replica(link, out, length);
  g_static_mutex_unlock (&link->mutex[segment]);

  free(out);
  return 0;
}

int fsp_res_import_commit (fsp_link *link, fs_segment segment)
{
  unsigned char *out = message_new(FS_COMMIT_RESOURCE, segment, 0);
  fsp_write_replica(link, out, 0);
  free(out);

  return check_message_replica(link, segment, "res_import_commit(%d) failed: %s");
}

int fsp_res_import_commit_all (fsp_link *link)
{
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_COMMIT_RESOURCE, segment, 0);
    fsp_write_replica(link, out, 0);
    free(out);
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "res_import_commit(%d) failed: %s");
  }

  return errors;
}

int fsp_quad_import_commit (fsp_link *link, fs_segment segment, int flags)
{
  unsigned char *out = message_new(FS_COMMIT_QUAD, segment, 4);
  memcpy (out + FS_HEADER, &flags, sizeof(flags));
  fsp_write_replica(link, out, 4);
  free(out);

  return check_message_replica(link, segment, "quad_import_commit(%d) failed: %s");
}

int fsp_quad_import_commit_all (fsp_link *link, int flags)
{
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_COMMIT_QUAD, segment, 4);
    memcpy (out + FS_HEADER, &flags, sizeof(flags));
    fsp_write_replica(link, out, 4);
    free(out);
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "quad_import_commit(%d) failed: %s");
  }

  return errors;
}

int fsp_start_import (fsp_link *link, fs_segment segment)
{
  unsigned char *out = message_new(FS_START_IMPORT, segment, 0);
  fsp_write_replica(link, out, 0);
  free(out);

  return check_message_replica(link, segment, "start_import(%d) failed: %s");
}

int fsp_start_import_all (fsp_link *link)
{
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_START_IMPORT, segment, 0);
    fsp_write_replica(link, out, 0);
    free(out);
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "start_import(%d) failed: %s");
  }

  return errors;
}

int fsp_stop_import (fsp_link *link, fs_segment segment)
{
  unsigned char *out = message_new(FS_STOP_IMPORT, segment, 0);
  fsp_write_replica(link, out, 0);
  free(out);

  return check_message_replica(link, segment, "stop_import(%d) failed: %s");
}

static fs_segment fsp_wait_for_answers(fsp_link *link, int sent[])
{
  struct pollfd active[link->segments];

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    active[segment].fd = link->socks[segment];
    active[segment].events = (sent[segment] == 1) ? POLLIN : 0;
    active[segment].revents = 0;
  }

  while (poll(active, link->segments, -1) == -1) {
    if (errno != EINTR) {
      link_error(LOG_ERR, "while polling: %s", strerror(errno));
      return 0;
    }
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    if (active[segment].revents) return segment;
  }

  link_error(LOG_ERR, "fsp_wait_for_answers fell through");
  return 0;
}

int fsp_stop_import_all (fsp_link *link)
{
  const int threshold = 32;
  int sent[link->segments], working[link->servers];

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    sent[segment] = 0;
  }

  for (int server = 0; server < link->servers; ++server) {
    working[server] = 0;
  }

  int errors = 0, done = 0, waiting = 0;

  do {
    fs_segment segment;
    for (segment = 0; segment < link->segments; ++segment) {
      const int group = link->groups[segment];
      if (sent[segment] || working[group] >= threshold) continue;
      unsigned char *out = message_new(FS_STOP_IMPORT, segment, 0);
      if (waiting == 0) {
        /* can't deadlock waiting for a single lock */
        fsp_write_replica(link, out, 0);
      } else if (g_static_mutex_trylock(&link->mutex[segment])) {
        /* see if we can safely take more locks */
        fsp_write_replica_locked(link, out, 0);
      } else {
        /* it was busy, we'll wait for something else to finish */
        free(out);
        continue;
      }
      free(out);
      sent[segment] = 1;
      working[group]++;
      waiting++;
    }
    segment = fsp_wait_for_answers(link, sent);
    sent[segment] = 2;
    const int group = link->groups[segment];
    errors += check_message_replica(link, segment, "stop_import(%d) failed: %s");
    working[group]--;
    waiting--;
    done++;
  } while (done != link->segments);

  return errors;
}

int fsp_delete_model (fsp_link *link, fs_segment segment, fs_rid_vector *models)
{
  unsigned int length = sizeof(fs_rid) * models->length;
  unsigned char *out = message_new(FS_DELETE_MODELS, segment, length);
  memcpy (out + FS_HEADER, models->data, length);
  fsp_write_replica(link, out, length);
  free(out);

  return check_message_replica(link, segment, "delete_model(%d) failed: %s");
}

int fsp_delete_model_all (fsp_link *link, fs_rid_vector *models)
{
  unsigned int length = sizeof(fs_rid) * models->length;
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_DELETE_MODELS, segment, length);
    memcpy (out + FS_HEADER, models->data, length);
    fsp_write_replica(link, out, length);
    free(out);
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "delete_model(%d) failed: %s");
  }

  return errors;
}

int fsp_delete_quads_all (fsp_link *link, fs_rid_vector *vec[4])
{
  unsigned int length = sizeof(fs_rid) * vec[0]->length;
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_DELETE_QUADS, segment, length * 4);
    for (int s=0; s<4; s++) {
      memcpy(out + FS_HEADER + s * length, vec[s]->data, length);
    }
    fsp_write_replica(link, out, length * 4);
    free(out);
  }

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "delete_quads(%d) failed: %s");
  }

  return errors;
}

int fsp_get_data_size (fsp_link *link, fs_segment segment,
                       fs_data_size *size)
{
  fs_old_data_size old_size;
  unsigned char *out = message_new(FS_GET_SIZE, segment, 0);
  int sock = fsp_write(link, out, 0);
  free(out);

  unsigned int length;
  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_SIZE) {
    link_error(LOG_ERR, "get_data_size(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  if (length != sizeof(fs_old_data_size)) {
    link_error(LOG_ERR, "get_data_size(%d): fs_data_size structure size mis-match", segment);
    free(in);
    return 3;
  }
  
  memcpy (&old_size, in + FS_HEADER, sizeof(fs_old_data_size));
  free(in);

  size->quads_s = old_size.quads_s;
  size->quads_o = old_size.quads_o;
  size->quads_sr = 0; /* get this separately from new enough backends */
  size->resources = old_size.resources;
  size->models_s = old_size.models_s;
  size->models_o = old_size.models_o;

  /* now try to get the newer reverse index size too */
  out = message_new(FS_GET_SIZE_REVERSE, segment, 0);
  sock = fsp_write(link, out, 0);
  free(out);

  in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_SIZE_REVERSE) {
    if (in && in[3] == FS_ERROR) {
      /* probably just not implemented */
      free(in);
      return 0;
    }
    link_error(LOG_ERR, "get_data_size(%d) reverse failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  if (length != sizeof(unsigned long long)) {
    link_error(LOG_ERR, "get_data_size(%d): reverse size mis-match", segment);
    free(in);
    return 3;
  }

  memcpy (&(size->quads_sr), in + FS_HEADER, sizeof(unsigned long long));
  free(in);

  return 0;
}

int fsp_get_import_times (fsp_link *link, fs_segment segment,
                          fs_import_timing *timing)
{
  unsigned char *out = message_new(FS_GET_IMPORT_TIMES, segment, 0);
  int sock = fsp_write(link, out, 0);
  free(out);

  unsigned int length;
  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_IMPORT_TIMES) {
    link_error(LOG_ERR, "get_import_times(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  if (length != sizeof(fs_import_timing)) {
    link_error(LOG_ERR, "get_import_times(%d): fs_import_timing structure size mis-match", segment);
    free(in);
    return 3;
  }
  
  memcpy (timing, in + FS_HEADER, sizeof(fs_import_timing));
  
  free(in);
  return 0;
}

int fsp_get_query_times (fsp_link *link, fs_segment segment,
                          fs_query_timing *timing)
{
  unsigned char *out = message_new(FS_GET_QUERY_TIMES, segment, 0);
  int sock = fsp_write(link, out, 0);
  free(out);

  unsigned int length;
  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_QUERY_TIMES) {
    link_error(LOG_ERR, "get_query_times(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  if (length != sizeof(fs_query_timing)) {
    link_error(LOG_ERR, "get_query_times(%d): fs_query_timing structure size mis-match", segment);
    free(in);
    return 3;
  }
  
  memcpy (timing, in + FS_HEADER, sizeof(fs_query_timing));
  
  free(in);
  return 0;
}


int fsp_bind_limit_many (fsp_link *link,
                         int flags,
                         fs_rid_vector *mrids,
                         fs_rid_vector *srids,
                         fs_rid_vector *prids,
                         fs_rid_vector *orids,
                         fs_rid_vector ***result,
                         int offset,
                         int limit)
{
  fs_rid_vector **vectors;
  fs_segment segment;
  int sock[link->segments];

  const int bind_direction = flags & (FS_BIND_BY_SUBJECT | FS_BIND_BY_OBJECT);

  switch (bind_direction) {
  case FS_BIND_BY_SUBJECT:
    {
      unsigned int shared = mrids->length + prids->length + orids->length;
      unsigned int subjects[link->segments];

      if (srids->length == 0) {
        link_error(LOG_WARNING, "bind_many passed BIND_BY_SUBJECT with no objects");
      }
      for (segment = 0; segment < link->segments; ++segment) {
        subjects[segment] = 0;
      }
      for (int k = 0; k < srids->length; ++k) {
        subjects[FS_RID_SEGMENT(srids->data[k], link->segments)]++;
      }

      for (segment = 0; segment < link->segments; ++segment) {
        unsigned int value, length = 32 + (subjects[segment] + shared) * 8;
        unsigned char *content, *out;

        if (subjects[segment] == 0) {
          sock[segment] = -1;
          continue;
        }
        out = message_new(FS_BIND_LIMIT, segment, length);
        content = out + FS_HEADER;

        memcpy(content, &flags, sizeof(flags));
        memcpy(content + 4, &offset, sizeof(offset));
        memcpy(content + 8, &limit, sizeof(limit));
        value = mrids->length * 8;
        memcpy(content + 12, &value, sizeof(value));
        value = subjects[segment] * 8;
        memcpy(content + 16, &value, sizeof(value));
        value = prids->length * 8;
        memcpy(content + 20, &value, sizeof(value));
        value = orids->length * 8;
        memcpy(content + 24, &value, sizeof(value));
        content += 32;

        memcpy(content, mrids->data, mrids->length * 8);
        content += mrids->length * 8;

        for (int k = 0; k < srids->length; ++k) {
          if (FS_RID_SEGMENT(srids->data[k], link->segments) == segment) {
            memcpy(content, srids->data + k, 8);
            content += 8;
          }
        }

        memcpy(content, prids->data, prids->length * 8);
        content += prids->length * 8;
        memcpy(content, orids->data, orids->length * 8);
        content += orids->length * 8;

        sock[segment] = fsp_write(link, out, length);
        free(out);
      }

      break;
    }
  case FS_BIND_BY_OBJECT:
    {
      unsigned int shared = mrids->length + srids->length + prids->length;
      unsigned int objects[link->segments];

      if (orids->length == 0) {
        link_error(LOG_WARNING, "bind_many passed BIND_BY_OBJECT with no objects");
      }
      for (segment = 0; segment < link->segments; ++segment) {
        objects[segment] = 0;
      }
      for (int k = 0; k < orids->length; ++k) {
        objects[FS_RID_SEGMENT(orids->data[k], link->segments)]++;
      }

      for (segment = 0; segment < link->segments; ++segment) {
        unsigned int value, length = 32 + (objects[segment] + shared) * 8;
        unsigned char *content, *out;

        if (objects[segment] == 0) {
          sock[segment] = -1;
          continue;
        }
        out = message_new(FS_BIND_LIMIT, segment, length);
        content = out + FS_HEADER;

        memcpy(content, &flags, sizeof(flags));
        memcpy(content + 4, &offset, sizeof(offset));
        memcpy(content + 8, &limit, sizeof(limit));
        value = mrids->length * 8;
        memcpy(content + 12, &value, sizeof(value));
        value = srids->length * 8;
        memcpy(content + 16, &value, sizeof(value));
        value = prids->length * 8;
        memcpy(content + 20, &value, sizeof(value));
        value = objects[segment] * 8;
        memcpy(content + 24, &value, sizeof(value));
        content += 32;

        memcpy(content, mrids->data, mrids->length * 8);
        content += mrids->length * 8;
        memcpy(content, srids->data, srids->length * 8);
        content += srids->length * 8;
        memcpy(content, prids->data, prids->length * 8);
        content += prids->length * 8;

        for (int k = 0; k < orids->length; ++k) {
          if (FS_RID_SEGMENT(orids->data[k], link->segments) == segment) {
            memcpy(content, orids->data + k, 8);
            content += 8;
          }
        }

        sock[segment] = fsp_write(link, out, length);
        free(out);
      }

      break;
    }
  default:
    link_error(LOG_ERR, "bind_many passed invalid combination of flags (%d)", flags);
    break;
  }

  int matches = 0, cols = 0, k;
  unsigned int length;

  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    vectors = calloc(1, sizeof(fs_rid_vector *));
  } else {
    vectors = calloc(cols, sizeof(fs_rid_vector *));
  }

  int errors = 0;

  for (segment = 0; segment < link->segments; ++segment) {
    if (sock[segment] == -1) continue;

    unsigned char *in = message_recv(sock[segment], &segment, &length);
    g_static_mutex_unlock (&link->mutex[segment]);

    if (!in) {
      link_error(LOG_ERR, "bind(%d) failed: no reply", segment);
      errors++ ;
      continue;
    } else if (in[3] == FS_NO_MATCH) {
      free(in);
      continue;
    } else if (in[3] != FS_BIND_LIST) {
      link_error(LOG_ERR, "bind(%d) failed: %s", segment, invalid_response(in));
      free(in);
      errors++ ;
      continue;
    }

    unsigned char *content = in + FS_HEADER;

    if (cols == 0) {
      matches++;
    } else {
      int count = length / (8 * cols);
      if (count == limit) {
        (link->hit_limits)++;
      }

      for (k = 0; k < cols; ++k) {
        fs_rid_vector *v = fs_rid_vector_new(count);
        memcpy(v->data, content, count * 8);
	if (vectors[k]) {
          fs_rid_vector_append_vector(vectors[k], v);
          fs_rid_vector_free(v);
        } else {
          vectors[k] = v;
        }
        content += count * 8;
      }
    }
    free(in);
  }

  if (cols == 0 && matches == 0) {
    free(vectors);
    *result = NULL; /* if there are no results, there's no match */
  } else {
    *result = vectors;
  }

  return errors;
}

int fsp_bnode_alloc (fsp_link *link, int count,
                     fs_rid *from, fs_rid *to)
{
  unsigned int length = sizeof(int);
  const fs_segment segment = 0; /* zero holds bNode allocation */
  unsigned char *out = message_new(FS_BNODE_ALLOC, segment, length);

  memcpy(out + FS_HEADER, &count, sizeof(count));
  fsp_write_replica(link, out, length);
  free(out);
  unsigned char *in = message_recv_replica(link, segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_BNODE_RANGE) {
    link_error(LOG_ERR, "bnode_alloc failed: %s", invalid_response(in));
    free(in);
    return 1;
  }

  if (length != 16) {
    link_error(LOG_ERR, "bnode_alloc wrong reply length %u", length);
    free(in);
    return 1;
  }

  memcpy(from, in + FS_HEADER, sizeof(fs_rid));
  memcpy(to, in + FS_HEADER + 8, sizeof(fs_rid));

  free(in);
  return 0;
}

int fsp_resolve (fsp_link *link,
                 fs_segment segment,
                 fs_rid_vector *rids,
                 fs_resource *resources)
{
  if (rids->length == 0) { /* no RIDs */
    return 0;
  }

  unsigned int length = rids->length * 8;
  unsigned char *out = message_new(FS_RESOLVE_ATTR, segment, length);

  memcpy(out + FS_HEADER, rids->data, length);
  int sock = fsp_write(link, out, length);
  free(out);

  unsigned char *in = message_recv(sock, &segment, &length);
  g_static_mutex_unlock (&link->mutex[segment]);

  if (!in || in[3] != FS_RESOURCE_ATTR_LIST) {
    link_error(LOG_ERR, "resolve(%d) failed: %s", segment, invalid_response(in));
    free(in);
    return 1;
  }

  unsigned char *content = in + FS_HEADER;

  for (int k = 0; k < rids->length; ++k) {
    if (content > in + FS_HEADER + length) {
      link_error(LOG_ERR, "resolve(%d) invalid offset", segment);
      free(in);
      return 1;
    }
    unsigned int offset;
    memcpy(&(resources[k].rid), content, sizeof(fs_rid));
    memcpy(&(resources[k].attr), content + 8, sizeof(fs_rid));
    memcpy(&offset, content + 16, sizeof(offset));
    resources[k].lex = strdup((char *) content + 20);
    content += offset;
  }

  free(in);

  return 0;
}

int fsp_resolve_all (fsp_link *link,
                     fs_rid_vector *rids[],
                     fs_resource *resources[])
{
  fs_segment segment;
  int sock[link->segments], ret = 0;

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned int length = rids[segment]->length * sizeof(fs_rid);

    if (length == 0) { /* no RIDs */
      sock[segment] = -1;
      continue;
    }

    unsigned char *out = message_new(FS_RESOLVE_ATTR, segment, length);
    memcpy(out + FS_HEADER, rids[segment]->data, length);
    sock[segment] = fsp_write(link, out, length);

    free(out);
  }

  for (segment = 0; segment < link->segments; ++segment) {
    fs_segment ignore;
    unsigned int length;

    if (sock[segment] == -1) continue; /* skip, no RIDs */

    unsigned char *in = message_recv(sock[segment], &ignore, &length);
    g_static_mutex_unlock (&link->mutex[segment]);

    if (!in || in[3] != FS_RESOURCE_ATTR_LIST) {
      link_error(LOG_ERR, "resolve(%d) failed: %s", segment, invalid_response(in));
      free(in);
      ret ++;
      continue;
    }

    unsigned char *content = in + FS_HEADER;
    fs_resource *res = resources[segment];

    for (int k = 0; k < rids[segment]->length; ++k) {
      unsigned int offset;

      if (content > in + FS_HEADER + length) {
        link_error(LOG_ERR, "resolve(%d) invalid offset", segment);
        ret ++;
        break;
      }
      memcpy(&(res[k].rid), content, sizeof(fs_rid));
      memcpy(&(res[k].attr), content + 8, sizeof(fs_rid));
      memcpy(&offset, content + 16, sizeof(offset));
      res[k].lex = strdup((char *) content + 20);
      content += offset;
    }

    free(in);
  }

  return ret;
}

int fsp_bind_first_all (fsp_link *link, int flags,
                        fs_rid_vector *mrids,
                        fs_rid_vector *srids,
                        fs_rid_vector *prids,
                        fs_rid_vector *orids,
                        fs_rid_vector ***result,
                        int count)
{
  fs_segment segment;
  unsigned char *out, *content;
  unsigned int length, value;
  int sock[link->segments], ret = 0;

  /* fill out */
  length = 32 +
         (mrids->length + srids->length + prids->length + orids->length ) * 8;

  out = message_new(FS_BIND_FIRST, 0, length);
  content = out + FS_HEADER;

  memcpy(content, &flags, sizeof(flags));
  memcpy(content + 4, &count, sizeof(count));
  value = mrids->length * 8;
  memcpy(content + 12, &value, sizeof(value));
  value = srids->length * 8;
  memcpy(content + 16, &value, sizeof(value));
  value = prids->length * 8;
  memcpy(content + 20, &value, sizeof(value));
  value = orids->length * 8;
  memcpy(content + 24, &value, sizeof(value));
  content += 32;

  memcpy(content, mrids->data, mrids->length * 8);
  content += mrids->length * 8;
  memcpy(content, srids->data, srids->length * 8);
  content += srids->length * 8;
  memcpy(content, prids->data, prids->length * 8);
  content += prids->length * 8;
  memcpy(content, orids->data, orids->length * 8);
  content += orids->length * 8;

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    sock[segment] = fsp_write(link, out, length);
  }
  free(out);

  fs_rid_vector **vectors;
  int matches = 0, cols = 0, k;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    vectors = calloc(1, sizeof(fs_rid_vector *));
  } else {
    vectors = calloc(cols, sizeof(fs_rid_vector *));
  }

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned char *in = message_recv(sock[segment], &segment, &length);
    g_static_mutex_unlock (&link->mutex[segment]);
    content = in + FS_HEADER;

    if (!in) {
      link_error(LOG_ERR, "bind_first(%d) failed: no reply", segment);
      ret++ ;
      continue;
    } else if (in[3] == FS_NO_MATCH) {
      free(in);
      continue;
    } else if (in[3] != FS_BIND_LIST) {
      link_error(LOG_ERR, "bind_first(%d) failed: %s", segment, invalid_response(in));
      free(in);
      ret++;
      continue;
    }

    if (cols == 0) {
      matches++;
    } else {
      int count = length / (8 * cols);
      for (k = 0; k < cols; ++k) {
        fs_rid_vector *v = fs_rid_vector_new(count);
        memcpy(v->data, content, count * 8);
	if (vectors[k]) {
          fs_rid_vector_append_vector(vectors[k], v);
          fs_rid_vector_free(v);
        } else {
          vectors[k] = v;
        }
        content += count * 8;
      }
    }

    free(in);
  }

  if (cols == 0 && matches == 0) {
    free(vectors);
    *result = NULL; /* if there are no results, there's no match */
  } else {
    *result = vectors;
  }

  return ret;
}

int fsp_bind_next_all (fsp_link *link, int flags,
                        fs_rid_vector ***result,
                        int count)
{
  fs_segment segment;
  int sock[link->segments], ret = 0;

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_BIND_NEXT, segment, 8);
    memcpy(out + FS_HEADER, &flags, sizeof(flags));
    memcpy(out + FS_HEADER + 4, &count, sizeof(count));
    sock[segment] = fsp_write(link, out, 8);
    free(out);
  }

  fs_rid_vector **vectors;
  int matches = 0, cols = 0, k;
  for (k = 0; k < 4; ++k) {
    if (flags & 1 << k) cols++;
  }

  if (cols == 0) {
    vectors = calloc(1, sizeof(fs_rid_vector *));
  } else {
    vectors = calloc(cols, sizeof(fs_rid_vector *));
  }

  for (segment = 0; segment < link->segments; ++segment) {
    unsigned int length;
    unsigned char *in = message_recv(sock[segment], &segment, &length);
    g_static_mutex_unlock (&link->mutex[segment]);

    if (!in) {
      link_error(LOG_ERR, "bind_next(%d) failed: no reply", segment);
      ret++ ;
      continue;
    } else if (in[3] == FS_NO_MATCH) {
      free(in);
      continue;
    } else if (in[3] != FS_BIND_LIST) {
      link_error(LOG_ERR, "bind_next(%d) failed: %s", segment, invalid_response(in));
      free(in);
      ret++ ;
      continue;
    }

    unsigned char *content = in + FS_HEADER;

    if (cols == 0) {
      matches++;
    } else {
      int count = length / (8 * cols);
      for (k = 0; k < cols; ++k) {
        fs_rid_vector *v = fs_rid_vector_new(count);
        memcpy(v->data, content, count * 8);
        if (vectors[k]) {
          fs_rid_vector_append_vector(vectors[k], v);
          fs_rid_vector_free(v);
        } else {
          vectors[k] = v;
        }
        content += count * 8;
      }
    }

    free(in);
  }

  if (cols == 0 && matches == 0) {
    free(vectors);
    *result = NULL; /* if there are no results, there's no match */
  } else {
    *result = vectors;
  }

  return ret;
}

int fsp_bind_done_all (fsp_link *link)
{
  int sock[link->segments];

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_BIND_DONE, segment, 0);
    sock[segment] = fsp_write(link, out, 0);
    free(out);
  }

  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message(link, sock[segment], "bind_done(%d) failed: %s");
  }

  return errors;
}

#if 0 /* currently un-used */
static int fsp_do_trans (fsp_link *link, fs_segment segment, unsigned char type,
                         const char *message)
{
  unsigned char *out = message_new(FS_TRANSACTION, segment, 1);
  out[FS_HEADER] = type;
  fsp_write_replica(link, out, 1);
  free(out);

  return check_message_replica(link, segment, message);
}
#endif

static int fsp_do_trans_all (fsp_link *link, unsigned char type,
                             const char *message)
{
  const int threshold = 2;
  int sent[link->segments], working[link->servers];
  int done = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    sent[segment] = 0;
  }

  for (int server = 0; server < link->servers; ++server) {
    working[server] = 0;
  }

  int errors = 0;

  do {
    fs_segment segment;
    for (segment = 0; segment < link->segments; ++segment) {
      const int group = link->groups[segment];
      if (sent[segment] || working[group] >= threshold) continue;
      unsigned char *out = message_new(FS_TRANSACTION, segment, 1);
      out[FS_HEADER] = type;
      fsp_write_replica(link, out, 1);
      free(out);
      sent[segment] = 1;
      working[group] ++;
    }
    segment = fsp_wait_for_answers(link, sent);
    sent[segment] = 2;
    const int group = link->groups[segment];
    errors += check_message_replica(link, segment, message);
    working[group] --;
    done++;
  } while (done != link->segments);

  return errors;
}

int fsp_transaction_begin_all(fsp_link *link)
{
  return fsp_do_trans_all(link, FS_TRANS_BEGIN, "transaction_begin(%d) failed: %s");
}

int fsp_transaction_rollback_all(fsp_link *link)
{
  return fsp_do_trans_all(link, FS_TRANS_ROLLBACK, "transaction_rollback(%d) failed: %s");
}

int fsp_transaction_pre_commit_all(fsp_link *link)
{
  return fsp_do_trans_all(link, FS_TRANS_PRE_COMMIT, "transaction_pre_commit(%d) failed: %s");
}

int fsp_transaction_commit_all(fsp_link *link)
{
  return fsp_do_trans_all(link, FS_TRANS_COMMIT, "transaction_commit(%d) failed: %s");
}

int fsp_lock (fsp_link *link)
{
  unsigned char *out = message_new(FS_LOCK, 0, 0);
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    fsp_write_primary(link, out, 0);

    errors += check_message(link, link->socks1[segment], "lock(%d) failed: %s");
  }
  free(out);

  return errors;
}

int fsp_unlock (fsp_link *link)
{
  unsigned char *out = message_new(FS_UNLOCK, 0, 0);
  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    fsp_write_primary(link, out, 0);

    errors += check_message(link, link->socks1[segment], "unlock(%d) failed: %s");
  }
  free(out);

  return errors;
}

int fsp_new_model_all (fsp_link *link, fs_rid_vector *models)
{
  unsigned int length = sizeof(fs_rid) * models->length;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned char *out = message_new(FS_NEW_MODELS, segment, length);
    memcpy (out + FS_HEADER, models->data, length);
    fsp_write_replica(link, out, length);
    free(out);
  }

  int errors = 0;

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    errors += check_message_replica(link, segment, "new_model(%d) failed: %s");
  }

  return errors;
}

int fsp_get_quad_freq_all (fsp_link *link, int index, int count, fs_quad_freq **freq)
{
  int sock[link->segments];
  *freq = malloc(sizeof(fs_quad_freq) * (count * link->segments + 1));
  fs_quad_freq *next = *freq;

  unsigned char *out = message_new(FS_GET_QUAD_FREQ, 0, 2 * sizeof(int));
  memcpy (out + FS_HEADER, &index, sizeof(int));
  memcpy (out + FS_HEADER + sizeof(int), &count, sizeof(int));

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    unsigned int * const s = (unsigned int *) (out + 8);
    *s = segment;
    sock[segment] = fsp_write(link, out, 2 * sizeof(int));
  }
  free(out);

  for (fs_segment segment = 0; segment < link->segments; ++segment) {
    fs_segment ignore;
    unsigned int length;
    unsigned char *in = message_recv(sock[segment], &ignore, &length);
    g_static_mutex_unlock (&link->mutex[segment]);

    if (!in || in[3] != FS_QUAD_FREQ) {
      link_error(LOG_ERR, "get_quad_freq(%d) failed: %s", segment, invalid_response(in));
      free(*freq);
      *freq = NULL;
      free(in);
      return 1;
    }

    if (length % sizeof(fs_quad_freq) != 0 || length > sizeof(fs_quad_freq) * count) {
      link_error(LOG_ERR, "get_quad_freq(%d): result size wrong", segment);
      free(*freq);
      *freq = NULL;
      free(in);
      return 3;
    }

    memcpy (next, in + FS_HEADER, length);
    next += length / sizeof(fs_quad_freq);

    free(in);
  }

  next->freq = 0;
  next->pri = next->sec = 0;

  return 0;
}

