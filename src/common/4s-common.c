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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <stdarg.h>
#include <errno.h>

#include "error.h"

static int fsp_syslog_active = 0;
static unsigned char fsp_vermagic[4] = { 'I', 'D', FS_PROTO_VER_MINOR, 0x0 };
static char *fs_config_file = NULL;

void fsp_syslog_enable(void)
{
  openlog("4store", LOG_PID | LOG_PERROR | LOG_NDELAY, LOG_LOCAL3);
  fsp_syslog_active = 1;
}

void fsp_syslog_disable(void)
{
  fsp_syslog_active = 0;
  closelog();
}

void fsp_log(int priority, const char *format, ...)
{
  va_list args;

  va_start(args, format);
  /* need to open the log elsewhere */
  if (fsp_syslog_active) {
    vsyslog(priority, format, args);
  } else {
    vfprintf(stderr, format, args);
    fputc('\n', stderr);
  }
  va_end(args);
}

/* for clients, send a NO OP, doesn't matter what protocol version
   the server will use their version in their reply even for errors */

int fsp_ver_fixup (fsp_link *link, int sock)
{
  int err;
  if (link->hash_type != FS_HASH_UNKNOWN) {
    /* already know the type of hash we're using */
    return 0;
  }

  unsigned char *out = message_new(FS_NO_OP, 0, 0);
  err = write(sock, out, FS_HEADER + 0);
  free(out);

  if (err != FS_HEADER) {
    fs_error(LOG_ERR, "unable to write to socket, %s", strerror(errno));
    return -1;
  }

  unsigned char header[FS_HEADER];
  err = recv(sock, header, FS_HEADER, MSG_WAITALL);

  if (err < 0) {
    fs_error(LOG_ERR, "recv header from socket failed, %s", strerror(errno));
    return -1;
  } else if (err == 0) {
    return -1;
  }

  switch (header[2]) {
    case 0x80:
      link->hash_type = FS_HASH_MD5;
      break;
    case 0x81:
      link->hash_type = FS_HASH_CRC64;
      break;
    case 0x82:
      link->hash_type = FS_HASH_UMAC;
      break;
       
    default:
      fs_error(LOG_ERR, "incompatible protocol");
      return -1;
  }

  if (fsp_vermagic[2] == header[2]) {
    /* guessed right first time, clean up */

    unsigned int * const l = (unsigned int *) (header + 4);
    char *buffer = calloc(1, *l);
    recv(sock, buffer, *l, MSG_WAITALL);
    free(buffer);
    return 0;
  } else {
    fsp_vermagic[2] = header[2];
    return 1;
  }
}

void default_hints(struct addrinfo *hints)
{
  memset(hints, 0, sizeof(struct addrinfo));
  hints->ai_family = AF_UNSPEC;
  hints->ai_socktype = SOCK_STREAM; /* tcp */
  /* no IPv6 without a routeable IPv6 address */
#if defined(AI_ADDRCONFIG)
  hints->ai_flags |= AI_ADDRCONFIG;
#endif
}

unsigned char *message_new(int type, fs_segment segment, size_t length)
{
  unsigned char *buffer = calloc(1, FS_HEADER + length);
  unsigned int * const l = (unsigned int *) (buffer + 4);
  unsigned int * const s = (unsigned int *) (buffer + 8);

  memcpy(buffer, fsp_vermagic, 3);
  buffer[3] = (unsigned char) type;
  *l = (unsigned int) length;
  *s = segment;

  return buffer;
}

unsigned char *fsp_error_new(fs_segment segment, const char *message)
{
  size_t length = strlen(message) + 1;
  unsigned char *err = message_new(FS_ERROR, segment, length);

  memcpy(err + FS_HEADER, message, length);

  return err;
}

unsigned char *message_recv(int sock,
                            unsigned int *segment,
                            unsigned int *length)
{
  int err;
  unsigned char header[FS_HEADER];
  unsigned char *buffer, *p;
  unsigned int * const l = (unsigned int *) (header + 4);
  unsigned int * const s = (unsigned int *) (header + 8);
  unsigned int len;

  err= recv(sock, header, FS_HEADER, MSG_WAITALL);
  if (err < 0) {
    fs_error(LOG_ERR, "recv header from socket failed, %s", strerror(errno));
    return NULL;
  } else if (err == 0) {
    return NULL;
  }

  if (memcmp(header, fsp_vermagic, 3)) {
    fs_error(LOG_ERR, "incorrect version magic %02x%02x%02x", header[0], header[1], header[2]);
    return NULL;
  }

  *segment = *s;
  len = *length = *l; /* FIXME check length overflow */

  buffer = calloc(1, FS_HEADER + len);
  memcpy(buffer, header, FS_HEADER);
  p = buffer + FS_HEADER;
  
  while (len > 0) {
    int count = recv(sock, p, len, 0);

    if (count <= 0) {
      fs_error(LOG_ERR, "recv body from socket failed, %s", strerror(errno));
      break;
    }
    p+= count;
    len-= count;
  }
  
  return buffer;
}

const char *fsp_kb_name(fsp_link *link)
{
  return link->kb_name;
}

int fsp_hit_limits(fsp_link *link)
{
  return link->hit_limits;
}

void fsp_hit_limits_reset(fsp_link *link)
{
  link->hit_limits = 0;
}

void fsp_hit_limits_add(fsp_link *link, int delta)
{
  if (delta < 1) return;

  (link->hit_limits) += delta;
}

fsp_hash_enum fsp_hash_type(fsp_link *link)
{
  return link->hash_type;
}

const char *fs_get_config_file(void)
{
    if (fs_config_file != NULL) {
        return fs_config_file;
    }
    else {
        return FS_CONFIG_FILE;
    }
}

void fs_set_config_file(const char *config_file)
{
    fs_config_file = (char *)config_file;
}
