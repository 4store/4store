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


    Copyright 2006 Nick Lamb for Garlik.com
    Copyright 2010 Martin Galpin (CORS support)
 */

typedef enum {
  FS_HTTP_OPTIONS = 0,
  FS_HTTP_GET,
  FS_HTTP_HEAD,
  FS_HTTP_POST,
  FS_HTTP_PUT,
  FS_HTTP_DELETE
} fs_http_method;

typedef struct {
  int sock;
  char *request;
  fs_http_method method;
  GIOChannel *ioch;
  GHashTable *headers;
  int importing;
  char *import_uri;
  long bytes_left;
  GByteArray *partial;
  char *query_string;
  char *update_string;
  fs_query *qr;
  int query_flags;
  guint watchdog;
  int soft_limit;
  char *output;
  unsigned int query_id;
  double start_time;
  char *apikey;
} client_ctxt;
