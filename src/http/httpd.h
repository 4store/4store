/*
 *  Copyright 2006 Nick Lamb for Garlik.com
 */

typedef struct {
  int sock;
  char *request;
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
} client_ctxt;
