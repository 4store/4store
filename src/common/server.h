
/* Let's not worry about why I called this 4store */
/* This is the intended API to the network code */

#include <syslog.h>

#include "../backend/backend.h"

/* server stuff */

typedef unsigned char * (* fsp_backend_fn) (fs_backend *bs,
					    fs_segment segment,
                                            unsigned int length,
                                            unsigned char *buffer) ;
typedef struct {
  /* no-op is handled internally */
  fsp_backend_fn resolve;
  fsp_backend_fn bind;
  fsp_backend_fn price;

  fsp_backend_fn insert_resource;
  fsp_backend_fn insert_triple; /* deprecated */
  fsp_backend_fn delete_models;
  fsp_backend_fn delete_quads;
  fsp_backend_fn new_models;

  fsp_backend_fn segments; /* deprecated */
  fsp_backend_fn commit_resource;
  fsp_backend_fn commit_triple; /* deprecated */

  fsp_backend_fn start_import;
  fsp_backend_fn stop_import;
  fsp_backend_fn get_data_size;
  fsp_backend_fn get_import_times;

  fsp_backend_fn insert_quad;
  fsp_backend_fn commit_quad;

  fsp_backend_fn get_query_times;
  fsp_backend_fn bind_limit;
  fsp_backend_fn reverse_bind;
  fsp_backend_fn bnode_alloc;
  fsp_backend_fn resolve_attr;

  fsp_backend_fn bind_first;
  fsp_backend_fn bind_next;
  fsp_backend_fn bind_done;

  fsp_backend_fn transaction;
  fsp_backend_fn lock;
  fsp_backend_fn unlock;

  fsp_backend_fn node_segments;
  fsp_backend_fn get_size_reverse;
  fsp_backend_fn get_quad_freq;

  fsp_backend_fn auth;
  fsp_backend_fn choose_segment;

  fs_backend * (* open) (const char *kb_name, int flags);
  void (* close) (fs_backend *backend);
  int (* segment_count) (fs_backend *backend);
} fsp_backend;

void fsp_serve (const char *kb_name, fsp_backend *implementation, int daemon, float free_disk);
