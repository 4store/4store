#ifndef FS_H
#define FS_H

/* Let's not worry about why I called this 4store */
/* This is the intended API to the network code */

#include <syslog.h>

#include "datatypes.h"
#include "backend/backend.h"

/* message types */
/* use a #define because these are part of the on-the-wire protocol
   and thus cannot be changed */

#define FS_NO_OP 0x01
#define FS_DONE_OK 0x02
#define FS_ERROR 0x03
#define FS_RESOLVE 0x04
#define FS_RESOURCE_LIST 0x05
#define FS_INSERT_RESOURCE 0x06
/* #define FS_INSERT_TRIPLE 0x07  deprecated */
#define FS_DELETE_MODEL 0x08
#define FS_BIND 0x09
#define FS_BIND_LIST 0x0a
#define FS_NO_MATCH 0x0b
#define FS_PRICE_BIND 0x0c
#define FS_ESTIMATED_ROWS 0x0d
#define FS_SEGMENTS 0x0e /* deprecate this later */
#define FS_SEGMENT_LIST 0x0f /* deprecate this later */
/* #define FS_COMMIT_TRIPLE 0x10  deprecated */
#define FS_COMMIT_RESOURCE 0x11
#define FS_START_IMPORT 0x12
#define FS_STOP_IMPORT 0x13
#define FS_GET_SIZE 0x14
#define FS_SIZE 0x15
#define FS_GET_IMPORT_TIMES 0x16
#define FS_IMPORT_TIMES 0x17
#define FS_INSERT_QUAD 0x18
#define FS_COMMIT_QUAD 0x19
#define FS_GET_QUERY_TIMES 0x1a
#define FS_QUERY_TIMES 0x1b
#define FS_BIND_LIMIT 0x1c
#define FS_BNODE_ALLOC 0x1d
#define FS_BNODE_RANGE 0x1e
#define FS_RESOLVE_ATTR 0x1f
#define FS_RESOURCE_ATTR_LIST 0x20
#define FS_AUTH 0x21
#define FS_DELETE_MODELS 0x22
#define FS_BIND_FIRST 0x23
#define FS_BIND_NEXT 0x24
#define FS_BIND_DONE 0x25
#define FS_TRANSACTION 0x26

#define FS_TRANS_BEGIN 'b'
#define FS_TRANS_ROLLBACK 'r'
#define FS_TRANS_PRE_COMMIT 'p'
#define FS_TRANS_COMMIT 'c'

#define FS_NODE_SEGMENTS 0x27
#define FS_NODE_SEGMENT_LIST 0x28
#define FS_REVERSE_BIND 0x29
#define FS_LOCK 0x2a
#define FS_UNLOCK 0x2b
#define FS_GET_SIZE_REVERSE 0x2c
#define FS_SIZE_REVERSE 0x2d
#define FS_NEW_MODELS 0x2e
#define FS_GET_QUAD_FREQ 0x2f
#define FS_QUAD_FREQ 0x30
#define FS_CHOOSE_SEGMENT 0x31

#define FS_DELETE_QUADS 0x32

/* message header  = 16 bytes */
#define FS_HEADER 16

typedef struct fsp_link_struct fsp_link;

#define FS_OPEN_HINT_RW 0
#define FS_OPEN_HINT_RO 1
fsp_link* fsp_open_link (const char *name, char *pw, int readonly);
void fsp_close_link (fsp_link *link);
int fsp_link_segments (fsp_link *link);
const char *fsp_link_features (fsp_link *link);
unsigned char *fsp_error_new(fs_segment segment, const char *message);
unsigned char *message_new(int type, fs_segment segment, size_t length);

char * fsp_argv_password (int *argc, char *argv[]);

#ifdef FS_PROFILE_WRITE
long long* fsp_profile_write(fsp_link *link);
#endif

void fsp_log(int priority, const char *format, ...)
                                   __attribute__ ((format(printf, 2, 3)));

void fsp_syslog_enable(void);
void fsp_syslog_disable(void);

int fsp_no_op (fsp_link *link, fs_segment segment);
int fsp_resolve (fsp_link *link, fs_segment segment,
                 fs_rid_vector *rids,
                 fs_resource *resources);
int fsp_res_import (fsp_link *link, fs_segment segment,
                    int count,
                    fs_resource buffer[]);
int fsp_quad_import (fsp_link *link, fs_segment segment,
                     int flags,
                     int count,
                     fs_rid buffer[][4]);
int fsp_res_import_commit (fsp_link *link, fs_segment segment);
int fsp_quad_import_commit (fsp_link *link, fs_segment segment,
                            int flags);
int fsp_bind_limit (fsp_link *link, fs_segment segment,
                    int flags,
                    fs_rid_vector *mrids,
                    fs_rid_vector *srids,
                    fs_rid_vector *prids,
                    fs_rid_vector *orids,
                    fs_rid_vector ***result,
                    int offset,
                    int limit);
int fsp_price_bind (fsp_link *link, fs_segment segment,
                    int flags,
                    fs_rid_vector *mrids,
                    fs_rid_vector *srids,
                    fs_rid_vector *prids,
                    fs_rid_vector *orids,
                    unsigned long long *rows);
int fsp_delete_model (fsp_link *link, fs_segment segment,
                      fs_rid_vector *models);
int fsp_start_import (fsp_link *link, fs_segment segment);
int fsp_stop_import (fsp_link *link, fs_segment segment);
int fsp_get_data_size (fsp_link *link, fs_segment segment,
                       fs_data_size *size);
int fsp_get_import_times (fsp_link *link, fs_segment segment,
                          fs_import_timing *timing);
int fsp_get_query_times (fsp_link *link, fs_segment segment,
                         fs_query_timing *timing);

int fsp_resolve_all (fsp_link *link, fs_rid_vector *rids[], fs_resource *resources[]);
int fsp_start_import_all (fsp_link *link);
int fsp_stop_import_all (fsp_link *link);
int fsp_delete_model_all (fsp_link *link, fs_rid_vector *models);
int fsp_new_model_all (fsp_link *link, fs_rid_vector *models);
int fsp_delete_quads_all (fsp_link *link, fs_rid_vector *vec[4]);

int fsp_bind_limit_many (fsp_link *link,
                         int flags,
                         fs_rid_vector *mrids,
                         fs_rid_vector *srids,
                         fs_rid_vector *prids,
                         fs_rid_vector *orids,
                         fs_rid_vector ***result,
                         int offset,
                         int limit);
int fsp_bind_limit_all (fsp_link *link,
                  int flags,
                  fs_rid_vector *mrids,
                  fs_rid_vector *srids,
                  fs_rid_vector *prids,
                  fs_rid_vector *orids,
                  fs_rid_vector ***result,
                  int offset,
                  int limit);

#define fsp_bind(link, segment, flags, mrids, srids, prids, orids, result) \
	fsp_bind_limit(link, segment, flags, mrids, srids, prids, orids, result, -1, -1)

#define fsp_bind_many(link, flags, mrids, srids, prids, orids, result) \
	fsp_bind_limit_many(link, flags, mrids, srids, prids, orids, result, -1, -1)

#define fsp_bind_all(link, flags, mrids, srids, prids, orids, result) \
	fsp_bind_limit_all(link, flags, mrids, srids, prids, orids, result, -1, -1)

int fsp_reverse_bind_all (fsp_link *link,
                          int flags,
                          fs_rid_vector *mrids,
                          fs_rid_vector *srids,
                          fs_rid_vector *prids,
                          fs_rid_vector *orids,
                          fs_rid_vector ***result,
                          int offset,
                          int limit);

int fsp_bind_first_all (fsp_link *link, int flags,
                        fs_rid_vector *mrids,
                        fs_rid_vector *srids,
                        fs_rid_vector *prids,
                        fs_rid_vector *orids,
                        fs_rid_vector ***result,
                        int count);
int fsp_bind_next_all (fsp_link *link, int flags,
                        fs_rid_vector ***result,
                        int count);
int fsp_bind_done_all (fsp_link *link);

int fsp_transaction_begin_all(fsp_link *link);
int fsp_transaction_rollback_all(fsp_link *link);
int fsp_transaction_pre_commit_all(fsp_link *link);
int fsp_transaction_commit_all(fsp_link *link);

int fsp_bnode_alloc (fsp_link *link, int count,
                     fs_rid *from, fs_rid *to);

int fsp_lock (fsp_link *link);
int fsp_unlock (fsp_link *link);

int fsp_get_quad_freq_all (fsp_link *link, int index, int count,
                           fs_quad_freq **freq);

int fsp_res_import_commit_all (fsp_link *link);
int fsp_quad_import_commit_all (fsp_link *link, int flags);

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

const char *fsp_kb_name(fsp_link *link);

int fsp_hit_limits(fsp_link *link);
void fsp_hit_limits_reset(fsp_link *link);
void fsp_hit_limits_add(fsp_link *link, int delta);

typedef enum {
  FS_HASH_UNKNOWN,
  FS_HASH_MD5,
  FS_HASH_UMAC,
  FS_HASH_CRC64
} fsp_hash_enum;

fsp_hash_enum fsp_hash_type(fsp_link *link);

#endif
