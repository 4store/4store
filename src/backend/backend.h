#ifndef BACKEND_H
#define BACKEND_H

#include "../common/4s-datatypes.h"
#include "../common/params.h"
#include "../frontend/decimal.h"

#define FS_BACKEND_QUIET   1
#define FS_BACKEND_NO_OPEN 2
#define FS_BACKEND_PRELOAD 4

/* legacy */
#define FS_QUIET     1

#define FS_OPEN_LEX     0x01
#define FS_OPEN_LIST    0x02
#define FS_OPEN_DEL     0x08
#define FS_OPEN_SLIST   0x10
#define FS_OPEN_MHASH   0x20
#define FS_OPEN_ALL     0xff

#ifndef HAVE_UUID_STRING_T
#define HAVE_UUID_STRING_T 1
typedef char uuid_string_t[37];
#endif

struct _fs_backend;
typedef struct _fs_backend fs_backend;

fs_backend *fs_backend_init(const char *db_name, int flags) __attribute__((warn_unused_result));
void fs_backend_fini(fs_backend *be);
const char *fs_backend_get_kb(fs_backend *be);
int fs_backend_get_segments(fs_backend *be);
fs_segment fs_backend_get_segment(fs_backend *be);
void fs_backend_set_min_free(fs_backend *be, float min_free);

int fs_backend_need_reload(void);
#define fs_backend_open_files(b, s, fl, fi) fs_backend_open_files_intl(b, s, fl, fi, __FILE__, __LINE__)
int fs_backend_open_files_intl(fs_backend *be, fs_segment seg, int flags, int files, char *file, int line);
int fs_backend_unlink_indexes(fs_backend *be, fs_segment seg);
void fs_backend_ptree_limited_open(fs_backend *be, int n);
int fs_backend_open_ptree(fs_backend *be, fs_rid pred);
int fs_backend_close_files(fs_backend *be, fs_segment seg);
int fs_backend_cleanup_files(fs_backend *be);
struct _fs_ptree *fs_backend_get_ptree(fs_backend *be, fs_rid pred, int object);

void fs_bnode_alloc(fs_backend *be, int count, fs_rid *from, fs_rid *to);

int fs_segments(fs_backend *be, int *segments);
void fs_node_segments(fs_backend *be, char *segments);

fs_import_timing fs_get_import_times(fs_backend *be, int seg);
fs_query_timing fs_get_query_times(fs_backend *be, int seg);

int fs_start_import(fs_backend *be, int seg);
int fs_stop_import(fs_backend *be, int seg);
int fs_backend_transaction(fs_backend *be, fs_segment seg, int op);

int fs_backend_is_transaction_open_intl(fs_backend *be, char *file, int line);
#define fs_backend_is_transaction_open(be) fs_backend_is_transaction_open_intl(be, __FILE__, __LINE__)

/* set val to the int stored in the model hash, used to indicate if the model
 * contains any quads */
int fs_backend_model_get_usage(fs_backend *be, int seg, fs_rid model, fs_index_node *val);
int fs_backend_model_set_usage(fs_backend *be, int seg, fs_rid model, fs_index_node val);

/* return true if were storing model data in nested dirs */
int fs_backend_model_dirs(fs_backend *be);
/* return true if were storing model data in files as opposed to a tblist */
int fs_backend_model_files(fs_backend *be);

#endif
