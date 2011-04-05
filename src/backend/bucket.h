#ifndef BUCKET_H
#define BUCKET_H

#include "../common/4s-datatypes.h"

/* 1k buckets 
#define FS_BUCKET_PADDING 1016
#define FS_RID_BUCKET_DATA_LEN 127
#define FS_I32_BUCKET_DATA_LEN 254
*/

#define FS_BUCKET_PADDING 504
#define FS_RID_BUCKET_DATA_LEN 63
#define FS_I32_BUCKET_DATA_LEN 126

#define FS_PACKED __attribute__((__packed__))

typedef struct _fs_bucket {
    fs_index_node cont;
    int32_t       length;
    unsigned char padding[FS_BUCKET_PADDING];
} FS_PACKED fs_bucket;

typedef struct _fs_rid_bucket {
    fs_index_node cont;
    int32_t       length;
    fs_rid        data[FS_RID_BUCKET_DATA_LEN];
} FS_PACKED fs_rid_bucket;

typedef struct _fs_i32_bucket {
    fs_index_node cont;
    int32_t       length;
    int32_t       data[FS_I32_BUCKET_DATA_LEN];
} FS_PACKED fs_i32_bucket;

int fs_rid_bucket_get_pair(fs_rid_bucket *b, fs_rid pair[2]);
int fs_rid_bucket_remove_pair(fs_rid_bucket *b, fs_rid pair[2], int *removed);

int fs_rid_bucket_add_single(fs_rid_bucket *b, fs_rid val);
int fs_rid_bucket_add_pair(fs_rid_bucket *b, fs_rid quad[2]);
int fs_rid_bucket_add_quad(fs_rid_bucket *b, fs_rid quad[4]);
void fs_rid_bucket_print(fs_rid_bucket *b, FILE *out, int verbosity);

int fs_i32_bucket_add_i32(fs_i32_bucket *b, int32_t data);
void fs_i32_bucket_print(fs_i32_bucket *b, FILE *out, int verbosity);

#endif
