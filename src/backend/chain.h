#ifndef CHAIN_H
#define CHAIN_H

#include "chain.h"
#include "backend.h"
#include "bucket.h"

struct fs_bc_header {
    int32_t id;
    int32_t size;
    int32_t length;
    int32_t free_list;
    char padding[500];
};

typedef struct _fs_chain {
  struct fs_bc_header *header;
  char *filename;
  void *ptr;		/* mmap ptr + head of file */
  size_t len;		/* length of mmap'd region */
  int fd;		/* fd of mapped file */
  int flags;		/* flags used in open call */
  fs_bucket *data;	/* array of used buckets, points into mmap'd space */
} fs_chain;

//#include "tree.h"

fs_chain *fs_chain_open(fs_backend *be, const char *label, int flags);
fs_chain *fs_chain_open_filename(const char *fname, int flags);
int fs_chain_sync(fs_chain *bc);
int fs_chain_close(fs_chain *bc);
void fs_chain_print(fs_chain *bc, FILE *out, int verbosity);
unsigned int fs_chain_length(fs_chain *bc, fs_index_node b);
fs_index_node fs_chain_new_bucket(fs_chain *bc);
int fs_chain_remove(fs_chain *c, fs_index_node b);
int fs_chain_remove_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2], int *removed);

/* returns 0 if the pair is present */
int fs_chain_get_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2]);

fs_index_node fs_chain_add_single(fs_chain *bc, fs_index_node b, fs_rid val);
fs_index_node fs_chain_add_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2]);
fs_index_node fs_chain_add_quad(fs_chain *bc, fs_index_node b, fs_rid quad[4]);
fs_index_node fs_chain_add_i32(fs_chain *bc, fs_index_node b, int32_t data, int *newlength);

fs_bucket *fs_chain_get_bucket(fs_chain *c, fs_index_node b);

int fs_chain_free_bucket(fs_chain *bc, fs_index_node b);

#endif
