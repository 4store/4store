#ifndef TREE_H
#define TREE_H

#include <stdint.h>

#include "backend.h"
#include "chain.h"
#include "../common/4s-datatypes.h"

#define FS_INDEX_IGNORED_BITS 5
#define FS_INDEX_ROOT_BITS 12
#define FS_INDEX_ROOTS (1<<FS_INDEX_ROOT_BITS)

#define FS_INDEX_BRANCH_BITS 4
#define FS_INDEX_BRANCHES (1<<FS_INDEX_BRANCH_BITS)

#define FS_TREE_NODE(x) ((x) | 0x80000000)
#define FS_TREE_OFFSET(x) ((x) & 0x7fffffffU)

#define FS_TREE_BLACKLISTED  FS_TREE_NODE(1)
#define FS_TREE_LONG_INDEX   FS_TREE_NODE(2)

typedef struct _fs_chain_ref {
	fs_index_node head;
	fs_index_node tail;
} fs_chain_ref;

typedef struct _fs_tree_index {
	fs_index_node branch[FS_INDEX_BRANCHES];
} fs_tree_index;

struct _fs_tree;
typedef struct _fs_tree fs_tree;

fs_tree *fs_tree_new(fs_backend *be, const char *name, int flags);

void fs_tree_node_print(fs_tree *t, fs_index_node node, FILE *out);
void fs_tree_print(fs_tree *t, FILE *out, int verbosity);

int fs_tree_sync(fs_tree *t);

int fs_tree_get_leaf(fs_tree *t, fs_rid a, fs_rid b, fs_index_node *tnnode_out, int *branch_out, int *level_out);
fs_index_node fs_tree_add_quad(fs_tree *t, fs_rid a, fs_rid b, fs_rid quad[4]);
fs_index_node fs_tree_add_i32(fs_tree *t, fs_rid a, fs_rid b, int32_t data);

static inline int fs_is_tree_ref(fs_index_node x)
{
    return x & (fs_index_node)(0x80000000);
}

static inline int fs_tree_root_from_hash(fs_rid r)
{
    return FS_TREE_NODE((r >> FS_INDEX_IGNORED_BITS) & (FS_INDEX_ROOTS - 1));
}

static inline int fs_tree_branch_from_hash(fs_rid a, fs_rid b, int offset)
{
    if (offset < 2) {
        /* offset 0 would be some part of the root bits */
        return (a >> (FS_INDEX_IGNORED_BITS + FS_INDEX_ROOT_BITS + FS_INDEX_BRANCH_BITS*offset)) & (FS_INDEX_BRANCHES-1);
    }

    if (b == FS_RID_NULL) {
	return -1;
    }
    offset -= 2;
    return (b >> (FS_INDEX_IGNORED_BITS + FS_INDEX_BRANCH_BITS*offset)) & (FS_INDEX_BRANCHES-1);
}

fs_chain_ref fs_tree_get_chain_ref(fs_tree *t, fs_rid a, fs_rid b);

fs_index_node fs_tree_new_index_node(fs_tree *t);

fs_tree_index *fs_tree_get_node(fs_tree *t, fs_index_node n);

fs_chain *fs_tree_get_chain(fs_tree *t);
int fs_tree_blacklist_index(fs_tree *t, fs_rid a, fs_rid b);

fs_tree *fs_tree_open_filename(fs_backend *be, const char *name, const char *filename, int flags);

fs_tree *fs_tree_open(fs_backend *be, const char *name, int flags);

int fs_tree_close(fs_tree *t);


#endif
