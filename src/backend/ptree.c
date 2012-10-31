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
#if !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__OpenBSD__)
#define _XOPEN_SOURCE 500
#endif
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "backend.h"
#include "ptree.h"
#include "chain.h"
#include "../common/4s-datatypes.h"
#include "../common/4s-hash.h"
#include "../common/params.h"
#include "../common/error.h"

#define FS_PTREE_ID 0x4a585031
#define FS_PTREE_REVISION 0

#define FS_PTREE_SIZE_INC  32768
#define FS_PTREE_NULL_NODE 0x80000000U
#define FS_PTREE_ROOT_NODE 0x80000001U

#define FS_PTREE_BRANCHES    4
#define FS_PTREE_BRANCH_BITS 2  /* log2(FS_PTREE_BRANCHES) */

#define IS_NODE(n) ((n) & 0x80000000U)
#define IS_LEAF(n) (!IS_NODE(n))
#define NODE_REF(pt, n) (pt->nodes+((n) & 0x7fffffffU))
#define LEAF_REF(pt, l) (pt->leaves+(l))
#define PK_BRANCH(pk, i) (pk >> (((64/FS_PTREE_BRANCH_BITS)-1-i) * FS_PTREE_BRANCH_BITS)) & (FS_PTREE_BRANCHES-1);

#define FS_PACKED __attribute__((__packed__))

typedef uint32_t nodeid;

typedef struct _node {
    nodeid branch[FS_PTREE_BRANCHES];
} node;

typedef struct _leaf {
    fs_rid pk;
    uint32_t block;
    uint32_t length;
} leaf;

struct ptree_header {
    int32_t id;             // "JXP1"
    uint32_t node_base;     // number of next node to be allocated
    uint32_t node_size;     // max id of node allocated on disk
    uint32_t node_count;    // nodes used
    uint32_t node_alloc;    // nodes allocated
    uint32_t leaf_base;     // number of leaf node to be allocated
    uint32_t leaf_size;     // max id of leaf allocated on disk
    uint32_t leaf_count;    // leaves used
    uint32_t leaf_alloc;    // leaves allocated
    int32_t revision;       // revision of the strucure
                            // rev=0 - vanilla, rev=1 - with staging
    int64_t alloc;          // length of file allocated to data (in bytes, not
                            // inc. header)
    int64_t count;
    nodeid node_free;       // list of free nodes, linked by branch[0]
    nodeid leaf_free;       // list of free leaves, linked by block
    char padding[448];      // allign to a block
} FS_PACKED;

struct _fs_ptree {
    int fd;
    char *filename;
    int flags;              // open() flags
    off_t file_length;
    void *ptr;              // mmap'd address
    struct ptree_header *header;
    node *nodes;
    leaf *leaves;
    fs_ptable *table;
};

typedef struct _tree_pos {
    nodeid node;
    int branch;
    struct _tree_pos *next;
} tree_pos;

struct _fs_ptree_it {
    fs_rid pk;
    fs_ptree *pt;
    leaf *leaf;
    fs_row_id block;
    int32_t step;
    int32_t length;
    fs_rid pair[2];
    int traverse;
    tree_pos *stack;
};

int fs_ptree_grow_nodes(fs_ptree *pt);
int fs_ptree_grow_leaves(fs_ptree *pt);

fs_ptree *fs_ptree_open(fs_backend *be, fs_rid pred, char pk, int flags, fs_ptable *chain)
{
    char *filename = g_strdup_printf(FS_PTREE, fs_backend_get_kb(be),
                                     fs_backend_get_segment(be), pk, pred);
    fs_ptree *pt = fs_ptree_open_filename(filename, flags, chain);
    g_free(filename);

    return pt;
}

node *node_ref(fs_ptree *pt, nodeid n)
{
    if (IS_LEAF(n)) {
        fs_error(LOG_ERR, "id 0x%x is not a nodeid", n);

        return NULL;
    }
    uint32_t offset = n & 0x7ffffff;
    if (offset == 0 || offset > pt->header->node_base) {
        fs_error(LOG_ERR, "node 0x%x out of range [1,%d]", offset, pt->header->node_base);

        return NULL;
    }

    return pt->nodes+offset;
}

static void map_file(fs_ptree *pt)
{
    pt->ptr = mmap(NULL, pt->file_length, PROT_READ | PROT_WRITE, MAP_SHARED, pt->fd, 0);
    if (pt->ptr == MAP_FAILED) {
        fs_error(LOG_ERR, "failed to mmap '%s'", pt->filename);
    }
    pt->header = pt->ptr;
    pt->nodes = (node *)((char *)(pt->ptr) + sizeof(struct ptree_header));
    pt->leaves = (leaf *)(pt->nodes);
}

fs_ptree *fs_ptree_open_filename(const char *filename, int flags, fs_ptable *chain)
{
    if (sizeof(struct ptree_header) != 512) {
        fs_error(LOG_CRIT, "incorrect ptree header size %zd, should be 512",
                 sizeof(struct ptree_header));

        return NULL;
    }

    fs_ptree *pt = calloc(1, sizeof(fs_ptree));
    pt->fd = open(filename, FS_O_NOATIME | flags, FS_FILE_MODE);
    pt->flags = flags;
    if (pt->fd == -1) {
        fs_error(LOG_ERR, "cannot open ptree file '%s': %s", filename, strerror(errno));
        free(pt);
        return NULL;
    }
    pt->filename = g_strdup(filename);

    if (flags & (O_WRONLY | O_RDWR)) {
        flock(pt->fd, LOCK_EX);
    }
    pt->file_length = lseek(pt->fd, 0, SEEK_END);
    if ((flags & O_TRUNC) || pt->file_length == 0) {
        fs_ptree_write_header(pt);
    } else {
        map_file(pt);
    }

    if (pt->header->id != FS_PTREE_ID) {
        fs_error(LOG_ERR, "%s does not appear to be a ptree file", pt->filename);

        return NULL;
    }
    if (pt->header->revision != FS_PTREE_REVISION) {
        fs_error(LOG_ERR, "%s is not a revision %d ptree", pt->filename, FS_PTREE_REVISION);

        return NULL;
    }

    pt->table = chain;

    if (pt->header->node_free == 0) {
        pt->header->node_free = FS_PTREE_NULL_NODE;
    }

    return pt;
}

int fs_ptree_write_header(fs_ptree *pt)
{
    struct ptree_header header;

    memset(&header, 0, sizeof(header));
    header.id = FS_PTREE_ID;
    header.revision = FS_PTREE_REVISION;
    header.node_size = FS_PTREE_SIZE_INC;
    header.node_alloc = FS_PTREE_SIZE_INC;
    header.node_count = 2;
    header.leaf_size = FS_PTREE_SIZE_INC * sizeof(node) / sizeof(leaf) + FS_PTREE_SIZE_INC;
    header.leaf_alloc = FS_PTREE_SIZE_INC;
    header.leaf_count = 2;
    /* reserve node 0 as the null node, and 1 as the root */
    header.node_base = 2;
    /* reserve the null leaf */
    header.leaf_base = header.node_size * sizeof(node) / sizeof(leaf) + 1;
    header.alloc = header.leaf_size * sizeof(leaf);
    if (pwrite(pt->fd, &header, sizeof(header), 0) == -1) {
        fs_error(LOG_CRIT, "failed to write header on %s: %s",
                 pt->filename, strerror(errno));

        return 1;
    }
    pt->file_length = sizeof(struct ptree_header) + header.alloc;
    char junk = '\0';
    if (pwrite(pt->fd, &junk, 1, pt->file_length) == -1) {
        fs_error(LOG_ERR, "failed to extend ptree file");
    }
    map_file(pt);
    if (msync(pt->ptr, pt->file_length, MS_SYNC))
        fs_error(LOG_ERR, "msync failed, ptree might be inconsistent");
    memset(pt->nodes, 0, sizeof(node));
    memset(pt->leaves, 0, sizeof(leaf));
    node *root = pt->nodes+1;
    for (int i=0; i<FS_PTREE_BRANCHES; i++) {
        root->branch[i] = FS_PTREE_NULL_NODE;
    }

    return 0;
}

int fs_ptree_grow_nodes(fs_ptree *pt)
{
    char junk = '\0';
    pt->header->node_base = pt->header->alloc / sizeof(node);
    pt->header->alloc += FS_PTREE_SIZE_INC * sizeof(node);
    pt->header->node_size = pt->header->alloc / sizeof(node);
    pt->header->node_alloc += FS_PTREE_SIZE_INC;

    /* need to copy this value as were about to unmap */
    off_t alloc = pt->header->alloc;
    if (msync(pt->ptr, pt->file_length, MS_SYNC))
        fs_error(LOG_ERR, "msync failed, ptree might be inconsistent");
    munmap(pt->ptr, pt->file_length);
    pt->file_length = sizeof(struct ptree_header) + alloc;
    if (pwrite(pt->fd, &junk, 1, pt->file_length) == -1) {
        fs_error(LOG_ERR, "failed to grow ptree file");

        return 1;
    }
    map_file(pt);

    return 0;
}

int fs_ptree_grow_leaves(fs_ptree *pt)
{
    char junk = '\0';
    pt->header->leaf_base = pt->header->alloc / sizeof(leaf);
    pt->header->alloc += FS_PTREE_SIZE_INC * sizeof(leaf);
    pt->header->leaf_size = pt->header->alloc / sizeof(leaf);
    pt->header->leaf_alloc += FS_PTREE_SIZE_INC;

    /* need to copy this value as were about to unmap */
    off_t alloc = pt->header->alloc;
    if (msync(pt->ptr, pt->file_length, MS_SYNC))
        fs_error(LOG_ERR, "msync failed, ptree might be inconsistent");
    munmap(pt->ptr, pt->file_length);
    pt->file_length = sizeof(struct ptree_header) + alloc;
    if (pwrite(pt->fd, &junk, 1, pt->file_length) == -1) {
        fs_error(LOG_ERR, "failed to grow ptree file");
        return 1;
    }
    map_file(pt);

    return 0;
}

void fs_ptree_free_node(fs_ptree *pt, nodeid n)
{
    if (IS_LEAF(n)) {
        fs_error(LOG_ERR, "tried to free leaf as node");

        return;
    }
    if (n == FS_PTREE_ROOT_NODE) {
        fs_error(LOG_ERR, "tried to free root node");

        return;
    }
    node *nr = NODE_REF(pt, n);
    nr->branch[0] = pt->header->node_free;
    pt->header->node_free = n;
}

nodeid fs_ptree_new_node(fs_ptree *pt)
{
    if (pt->header->node_free != FS_PTREE_NULL_NODE) {
        nodeid n = pt->header->node_free;
        if (!IS_NODE(n)) {
            fs_error(LOG_ERR, "got free'd node (%08x) that is actually a leaf", n);
        } else {
            node *nr = NODE_REF(pt, n);
            pt->header->node_free = nr->branch[0];
            for (int i=0; i<FS_PTREE_BRANCHES; i++) {
                nr->branch[i] = FS_PTREE_NULL_NODE;
            }

            return n;
        }
    }

    if (pt->header->node_base == pt->header->node_size) {
        fs_ptree_grow_nodes(pt);
    }

    nodeid n = pt->header->node_base | 0x80000000;
    pt->header->node_base++;
    pt->header->node_count++;
    node *nn = NODE_REF(pt, n);
    for (int i=0; i<FS_PTREE_BRANCHES; i++) {
        nn->branch[i] = FS_PTREE_NULL_NODE;
    }

    return n;
}

void fs_ptree_free_leaf(fs_ptree *pt, nodeid n)
{
    if (IS_NODE(n)) {
        fs_error(LOG_ERR, "tried to free node as leaf");

        return;
    }
    leaf *lr = LEAF_REF(pt, n);
    lr->block = pt->header->leaf_free;
    pt->header->leaf_free = n;
}

nodeid fs_ptree_new_leaf(fs_ptree *pt)
{
    if (pt->header->leaf_free != 0) {
        nodeid n = pt->header->leaf_free;
        leaf *lr = LEAF_REF(pt, n);
        pt->header->leaf_free = lr->block;
        for (int i=0; i<FS_PTREE_BRANCHES; i++) {
            lr->pk = FS_RID_NULL;
            lr->block = 0;
            lr->length = 0;
        }

        return n;
    }

    if (pt->header->leaf_base == pt->header->leaf_size) {
        fs_ptree_grow_leaves(pt);
    }

    nodeid n = pt->header->leaf_base;
    pt->header->leaf_base++;
    pt->header->leaf_count++;
    leaf *nl = LEAF_REF(pt, n);
    static const leaf init = { FS_RID_NULL, 0, 0 };
    *nl = init;

    return n;
}

static nodeid get_leaf_and_parent(fs_ptree *pt, fs_rid pk, nodeid *parent)
{
    nodeid pos = FS_PTREE_ROOT_NODE;
    for (int i=0; i < 64/FS_PTREE_BRANCH_BITS; i++) {
        int kbranch = PK_BRANCH(pk, i);
        nodeid newpos = node_ref(pt, pos)->branch[kbranch];
        if (newpos == FS_PTREE_NULL_NODE) {
            return 0;
        } else if (IS_LEAF(newpos)) {
            if (pk == LEAF_REF(pt, newpos)->pk) {
                /* PKs are the same, we can use the block */
                if (parent) *parent = pos;

                return newpos;
            }

            return 0;
        }
        pos = newpos;
    }

    fs_error(LOG_ERR, "fell through get_leaf(%016llx)", pk);
    char tmp[256];
    tmp[0] = '\0';
    for (int i=0; i < 64/FS_PTREE_BRANCH_BITS; i++) {
        int kbranch = PK_BRANCH(pk, i);
        char tmp2[16];
        sprintf(tmp2, "%d.", kbranch);
        strcat(tmp, tmp2);
    }
    fs_error(LOG_ERR, "path was %s", tmp);

    return 0;
}

static nodeid get_leaf(fs_ptree *pt, fs_rid pk)
{
    return get_leaf_and_parent(pt, pk, NULL);
}

static nodeid get_or_create_leaf(fs_ptree *pt, fs_rid pk)
{
    nodeid pos = FS_PTREE_ROOT_NODE;
    for (int i=0; i < 64/FS_PTREE_BRANCH_BITS; i++) {
        int kbranch = PK_BRANCH(pk, i);
again:;
        node *nr = node_ref(pt, pos);
        if (!nr) {
            fs_error(LOG_CRIT, "failed to get node ref");

            break;
        }
        nodeid newpos = nr->branch[kbranch];
        if (newpos == FS_PTREE_NULL_NODE) {
            int done = 0;
            if (i > 1) {
                newpos = fs_ptree_new_leaf(pt);
                LEAF_REF(pt, newpos)->pk = pk;
                done = 1;
            } else {
                newpos = fs_ptree_new_node(pt);
            }
            node_ref(pt, pos)->branch[kbranch] = newpos;

            if (done) {
                return newpos;
            }
        } else if (IS_LEAF(newpos)) {
            const fs_rid existpk = LEAF_REF(pt, newpos)->pk;
            if (pk == existpk) {
                /* PKs are the same, we can reuse the block */
                return newpos;
            }
            /* split and insert node */
            int oldkbr = PK_BRANCH(existpk, i-1);
            nodeid split = fs_ptree_new_node(pt);
            node_ref(pt, pos)->branch[kbranch] = split;
            node_ref(pt, split)->branch[oldkbr] = newpos;
            goto again;
        }
        pos = newpos;
    }

    fs_error(LOG_ERR, "fell through get_or_create_leaf(%016llx)", pk);
    char tmp[256];
    tmp[0] = '\0';
    for (int i=0; i < 64/FS_PTREE_BRANCH_BITS; i++) {
        int kbranch = PK_BRANCH(pk, i);
        char tmp2[16];
        sprintf(tmp2, "%d.", kbranch);
        strcat(tmp, tmp2);
    }
    fs_error(LOG_ERR, "path was %s", tmp);

    return 0;
}

int fs_ptree_add(fs_ptree *pt, fs_rid pk, fs_rid pair[2], int force)
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to add to NULL ptree");

        return 1;
    }
    nodeid lid = get_or_create_leaf(pt, pk);
    if (!pair) return 1;
    leaf *lref = LEAF_REF(pt, lid);
#ifdef FS_INSERT_DEDUP
    if (lref->block && fs_ptable_pair_exists(pt->table, lref->block, pair)) {
        /* nothing to be done */
        return 0;
    }
#endif
    fs_row_id new_block = fs_ptable_add_pair(pt->table, lref->block, pair);
    if (new_block) {
        lref->length++;
        pt->header->count++;
        if (new_block != lref->block) lref->block = new_block;
    }

    return 0;
}

enum recurse_action { NONE, CULL, MERGE };

static enum recurse_action remove_all_recurse(fs_ptree *pt, fs_rid pair[2], nodeid n, int *removed)
{
    node *no = node_ref(pt, n);
    int branches = 0;
    int leaves = 0;
    for (int b=0; b<FS_PTREE_BRANCHES; b++) {
        if (no->branch[b] == FS_PTREE_NULL_NODE) {
            /* dead end, do nothing */
        } else if (IS_LEAF(no->branch[b])) {
            leaf *lref = LEAF_REF(pt, no->branch[b]);
            int sub_removed = 0;
            if (lref->block) {
                fs_row_id newblock = fs_ptable_remove_pair(pt->table,
                                        lref->block, pair, &sub_removed, NULL);
                if (lref->block != newblock) {
                    lref->block = newblock;
                }
                if (sub_removed) {
                    lref->length -= sub_removed;
                }
                if (removed) {
                    (*removed) += sub_removed;
                }
            }
            if (sub_removed && lref->length == 0) {
                fs_ptree_free_leaf(pt, no->branch[b]);
                no->branch[b] = FS_PTREE_NULL_NODE;
            } else {
                branches++;
                leaves++;
            }
        } else {
            enum recurse_action action = remove_all_recurse(pt, pair, no->branch[b], removed);
            if (action == CULL) {
                fs_ptree_free_node(pt, no->branch[b]);
                no->branch[b] = FS_PTREE_NULL_NODE;
            } else if (action == MERGE) {
                node *sub = NODE_REF(pt, no->branch[b]);
                nodeid subn = FS_PTREE_NULL_NODE;
                for (int subb=0; subb<FS_PTREE_BRANCHES; subb++) {
                    if (sub->branch[subb] != FS_PTREE_NULL_NODE) {
                        subn = sub->branch[subb];

                        break;
                    }
                }
                if (subn != FS_PTREE_NULL_NODE) {
                    fs_ptree_free_node(pt, no->branch[b]);
                    no->branch[b] = subn;
                } else {
                    fs_error(LOG_CRIT, "tried to merge nodes, but no children were available");
                }

                branches++;
            } else {
                branches++;
            }
        }
    }
    if (branches == 0) {
        return CULL;
    } else if (branches == 1 && leaves == 1) {
        return MERGE;
    }

    return NONE;
}

static enum recurse_action collapse_by_pk_recurse(fs_ptree *pt, fs_rid pk, fs_index_node n, int level)
{
    node *no = node_ref(pt, n);
    int b = PK_BRANCH(pk, level);
    if (no->branch[b] == FS_PTREE_NULL_NODE) {
        /* dead end, do nothing */

        return NONE;
    } else if (IS_LEAF(no->branch[b])) {
        leaf *lref = LEAF_REF(pt, no->branch[b]);
        if (lref->length == 0 && lref->block == 0) {
            fs_ptree_free_leaf(pt, no->branch[b]);
            no->branch[b] = FS_PTREE_NULL_NODE;
        } else {
            fs_error(LOG_ERR, "hit an unexpected non-empty leaf recursing pk %016llx in %s", pk, pt->filename);
        }
    } else {
        enum recurse_action action = collapse_by_pk_recurse(pt, pk, no->branch[b], level+1);
        if (action == CULL) {
            fs_ptree_free_node(pt, no->branch[b]);
            no->branch[b] = FS_PTREE_NULL_NODE;
        } else if (action == MERGE) {
	    /* MERGE not currently implemented, harmless, but less efficient
             * than it could be */
        }
    }
    int branches = 0;
    for (int c=0; c<FS_PTREE_BRANCHES; c++) {
        if (no->branch[c] != FS_PTREE_NULL_NODE) branches++;
    }
    if (branches == 0) {
        return CULL;
    }

    return NONE;
}

static int collapse_by_pk(fs_ptree *pt, fs_rid pk)
{
    collapse_by_pk_recurse(pt, pk, FS_PTREE_ROOT_NODE, 0);

    return 0;
}

int fs_ptree_remove_all(fs_ptree *pt, fs_rid pair[2])
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to remove from to NULL ptree");

        return 1;
    }
    if (!pair) return 1;

    int removed = 0;
    remove_all_recurse(pt, pair, FS_PTREE_ROOT_NODE, &removed);
    pt->header->count -= removed;

    if (removed) {
        return 0;
    }

    return 1;
}

int fs_ptree_remove(fs_ptree *pt, fs_rid pk, fs_rid pair[2], fs_rid_set *models)
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to remove from to NULL ptree");

        return 1;
    }

    nodeid lid = get_leaf(pt, pk);
    if (!lid) {
        /* the leaf doesn't exist, so it doesn't need to be deleted */

        return 0;
    }
    leaf *lref = LEAF_REF(pt, lid);
    if (!lref->block) {
        fs_error(LOG_ERR, "block for leaf %x not found", lid);

        return 1;
    }

    int removed = 0;
    fs_row_id newblock = fs_ptable_remove_pair(pt->table, lref->block, pair, &removed, models);
    if (lref->block != newblock) {
        lref->block = newblock;
    }
    if (removed) {
        lref->length -= removed;
        pt->header->count -= removed;
        if (lref->length == 0) {
            collapse_by_pk(pt, pk);
#if 0
            node *nref = NODE_REF(pt, parent);
            int collapsed = 0;
            for (int i=0; i<FS_PTREE_BRANCHES; i++) {
                if (nref->branch[i] == lid) {
                    nref->branch[i] = FS_PTREE_NULL_NODE;
                    fs_ptree_free_leaf(pt, lid);
                    collapsed = 1;
                    collapse_by_pk(pt, pk);
                    break;
                }
            }
            if (!collapsed) {
                fs_error(LOG_INFO, "leaf %s/%x is empty, should be removed", pt->filename, lid);
            }
#endif
        }

        return 0;
    }

    return 1;
}

fs_ptree_it *fs_ptree_search(fs_ptree *pt, fs_rid pk, fs_rid pair[2])
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to search NULL ptree");

        return NULL;
    }

    nodeid lid = get_leaf(pt, pk);
    if (!lid) {
        return NULL;
    }
    fs_ptree_it *it = calloc(1, sizeof(fs_ptree_it));
    it->pt = pt;
    it->leaf = LEAF_REF(pt, lid);
    it->length = it->leaf->length;
    it->block = it->leaf->block;
    it->pair[0] = pair[0];
    it->pair[1] = pair[1];

    return it;
}

int fs_ptree_it_get_length(fs_ptree_it *it)
{
    if (it) return it->length;

    return 0;
}

int fs_ptree_it_next(fs_ptree_it *it, fs_rid pair[2])
{
    if (!it) {
        fs_error(LOG_ERR, "tried to iterate NULL iterator");

        return 0;
    }

    (it->step)++;

    while (it->block) {
        int matched = 0;
        fs_rid row[2];
        fs_ptable_get_row(it->pt->table, it->block, row);
        if ((it->pair[0] == FS_RID_NULL ||
             it->pair[0] == row[0]) &&
            (it->pair[1] == FS_RID_NULL ||
             it->pair[1] == row[1])) {
            pair[0] = row[0];
            pair[1] = row[1];
            matched = 1;
        }
        it->block = fs_ptable_get_next(it->pt->table, it->block);

        if (matched) return 1;
    }

    return 0;
}

int fs_ptree_it_next_quad(fs_ptree_it *it, fs_rid quad[4])
{
    fs_error(LOG_CRIT, "not implemented");

    return 0;
}

fs_ptree_it *fs_ptree_traverse(fs_ptree *pt, fs_rid mrid)
{
    fs_ptree_it *it = calloc(1, sizeof(fs_ptree_it));
    it->pt = pt;
    it->traverse = 1;
    it->stack = malloc(sizeof(tree_pos));
    it->pair[0] = mrid;
    it->stack->node = FS_PTREE_ROOT_NODE;
    it->stack->branch = 0;
    it->stack->next = NULL;

    return it;
}

int fs_ptree_traverse_next(fs_ptree_it *it, fs_rid quad[4])
{
    top:;
    while (it->block) {
        int matched = 0;
        fs_rid row[2];
        fs_ptable_get_row(it->pt->table, it->block, row);
        if ((it->pair[0] == FS_RID_NULL ||
             it->pair[0] == row[0])) {
            quad[0] = row[0];
            quad[1] = it->pk;
            /* don't fill out the predicate */
            quad[3] = row[1];
            matched = 1;
        }
        it->block = fs_ptable_get_next(it->pt->table, it->block);

        if (matched) return 1;
    }

    if (!it->stack) return 0;

    it->block = 0;
    tree_pos *pos = it->stack;
    node *no = node_ref(it->pt, pos->node);
    for (int b=pos->branch; b<FS_PTREE_BRANCHES; b++) {
        if (no->branch[b] == FS_PTREE_NULL_NODE) {
            /* dead end, do nothing */
        } else if (IS_LEAF(no->branch[b])) {
            leaf *l = LEAF_REF(it->pt, no->branch[b]);
            it->block = l->block;
            it->pk = l->pk;
            pos->branch = b+1;

            goto top;
        } else {
            tree_pos *newpos = malloc(sizeof(tree_pos));
            newpos->node = no->branch[b];
            newpos->branch = 0;
            newpos->next = pos->next;
            pos->next = newpos;
        }
    }

    it->stack = pos->next;
    free(pos);

    goto top;
}

void fs_ptree_it_free(fs_ptree_it *it)
{
    if (it) free(it);
}

int fs_ptree_count(fs_ptree *pt)
{
    return pt->header->count;
}

int fs_ptree_unlink(fs_ptree *pt)
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to unlink NULL ptree");

        return 1;
    }
    if (!pt->filename) {
        fs_error(LOG_ERR, "tried to unlink closed ptree");

        return 1;
    }

    return unlink(pt->filename);
}

int fs_ptree_close(fs_ptree *pt)
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to close NULL ptree");

        return 1;
    }
    if (!pt->filename) {
        fs_error(LOG_ERR, "tried to close already closed ptree");

        return 1;
    }
    if (munmap(pt->ptr, pt->file_length) == -1) {
        fs_error(LOG_CRIT, "failed to unmap '%s'", pt->filename);
    }
    flock(pt->fd, LOCK_UN);
    close(pt->fd);
    pt->fd = -1;
    g_free(pt->filename);
    pt->filename = NULL;
    free(pt);

    return 0;
}

struct ptree_stats {
    int count;
    int nodes;
    int leaves;
    int deadends;
};

static void recurse_print(fs_ptree *pt, nodeid n, char *buffer, int pos, struct ptree_stats *stats, FILE *out, int verbosity)
{
    unsigned int len = 0;
    node *no = node_ref(pt, n);
    int branches = 0;
    int leaves = 0;
    for (int b=0; b<FS_PTREE_BRANCHES; b++) {
        sprintf(buffer+pos, "%x", b);
        if (no->branch[b] == FS_PTREE_NULL_NODE) {
            (stats->deadends)++;
        } else if (IS_LEAF(no->branch[b])) {
            branches++;
            leaves++;
            (stats->leaves)++;
            buffer[pos+1] ='\0';
            fprintf(out, "%-32s [B%08x, %016llx x %d]\n", buffer,
                    LEAF_REF(pt, no->branch[b])->block,
                    LEAF_REF(pt, no->branch[b])->pk,
                    LEAF_REF(pt, no->branch[b])->length);
            if (pt->table) {
                int check_len = 0;
                fs_ptable_check_consistency(pt->table, out, no->branch[b], LEAF_REF(pt, no->branch[b])->block, &check_len);
                if (LEAF_REF(pt, no->branch[b])->length != check_len) {
                    fprintf(out, "ERROR: tree leaf has length %d, but consistency check says %d\n", LEAF_REF(pt, no->branch[b])->length, check_len);
                }
                if (LEAF_REF(pt, no->branch[b])->length == 0) {
                    if (LEAF_REF(pt, no->branch[b])->block != 0) {
                        fprintf(out, "ERROR: tree leaf has 0 length, but block is non-zero\n");
                    } else {
                        fprintf(out, "ERROR: tree leaf has 0 length, should have been colected\n");
                    }
                } else if (LEAF_REF(pt, no->branch[b])->length < 0) {
                    fprintf(out, "ERROR: tree node length is %d\n", LEAF_REF(pt, no->branch[b])->length < 0);
                } else if (LEAF_REF(pt, no->branch[b])->block == 0) {
                    fprintf(out, "ERROR: tree node references block zero, but has non-zero length\n");
                } else if ((len = fs_ptable_chain_length(pt->table, LEAF_REF(pt, no->branch[b])->block, LEAF_REF(pt, no->branch[b])->length + 100)) != LEAF_REF(pt, no->branch[b])->length) {
                    if (LEAF_REF(pt, no->branch[b])->length + 101 == len) {
                        fprintf(out, "ERROR: probable loop in table, tree node length %d, table length > %d\n",
                            LEAF_REF(pt, no->branch[b])->length,
                            len);
                    } else {
                        fprintf(out, "ERROR: tree node length %d != table length %d\n",
                            LEAF_REF(pt, no->branch[b])->length,
                            len);
                    }
                }
            }
            stats->count += LEAF_REF(pt, no->branch[b])->length;
            for (int c=0; c<pos; c++) {
                buffer[c] = '.';
            }
        } else {
            (stats->nodes)++;
            branches++;
            recurse_print(pt, no->branch[b], buffer, pos+1, stats, out, verbosity);
        }
    }
    if (branches == 1 && leaves == 1 && pos > 2 && n != FS_PTREE_ROOT_NODE) {
        fprintf(out, "ERROR: node %08x has 1 leaf at depth %d, should have "
                     "been merged up\n", n, pos);
    } else if (branches == 0 && n != FS_PTREE_ROOT_NODE) {
        fprintf(out, "ERROR: node %08x has 0 branches at depth %d, should "
                     "have been culled\n", n, pos);
    }
}

void fs_ptree_print(fs_ptree *pt, FILE *out, int verbosity)
{
    fprintf(out, "ptree: %s\n", pt->filename);
    fprintf(out, "nodes: %d/%d\n", pt->header->node_count, pt->header->node_alloc);
    fprintf(out, "leaves: %d/%d\n", pt->header->leaf_count, pt->header->leaf_alloc);
    fprintf(out, "rows:    %lld\n", (long long)pt->header->count);
    fprintf(out, "\n");

    char buffer[256];
    /* tree walk doesn't count the root, or null nodes and leaves */
    struct ptree_stats stats = { 0, 2, 2, 0 };
    recurse_print(pt, FS_PTREE_ROOT_NODE, buffer, 0, &stats, out, verbosity);
    int free_leaves = 0;
    int free_nodes = 0;
    nodeid n = pt->header->leaf_free;
    while (n) {
        free_leaves++;
        leaf *lr = LEAF_REF(pt, n);
        n = lr->block;
    }
    n = pt->header->node_free;
    while (n) {
        free_nodes++;
        node *nr = NODE_REF(pt, n);
        n = nr->branch[0];
    }
    fprintf(out, "\nrows:         %d (%+lld)\n", stats.count, (long long)(stats.count - pt->header->count));
    fprintf(out, "nodes:        %d\n", stats.nodes);
    fprintf(out, "leaves:       %d\n", stats.leaves);
    fprintf(out, "freed nodes:  %d\n", free_nodes);
    fprintf(out, "freed leaves: %d\n", free_leaves);
    fprintf(out, "deadends:     %d (%d%%)\n", stats.deadends, 100 * stats.deadends / (stats.nodes * FS_PTREE_BRANCHES));
    if (stats.count != pt->header->count) {
        fprintf(out, "ERROR: number of rows in header (%d) does not match data (%d) in %s\n", (int)pt->header->count, stats.count, pt->filename);
    }
    if (stats.leaves + free_leaves != pt->header->leaf_count) {
        fprintf(out, "ERROR: %d leaves have been leaked\n",
                     pt->header->leaf_count - free_leaves - stats.leaves);
    }
    if (stats.nodes + free_nodes != pt->header->node_count + 1) {
        fprintf(out, "ERROR: %d nodes have been leaked\n",
                     pt->header->node_count + 1 - free_nodes - stats.nodes);
    }
}

/* vi:set expandtab sts=4 sw=4: */
