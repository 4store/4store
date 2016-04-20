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
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <glib.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "../common/timing.h"

#include "backend.h"
#include "tree.h"
#include "../common/params.h"
#include "../common/4s-store-root.h"
#include "../common/error.h"

#include "tree-intl.h"

#define DEFAULT_ENTRIES 8

#define TREE_ID 0x4a585454

struct tree_stats {
    int leaves;
    int quads;
    int longest;
    int gt1b;
    int gt10;
    int gt100;
    int gt1000;
    int gt10000;
};

static void descend_branch(fs_tree *t, fs_tree_index *ti, int level, struct tree_stats *count)
{
    int first = 1;

    for (int b=0; b<FS_INDEX_BRANCHES; b++) {
        if (ti->branch[b] == 0) {
            /* do nothing */
        } else if (fs_is_tree_ref(ti->branch[b])) {
            fs_tree_index *i = fs_tree_get_node(t, ti->branch[b]);
            if (!first) {
                putchar('\n');
                for (int c=0; c<level; c++) putchar(' ');
            } else first = 0;
            printf("/%01x", b);
            descend_branch(t, i, level+2, count);
        } else {
            if (!first) {
                putchar('\n');
                for (int c=0; c<level; c++) putchar(' ');
            } else first = 0;
            int length = fs_chain_length(t->bc, ti->branch[b]);
            printf("/%01x/[%08x] x%d", b, ti->branch[b], length);
            count->leaves++;
            count->quads += length;
            if (length > count->longest) count->longest = length;
            if (length > FS_I32_BUCKET_DATA_LEN) count->gt1b++;
            if (length > 10) {
                count->gt10++;
                if (length > 100) {
                    count->gt100++;
                    if (length > 1000) {
                        count->gt1000++;
                        if (length > 10000) {
                            count->gt10000++;
                        }
                    }
                }
            }
        }
    }
}

void fs_tree_node_print(fs_tree *t, fs_index_node node, FILE *out)
{
    struct tree_stats stats = {0, 0, 0, 0, 0, 0};
    int ofs = FS_TREE_OFFSET(node);
    printf("%04x", ofs);
    descend_branch(t, &(t->data[ofs]), 4, &stats);
    putchar('\n');
}

void fs_tree_print(fs_tree *t, FILE *out, int verbosity)
{
    fprintf(out, "tree with %d/%d blocks\n", t->header->length, t->header->size);
    if (t->blacklist) fprintf(out, "  blacklist\n");
    for (struct tree_blacklist *it=t->blacklist; it; it=it->next) {
        fprintf(out, "    %016llx %016llx\n", it->a, it->b);
    }
    fprintf(out, "  chain at %p, length %d\n", t->bc, t->bc->header->length);
    fprintf(out, "  address %p\n", t->ptr);
    if (verbosity > 0) {
        struct tree_stats stats = {0, 0, 0};
        for (int i=0; i<FS_INDEX_ROOTS; i++) {
            int has_branch = 0;
            for (int j=0; j<FS_INDEX_BRANCHES; j++) {
                if (t->data[i].branch[j]) {
                    has_branch = 1;
                    break;
                }
            }
            if (has_branch) {
                printf("%04x", i);
                descend_branch(t, &(t->data[i]), 4, &stats);
                putchar('\n');
            }
        }
        printf("\n");
        printf("leaves          %d\n", stats.leaves);
        printf("total quads:    %d\n", stats.quads);
        printf("average chain:  %.1f\n", (double)stats.quads / (double)stats.leaves);
        printf("longest chain:  %d\n", stats.longest);
        printf("chain > bucket: %d\n", stats.gt1b);
        printf("chain > 10:     %d\n", stats.gt10);
        printf("chain > 100:    %d\n", stats.gt100);
        printf("chain > 1000:   %d\n", stats.gt1000);
        printf("chain > 10000:  %d\n", stats.gt10000);
    }
}

fs_tree *fs_tree_open(fs_backend *be, const char *name, int flags)
{
    char *file = g_strdup_printf(fs_get_tree_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), name);
    fs_tree *t = fs_tree_open_filename(be, name, file, flags);

    return t;
}

static int map_tree(fs_tree *t, size_t length, size_t size)
{
    //TIME(NULL);
    t->len = (size+1) * sizeof(fs_tree_index);
    lseek(t->fd, t->len, SEEK_SET);
    int ret = write(t->fd, "", 1);
    if (ret != 1) {
        fs_error(LOG_CRIT, "failed to grow tree map file");

        return 1;
    }
    t->ptr = mmap(NULL, t->len, PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, t->fd, 0);
    if (t->ptr == (void *)-1) {
        fs_error(LOG_CRIT, "failed to mmap: %s", strerror(errno));
        t->ptr = NULL;

        return 1;
    }
    t->header = (struct tree_header *)(t->ptr);
    t->header->size = size;
    t->header->length = length;
    t->data = (fs_tree_index *)(((char *)t->ptr) + sizeof(struct tree_header));
    //TIME("tree map");

    return 0;
}

static int unmap_tree(fs_tree *t)
{
    munmap(t->ptr, t->len);
    t->ptr = NULL;

    return 0;
}

fs_tree *fs_tree_open_filename(fs_backend *be, const char *name, const char *filename, int flags)
{
    struct tree_header header;

    fs_tree *t = calloc(1, sizeof(fs_tree));
    int fd = open(filename, FS_O_NOATIME | O_RDWR | flags, FS_FILE_MODE);
    if (fd == -1) {
        fs_error(LOG_ERR, "failed to open tree file '%s': %s", filename, strerror(errno));
        free(t);
        return NULL;
    }
    t->filename = (char *)filename;
    t->fd = fd;
    t->be = be;
    int ret = read(fd, &header, sizeof(header));
    if (ret == 0 || flags & O_TRUNC) {
        /* it's a blank file, probably */
        int ret = ftruncate(fd, 0);
        if (ret == -1) {
            fs_error(LOG_WARNING, "failed to truncate treefile");
        }
        t->bc = fs_chain_open(be, name, flags);
        map_tree(t, FS_INDEX_ROOTS, FS_INDEX_ROOTS * 2);
        strncpy(t->header->chainfile, t->bc->filename, 256);
        t->header->id = TREE_ID;

        return t;
    }
    if (header.id != TREE_ID) {
        fs_error(LOG_ERR, "'%s' does not appear to be a valid treefile", filename);
        close(fd);
        free(t);
        return NULL;
    }
    t->bc = fs_chain_open_filename(header.chainfile, flags);
    if (!t->bc) {
        fs_error(LOG_CRIT, "failed to open chain file '%s'", header.chainfile);
        free(t);
        return NULL;
    }

    map_tree(t, header.length, header.size);

    char *blfile = g_strdup_printf("%sbl", t->filename);
    int blfd = open(blfile, FS_O_NOATIME | O_CREAT | O_RDONLY, FS_FILE_MODE);
    if (blfd == -1) {
        fs_error(LOG_WARNING, "failed to open to blacklist file %s: %s",
                blfile, strerror(errno));
    } else {
        int ret;
        fs_rid ab[2];
        do {
            ret = read(blfd, &ab, 2 * sizeof(fs_rid));
            if (ret == 2 * sizeof(fs_rid)) {
                fs_tree_blacklist_index(t, ab[0], ab[1]);
            }
        } while (ret == 2 * sizeof(fs_rid));
        if (ret == -1) {
            fs_error(LOG_ERR, "error reading blacklist file %s: %s", blfile,
                    strerror(errno));
        }
    }
    g_free(blfile);
   
    return t;
}

int fs_tree_close(fs_tree *t)
{
    if (!t) {
        fs_error(LOG_WARNING, "tried to close NULL tree");

        return 1;
    }
    unmap_tree(t);
    close(t->fd);
    fs_chain_close(t->bc);
    char *blfile = g_strdup_printf("%sbl", t->filename);
    int blfd = open(blfile, FS_O_NOATIME | O_CREAT | O_WRONLY | O_TRUNC, FS_FILE_MODE);
    if (blfd == -1) {
        fs_error(LOG_ERR, "failed to write to blacklist file %s: %s", blfile, strerror(errno));
    } else {
        for (struct tree_blacklist *it=t->blacklist; it; it=it->next) {
            int ret = write(blfd, &(it->a), 2 * sizeof(fs_rid));
            if (ret != 2 * sizeof(fs_rid)) {
                fs_error(LOG_ERR, "failed to write blacklist entry for %s", blfile);
                break;
            }
        }
    }
    close(blfd);
    g_free(blfile);
        
    memset(t, 0, sizeof(fs_tree));
    free(t);

    return 0;
}

fs_tree_index *fs_tree_get_node(fs_tree *t, fs_index_node n)
{
    //TIME(NULL);
    if (!fs_is_tree_ref(n)) {
        fs_error(LOG_ERR, "fs_tree_get_node() passed non-tree reference %08x", n);

        return NULL;
    }
    if (t->ptr == NULL) {
        fs_error(LOG_ERR, "attempted to get tree node from unmapped treefile");

        return NULL;
    }
    n = FS_TREE_OFFSET(n);

    if (n > t->header->length) {
        fs_error(LOG_ERR, "tried to fetch tree node %d, past end of nodelist", n);

        return NULL;
    }
    //TIME("tree get node");

    return &(t->data[n]);
}

int fs_tree_get_leaf(fs_tree *t, fs_rid a, fs_rid b, fs_index_node *tnnode_out, int *branch_out, int *level_out)
{
    fs_index_node node = fs_tree_root_from_hash(a);
    fs_tree_index *tn;
    fs_index_node tnnode;

    int level = 0;
    int branch;
    //TIME(NULL);
    do {
        tnnode = node;
        tn = fs_tree_get_node(t, node);
        if (!tn) {
            fs_error(LOG_CRIT, "fetch of node %08x failed", node);
            *tnnode_out = 0;

            return 1;
        }
        branch = fs_tree_branch_from_hash(a, b, level++);
        if (branch < 0) break;
        node = tn->branch[branch];
    } while (fs_is_tree_ref(node));

    *tnnode_out = tnnode;
    *branch_out = branch;
    *level_out = level;
    //TIME("walk tree");

    return 0;
}

fs_index_node fs_tree_add_quad(fs_tree *t, fs_rid a, fs_rid b, fs_rid quad[4])
{
    fs_index_node node = fs_tree_root_from_hash(a);
    fs_tree_index *tn;
    fs_index_node tnnode;

    int level = 0;
    int branch;
    //TIME(NULL);
    do {
        tnnode = node;
        tn = fs_tree_get_node(t, node);
        if (!tn) {
            fs_error(LOG_CRIT, "fetch of node %08x failed", node);

            return 0;
        }
        branch = fs_tree_branch_from_hash(a, b, level++);
        node = tn->branch[branch];
    } while (fs_is_tree_ref(node));
    //TIME("walk tree");

    if (level < 2) {
        node = fs_tree_new_index_node(t);
        /* we need this incase fetching the node has invalidated the old
         * pointer */
        tn = fs_tree_get_node(t, tnnode);
        if (!tn) {
            fs_error(LOG_CRIT, "fetch of node %08x failed", tnnode);

            return 0;
        }
        tn->branch[branch] = node;
        tn = fs_tree_get_node(t, node);
        //TIME("expand tree");
        if (!tn) {
            fs_error(LOG_CRIT, "fetch of node %08x failed", node);

            return 0;
        }
        branch = fs_tree_branch_from_hash(a, b, level++);
//printf("got branch %d\n", branch);
        node = fs_chain_new_bucket(t->bc);
        tn->branch[branch] = node;
        //TIME("expand chain for tree");
    }

    /* theres no chain ref at the leaf of the tree */
    if (tn->branch[branch] == 0) {
        node = fs_chain_new_bucket(t->bc);
        tn->branch[branch] = node;
        //TIME("expand chain");
    }

    fs_index_node tnode;
    if (!(tnode = fs_chain_add_quad(t->bc, tn->branch[branch], quad))) {
        fs_error(LOG_CRIT, "add quad failed");

        return 0;
    }
    tn->branch[branch] = tnode;
    //TIME("add quad");

    return tnode;
}

fs_index_node fs_tree_add_i32(fs_tree *t, fs_rid a, fs_rid b, int32_t data)
{
    fs_tree_index *tn;
    fs_index_node tnnode;

    for (struct tree_blacklist *it=t->blacklist; it; it=it->next) {
        if (it->a == a && it->b == b) {
            return FS_TREE_BLACKLISTED;
        }
    }

    int level;
    int branch;
    //TIME(NULL);
    int ret = fs_tree_get_leaf(t, a, b, &tnnode, &branch, &level);
    if (tnnode == 0) {
        return ret;
    }
    tn = fs_tree_get_node(t, tnnode);
    //TIME("walk tree");

    /* theres no chain ref at the leaf of the tree */
    if (tn->branch[branch] == 0) {
        fs_index_node node = fs_chain_new_bucket(t->bc);
        tn->branch[branch] = node;
        //TIME("expand chain");
    }

    fs_index_node tnode;
    int chainlength = 0;
    if (!(tnode = fs_chain_add_i32(t->bc, tn->branch[branch], data, &chainlength))) {
        fs_error(LOG_CRIT, "add quad failed");

        return 0;
    }
    tn->branch[branch] = tnode;

    //TIME("add i32");
    if (chainlength) {
        if (chainlength > 10000) {
            return FS_TREE_LONG_INDEX;
        }
    }

    return tnode;
}

int fs_tree_sync(fs_tree *t)
{
    //TIME(NULL);
    fs_chain_sync(t->bc);
    if (msync(t->ptr, t->len, MS_SYNC) == -1) {
        fs_error(LOG_CRIT, "failed to msync tree: %s", strerror(errno));

        return 1;
    }
    //TIME("tree sync");

    return 0;
}

fs_index_node fs_tree_new_index_node(fs_tree *t)
{
    //TIME(NULL);
    while (t->header->length >= t->header->size) {
        int length = t->header->length;
        int size = t->header->size;
        unmap_tree(t);
        map_tree(t, length, size + FS_INDEX_ROOTS);
        //TIME("grow tree");
    }

    memset(&t->data[t->header->length], 0, sizeof(fs_tree_index));
    fs_index_node ret = FS_TREE_NODE(t->header->length);
    (t->header->length)++;
    //TIME("new tree node");

    return ret;
}

fs_chain *fs_tree_get_chain(fs_tree *t)
{
    return t->bc;
}

int fs_tree_blacklist_index(fs_tree *t, fs_rid a, fs_rid b)
{
    struct tree_blacklist *nbl = calloc(1, sizeof(struct tree_blacklist));
    nbl->next = t->blacklist;
    nbl->a = a;
    nbl->b = b;
    t->blacklist = nbl;

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
