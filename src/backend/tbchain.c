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
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "backend.h"
#include "tbchain.h"
#include "ptree.h"
#include "../common/params.h"
#include "../common/4s-store-root.h"
#include "../common/error.h"
#include "../common/timing.h"

#define TBCHAIN_ID 0x4a585442  /* JXTB */

#define TBCHAIN_INIT_SIZE 1024

#define FS_PACKED __attribute__((__packed__))

#define FS_TBLOCK_LEN    5  /* in triples */
#define FS_TBLOCK_SIZE 128  /* in bytes */

#define FS_TBCHAIN_SPARSE   1 /* bit set on first block if chain is sparse */
#define FS_TBCHAIN_SUPERSET 2 /* bit set on first block if chain is sparse */

struct fs_tbc_header {
    int32_t id;
    int32_t revision;
    int64_t size;
    int64_t length;
    fs_index_node free_list;
    char padding[484];
};

typedef struct _fs_tblock {
    fs_index_node cont;
    uint8_t       length;
    uint8_t       flags;
    uint8_t       padding[2];
    fs_rid        data[FS_TBLOCK_LEN][3];
} FS_PACKED fs_tblock;

struct _fs_tbchain {
  struct fs_tbc_header *header;
  char *filename;
  void *ptr;            /* mmap ptr + head of file */
  size_t len;           /* length of mmap'd region in bytes */
  int fd;               /* fd of mapped file */
  int flags;            /* flags used in open call */
  fs_tblock *data;      /* array of used buckets, points into mmap'd space */
  fs_index_node *const_data;
  fs_rid *model_data;
                        /* allocated + used when we do consistency checks */
  fs_backend *be;       /* pointer to backend object, used to check if triples
                         * still exist */
};

struct _fs_tbchain_it {
    fs_tbchain *tbc;
    fs_rid model;
    fs_index_node chain;
    fs_index_node node;
    int pos;
    int superset;
};

static int fs_tbchain_free_block(fs_tbchain *bc, fs_index_node b);
static fs_index_node fs_tbchain_new_block(fs_tbchain *bc);

static char *fname_from_label(fs_backend *be, const char *label)
{
    return g_strdup_printf(fs_get_tbchain_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), label);
}

static int unmap_bc(fs_tbchain *bc)
{
    if (!bc) return 1;

    if (bc->ptr) {
        if (msync(bc->ptr, bc->len, MS_SYNC))
            fs_error(LOG_ERR, "msync failed for %s: %s", bc->filename, strerror(errno));
        if (munmap(bc->ptr, bc->len)) {
            fs_error(LOG_CRIT, "munmap failed on %s: %s", bc->filename, strerror(errno));
            return 1;
        }
    }
    bc->ptr = NULL;
    bc->header = NULL;

    return 0;
}

static int map_bc(fs_tbchain *bc, long length, long size)
{
    if (!bc) {
        fs_error(LOG_ERR, "tried to map NULL chain");
        return 1;
    }

    bc->len = (size + 1) * sizeof(fs_tblock) + sizeof(struct fs_tbc_header);
    if (ftruncate(bc->fd, bc->len)) {
        fs_error(LOG_CRIT, "failed to ftruncate %s: %s", bc->filename, strerror(errno));
        return 1;
    }

    int mflags = PROT_READ;
    if (bc->flags & (O_RDWR | O_WRONLY)) mflags |= PROT_WRITE;

    bc->ptr = mmap(NULL, bc->len, mflags, MAP_FILE | MAP_SHARED, bc->fd, 0);
    if (bc->ptr == MAP_FAILED || bc->ptr == NULL) {
        fs_error(LOG_CRIT, "failed to mmap: %s", strerror(errno));
        return 1;
    }

    bc->header = (struct fs_tbc_header *)(bc->ptr);
    if (mflags & PROT_WRITE) {
        bc->header->id = TBCHAIN_ID;
        bc->header->size = size;
        bc->header->length = length;
    }
    bc->data = (fs_tblock *)(((char *)bc->ptr) + sizeof(struct fs_tbc_header));

    return 0;
}

fs_tbchain *fs_tbchain_open(fs_backend *be, const char *label, int flags)
{
    char *fname = fname_from_label(be, label);
    fs_tbchain *c = fs_tbchain_open_filename(fname, flags);
    c->be = be;
    g_free(fname);

    return c;
}

fs_tbchain *fs_tbchain_open_filename(const char *fname, int flags)
{
    struct fs_tbc_header *header;
    fs_tbchain *bc = calloc(1, sizeof(fs_tbchain));

    bc->fd = open(fname, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (bc->fd == -1) {
        fs_error(LOG_CRIT, "failed to open chain %s: %s", fname, strerror(errno));
        free(bc);
        return NULL;
    }
    bc->filename = g_strdup(fname);
    bc->flags = flags;

    if (sizeof(*header) != 512) {
        fs_error(LOG_CRIT, "tbchain header size is %zd, not 512 bytes", sizeof(header));
        free(bc);
        return NULL;
    }
    if (sizeof(fs_tblock) != FS_TBLOCK_SIZE) {
        fs_error(LOG_CRIT, "tblock size is %zd, not %d bytes", sizeof(fs_tblock), FS_TBLOCK_SIZE);
        free(bc);
        return NULL;
    }
    int init_reqd = 0;

    if (bc->flags & O_TRUNC && ftruncate(bc->fd, sizeof(*header))) {
        fs_error(LOG_CRIT, "ftruncate failed on %s: %s", fname, strerror(errno));
        free(bc);
        return NULL;
    }

    int mflags = PROT_READ;
    if (bc->flags & (O_RDWR | O_WRONLY)) mflags |= PROT_WRITE;
    if (bc->flags & O_TRUNC) mflags |= PROT_WRITE;
    header = mmap(NULL, sizeof(*header), mflags, MAP_FILE | MAP_SHARED, bc->fd, 0);
    if (header == MAP_FAILED || header == NULL) {
        munmap(header, sizeof(*header));
        fs_error(LOG_CRIT, "header mmap failed for %s: %s", bc->filename, strerror(errno));
        free(bc);
        return NULL;
    }

    if (bc->flags & O_TRUNC) {
        header->id = TBCHAIN_ID;
        header->size = TBCHAIN_INIT_SIZE;
        header->length = TBCHAIN_INIT_SIZE;
        header->free_list = 0;
        if (msync(header, sizeof(*header), MS_SYNC))
            fs_error(LOG_ERR, "header msync failed for %s: %s", bc->filename, strerror(errno));
        init_reqd = 1;
    }
    if (header->id != TBCHAIN_ID) {
        fs_error(LOG_CRIT, "%s does not look like a tbchain file", bc->filename);
        munmap(header, sizeof(*header));
        return NULL;
    }

    if (map_bc(bc, header->length, header->size)) {
        fs_error(LOG_CRIT, "failed to map %s", bc->filename);
        munmap(header, sizeof(*header));
        return NULL;
    }
    if (munmap(header, sizeof(*header)))
        fs_error(LOG_ERR, "failed to unmap header for %s: %s", bc->filename, strerror(errno));

    if (init_reqd) {
        for (fs_index_node i = 2; i < bc->header->size; i++) {
            fs_tbchain_free_block(bc, i);
        }
    }

    return bc;
}

int fs_tbchain_sync(fs_tbchain *bc)
{
    if (msync(bc->ptr, bc->len, MS_SYNC) == -1) {
        fs_error(LOG_CRIT, "failed to msync chain: %s", strerror(errno));
        return 1;
    }

    return 0;
}

void fs_tbchain_print(fs_tbchain *bc, FILE *out, int verbosity)
{
    fprintf(out, "TBC %p\n", bc);
    fprintf(out, "  image size: %zd bytes\n", bc->len);
    fprintf(out, "  image: %p - %p\n", bc->ptr, bc->ptr + bc->len);
    fprintf(out, "  length: %lld blocks\n", (long long)bc->header->length);
    int free_count = 0;
    fs_index_node i = bc->header->free_list;
    fprintf(out, "  free list:");
    while (i) {
        if (free_count < 10 || bc->data[i].cont == 0) fprintf(out, " %d", i);
        if (free_count == 10) fprintf(out, " ...");
        i = bc->data[i].cont;
        free_count++;
    }
    fprintf(out, "\n");
    fprintf(out, "  free list length: %d blocks\n", free_count);
    for (fs_index_node i=2; i<bc->header->length; i++) {
        if (bc->data[i].length == 0) {
            continue;
        }
        fprintf(out, "  B%d", i);
        if (bc->data[i].cont) {
            fprintf(out, " -> B%d", bc->data[i].cont);
        }
        fprintf(out, " flags: %s %s\n",
                bc->data[i].flags & FS_TBCHAIN_SUPERSET ? "superset" : "-",
                bc->data[i].flags & FS_TBCHAIN_SPARSE ? "sparse" : "-");
        if (bc->data[i].cont < 2 && bc->data[i].cont != 0) {
            fprintf(out, "ERROR: B%d is out of range\n", bc->data[i].cont);
        }
        if (bc->data[i].cont >= bc->header->length) {
            fprintf(out, "ERROR: B%d is out of range\n", bc->data[i].cont);
        }
        if (verbosity > 0) {
            fprintf(out, "  length: %d\n", bc->data[i].length);
            for (int j=0; j<bc->data[i].length; j++) {
                fprintf(out, "    %016llx %016llx %016llx\n", bc->data[i].data[j][0], bc->data[i].data[j][1], bc->data[i].data[j][2]);
            }
        }
    }
}

static fs_index_node fs_tbchain_new_block(fs_tbchain *bc)
{
    TIME(NULL);
    if (!bc->ptr) {
        fs_error(LOG_CRIT, "attempted to get block from unmapped chain");

        return 0;
    }
    if (bc->header->length == 0) {
        fs_error(LOG_CRIT, "length reset, this is a bug, attempting recovery");
        bc->header->length = bc->header->size;
    }

    /* if free list is empty, we need to allocate space */
    if (!bc->header->free_list) {
        int length = bc->header->length;
        int size = bc->header->size;
        unmap_bc(bc);
        map_bc(bc, length, size * 2);
        TIME("grow chain");

        bc->header->length = size * 2;
        for (fs_index_node i = size; i < size * 2; i++) {
            fs_tbchain_free_block(bc, i);
        }
    }

    if (bc->header->free_list) {
        fs_index_node newb = bc->header->free_list;
        bc->header->free_list = bc->data[newb].cont;
        memset(&bc->data[newb], 0, sizeof(fs_tblock));
        TIME("reuse + clear block");

        return newb;
    }

    fs_error(LOG_CRIT, "failed to get free block");

    return 0;
}

fs_index_node fs_tbchain_new_chain(fs_tbchain *bc)
{
    fs_index_node b = fs_tbchain_new_block(bc);
    bc->data[b].cont = 0;

    return b;
}

int fs_tbchain_remove_chain(fs_tbchain *c, fs_index_node b)
{
    if (!c->ptr) {
        fs_error(LOG_CRIT, "attempted to remove block from unmapped chain");

        return 1;
    }
    if (b > c->header->size) {
        fs_error(LOG_CRIT, "tried to remove block B%u, past end of chain\n", b);

        return 1;
    }
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "attempted to remove block B%u", b);

        return 1;
    }
    do {
        fs_index_node next = c->data[b].cont;
        fs_tbchain_free_block(c, b);
        b = next;
        if (b > c->header->size) {
            fs_error(LOG_CRIT, "tried to remove block B%u, past end of chain\n", b);

            return 1;
        }
    } while (b != 0);

    return 0;
}

int fs_tbchain_set_bit(fs_tbchain *bc, fs_index_node b, fs_tbchain_bit bit)
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to set flag on block %u\n", b);

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to set flag past end of chain\n");

        return 0;
    }

    /* flipping bits can cause writes, so lets avoid if possible */
    if (!(bc->data[b].flags & bit)) bc->data[b].flags |= bit;

    return 0;
}

int fs_tbchain_clear_bit(fs_tbchain *bc, fs_index_node b, fs_tbchain_bit bit)
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to clear flag on block %u\n", b);

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to clear flag past end of chain\n");

        return 0;
    }

    /* flipping bits can cause writes, so lets avoid if possible */
    if (bc->data[b].flags & bit) bc->data[b].flags &= ~bit;

    return 0;
}

int fs_tbchain_get_bit(fs_tbchain *bc, fs_index_node b, fs_tbchain_bit bit)
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to read flag on block %u\n", b);

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to read flag on block %u, past end of chain\n", b);

        return 0;
    }

    return bc->data[b].flags & bit;
}

fs_index_node fs_tbchain_add_triple(fs_tbchain *bc, fs_index_node b, fs_rid triple[3])
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to write to block %u\n", b);

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to write past end of chain\n");

        return 0;
    }

    /* insert new block at head */
    if (bc->data[b].length + 1 > FS_TBLOCK_LEN) {
        fs_index_node new_b = fs_tbchain_new_block(bc);

        if (!new_b) {
            fs_error(LOG_CRIT, "count not get new tbchain block, unable to add triple");

            return b;
        }
        bc->data[new_b].cont = b;
        bc->data[new_b].length = 0;
        bc->data[new_b].flags = bc->data[b].flags;
        b = new_b;
    }

    const int len = bc->data[b].length;
    bc->data[b].data[len][0] = triple[0];
    bc->data[b].data[len][1] = triple[1];
    bc->data[b].data[len][2] = triple[2];
    bc->data[b].length++;

    return b;
}

static int fs_tbchain_free_block(fs_tbchain *bc, fs_index_node b)
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to free block %u\n", b);

        return 1;
    }
    if (b >= bc->header->length) {
        fs_error(LOG_CRIT, "tried to free block %u, past end of list\n", b);

        return 1;
    }

    bc->data[b].cont = bc->header->free_list;
    bc->header->free_list = b;

    return 0;
}

fs_index_node fs_tbchain_length(fs_tbchain *bc, fs_index_node b)
{
    if (b == 0 || b == 1) {
        fs_error(LOG_CRIT, "tried to read from block %u\n", b);

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to read past end of chain (%u, length=%lld)\n", b, (long long)bc->header->length);

        return 0;
    }

    int length = 0;
    fs_tblock *bp = &(bc->data[b]);
    while (bp->cont != 0) {
        length += bp->length;
        bp = &(bc->data[bp->cont]);
    }
    length += bp->length;

    return length;
}

unsigned int fs_tbchain_allocated_blocks(fs_tbchain *bc)
{
    return bc->header->length;
}

int fs_tbchain_get_stats(fs_tbchain *bc, fs_index_node chain, FILE *out)
{
    int block_count = 0;
    int triple_count = 0;
    int walk_distance = 1;
    int max_step = 0;

    fs_index_node n = chain;
    while (n) {
        block_count++;
        triple_count += bc->data[n].length;
        if (bc->data[n].cont) {
            walk_distance += abs(n - bc->data[n].cont);
            if (abs(n - bc->data[n].cont) > max_step) {
                max_step = abs(n - bc->data[n].cont);
            }
        }
        n = bc->data[n].cont;
    }
    fprintf(out, "  length (blocks): %d\n", block_count);
    fprintf(out, "  length (triples): %d\n", triple_count);
    fprintf(out, "  efficiency: %f bytes/triple\n", block_count * 128.0 / (double)triple_count);
    fprintf(out, "  walk distance: %d blocks\n", walk_distance);
    fprintf(out, "  avg. step size: %f blocks\n", (double)walk_distance/(double)block_count);
    fprintf(out, "  max. step size: %d blocks\n", max_step);

    return 0;
}

int fs_tbchain_check_consistency(fs_tbchain *bc, fs_rid model, fs_index_node chain, FILE *out)
{
    if (!bc->const_data) {
        bc->const_data = calloc(bc->header->length, sizeof(fs_index_node));
        bc->model_data = calloc(bc->header->length, sizeof(fs_rid));

        if (!bc->const_data || !bc->model_data) {
            fprintf(out, "insufficient memory to do test\n");

            return 1;
        }
    }

    fs_index_node n = chain;
    int retval = 0;
    int count = 0;

    while (n) {
        if (n < 2) {
            fprintf(out, "ERROR: B%d appears in chain %d\n",
                    n, chain);
            retval = 1;
        }
        if (n >= bc->header->length) {
            fprintf(out, "ERROR: B%d appears in chain %d, model %016llx and is "
                    ">= length\n", n, chain, model);
            retval = 1;
        }
        if (bc->const_data[n] && bc->const_data[n] != chain) {
            fprintf(out, "ERROR: B%d appears in chain %d and chain %d, model "
                    "%016llx and model %016llx\n", n, bc->const_data[n], chain,
                    bc->model_data[n], model);
            retval = 1;
        }
        if (bc->data[n].length == 0) {
            fprintf(out, "ERROR: B%d is in chain %d and model %016llx but has "
                    "zero length\n", n, chain, model);
            retval = 1;
        }
        if (bc->const_data[n] == chain) {
            fprintf(out, "ERROR: loop detected at B%d, chain %d, model %016llx and %016llx\n", n, chain, bc->model_data[n], model);
            retval = 1;
            break;
        }
        bc->const_data[n] = chain;
        bc->model_data[n] = model;
        count += bc->data[n].length;
        n = bc->data[n].cont;
    }
    printf("  %d triples\n", count);

    return retval;
}

int fs_tbchain_check_leaks(fs_tbchain *bc, FILE *out)
{
    int retval = 0;

    if (!bc->const_data) {
        fs_error(LOG_CRIT, "chain not tested for consistency");

        return 1;
    }

    for (fs_index_node f = bc->header->free_list; f; f = bc->data[f].cont) {
        if (bc->const_data[f]) {
            fprintf(out, "ERROR: B%d appears in the free list and chain %d\n", f, bc->const_data[f]);
            retval = 1;
        }
        bc->const_data[f] = UINT_MAX;
    }

    int lost_range = 0;
    for (fs_index_node i = 2; i < bc->header->size; i++) {
        if (!bc->const_data[i]) {
            if (lost_range) {
                if (i == bc->header->size - 1 || bc->const_data[i+1]) {
                    fprintf(out, "B%d do not appear in any chain, or the free list\n", i);
                    lost_range = 0;
                }
            } else {
                retval = 1;
                if (i < bc->header->size && bc->const_data[i+1] == 0) {
                    fprintf(out, "ERROR: B%d - ", i);
                    lost_range = 1;
                } else {
                    fprintf(out, "ERROR: B%d does not appear in any chain, or the free list\n", i);
                }
            }
        }
    }

    return retval;
}

int fs_tbchain_unlink(fs_tbchain *bc)
{
    if (!bc) return 1;

    return unlink(bc->filename);
}

int fs_tbchain_close(fs_tbchain *bc)
{
    if (!bc) return 1;

    unmap_bc(bc);
    close(bc->fd);
    g_free(bc->filename);
    free(bc);

    return 0;
}

fs_tbchain_it *fs_tbchain_new_iterator(fs_tbchain *bc, fs_rid model, fs_index_node chain)
{
    if (!bc) {
        fs_error(LOG_ERR, "tried to create iterator from null tbchain");

        return NULL;
    }
    if (chain == 0 || chain == 1) {
        fs_error(LOG_ERR, "tried to create iterator for chain %u", chain);

        return NULL;
    }
    if (chain > bc->header->size) {
        fs_error(LOG_ERR, "tried to create iterator for chain %u, past end",
                 chain);

        return NULL;
    }

    fs_tbchain_it *it = calloc(1, sizeof(fs_tbchain_it));
    if (it == NULL) return NULL;

    it->tbc = bc;
    it->chain = chain;
    it->model = model;
    it->node = chain;
    it->superset = fs_tbchain_get_bit(bc, chain, FS_TBCHAIN_SUPERSET);
    it->pos = 0;

    return it;
}

int fs_tbchain_it_next(fs_tbchain_it *it, fs_rid triple[3])
{
    if (!it) {
        fs_error(LOG_ERR, "fs_tbchain_it_next() passed NULL iterator");

        return 0;
    }

    while (1) {
        if (it->node == 0) {
            triple[0] = FS_RID_NULL;
            triple[1] = FS_RID_NULL;
            triple[2] = FS_RID_NULL;
            if (it->superset) {
                fs_tbchain_clear_bit(it->tbc, it->chain, FS_TBCHAIN_SUPERSET);
            }

            return 0;
        } else if (it->node == 1) {
            fs_error(LOG_CRIT, "iterator ended up at B1");
            triple[0] = FS_RID_NULL;
            triple[1] = FS_RID_NULL;
            triple[2] = FS_RID_NULL;

            return 0;
        } else if (it->pos >= it->tbc->data[it->node].length) {
            it->node = it->tbc->data[it->node].cont;
            it->pos = 0;
        } else if (it->tbc->data[it->node].data[it->pos][0] == FS_RID_GONE) {
            (it->pos)++;
        } else {
            int ok = 1;
            triple[0] = it->tbc->data[it->node].data[it->pos][0];
            triple[1] = it->tbc->data[it->node].data[it->pos][1];
            triple[2] = it->tbc->data[it->node].data[it->pos][2];

            if (it->superset) {
                if (it->tbc->be) {
                    fs_ptree *pt = fs_backend_get_ptree(it->tbc->be, triple[1], 0);
                    fs_rid pair[2] = { it->model, triple[2] };
                    fs_ptree_it *pit = fs_ptree_search(pt, triple[0], pair);
                    if (pit) {
                        fs_rid dummy[2];
                        if (!fs_ptree_it_next(pit, dummy)) {
                            ok = 0;
                        }
                        fs_ptree_it_free(pit);
                    } else {
                        ok = 0;
                    }
                } else {
                    fs_error(LOG_ERR, "backend pointer missing from tbchain, "
                             "returning superset of results");
                }
            }

            if (ok) {
                (it->pos)++;

                return 1;
            } else {
                it->tbc->data[it->node].data[it->pos][0] = FS_RID_GONE;
                fs_tbchain_set_bit(it->tbc, it->chain, FS_TBCHAIN_SPARSE);
                (it->pos)++;
            }
        }
    }
}

int fs_tbchain_it_free(fs_tbchain_it *it)
{
    free(it);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
