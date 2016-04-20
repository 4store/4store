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

#include "../common/timing.h"

#include "backend.h"
#include "bucket.h"
#include "chain.h"
#include "../common/params.h"
#include "../common/4s-store-root.h"
#include "../common/error.h"

#define CHAIN_ID 0x4a584230

static char *fname_from_label(fs_backend *be, const char *label)
{
  return g_strdup_printf(fs_get_chain_format(), fs_backend_get_kb(be), fs_backend_get_segment(be), label);
}

static int unmap_bc(fs_chain *bc)
{
    if (!bc) return 1;

    if (bc->ptr) munmap(bc->ptr, bc->len);
    bc->ptr = NULL;
    bc->header = NULL;

    return 0;
}

static int map_bc(fs_chain *bc, long length, long size)
{
    if (!bc) {
        fs_error(LOG_ERR, "tried to map NULL chain");

        return 1;
    }

    bc->len = (size + 1) * sizeof(fs_bucket) + sizeof(struct fs_bc_header);
    if (lseek(bc->fd, bc->len, SEEK_SET) == -1) {
        fs_error(LOG_CRIT, "failed to seek in chain file %s: %s", bc->filename, strerror(errno));
    }
    if (bc->flags & (O_RDWR | O_WRONLY) && write(bc->fd, "", 1) == -1) {
        fs_error(LOG_CRIT, "failed to init chain file %s (fd %d): %s", bc->filename, bc->fd, strerror(errno));
    }
    int mflags = PROT_READ;
    if (bc->flags & (O_RDWR | O_WRONLY)) mflags |= PROT_WRITE;
    bc->ptr = mmap(NULL, bc->len, mflags, MAP_FILE | MAP_SHARED, bc->fd, 0);
    if (bc->ptr == (void *)-1 || bc->ptr == NULL) {
        fs_error(LOG_CRIT, "failed to mmap: %s", strerror(errno));

        return 1;
    }
    bc->header = (struct fs_bc_header *)(bc->ptr);
    if (mflags & PROT_WRITE) {
        bc->header->id = CHAIN_ID;
        bc->header->size = size;
        bc->header->length = length;
    }
    bc->data = (fs_bucket *)(((char *)bc->ptr) + sizeof(struct fs_bc_header));

    return 0;
}

fs_chain *fs_chain_open(fs_backend *be, const char *label, int flags)
{
    char *fname = fname_from_label(be, label);
    fs_chain *c = fs_chain_open_filename(fname, flags);

    return c;
}

fs_chain *fs_chain_open_filename(const char *fname, int flags)
{
    fs_chain *bc = calloc(1, sizeof(fs_chain));

    bc->fd = open(fname, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (bc->fd == -1) {
        fs_error(LOG_CRIT, "failed to open chain %s: %s", fname, strerror(errno));
        free(bc);
        return NULL;
    }
    bc->filename = g_strdup(fname);
    bc->flags = flags;
    struct fs_bc_header header;
    lseek(bc->fd, 0, SEEK_SET);
    if (read(bc->fd, &header, sizeof(header)) <= 0) {
        header.id = CHAIN_ID;
        header.size = 1024;
        header.length = 0;
    }
    if (header.id != CHAIN_ID) {
        fs_error(LOG_CRIT, "%s does not look like a chain file", bc->filename);
        free(bc);
        return NULL;
    }
    if (map_bc(bc, header.length, header.size)) {
        return NULL;
    }

    return bc;
}

int fs_chain_sync(fs_chain *bc)
{
    if (msync(bc->ptr, bc->len, MS_SYNC) == -1) {
        fs_error(LOG_CRIT, "failed to msync chain: %s", strerror(errno));

        return 1;
    }

    return 0;
}

void fs_chain_print(fs_chain *bc, FILE *out, int verbosity)
{
    fprintf(out, "BC %p\n", bc);
    fprintf(out, "  image size: %zd bytes\n", bc->len);
    fprintf(out, "  image: %p - %p\n", bc->ptr, bc->ptr + bc->len);
    fprintf(out, "  length: %d buckets\n", bc->header->length);
    for (int i=1; i<bc->header->length; i++) {
        fprintf(out, "  B%08x", i);
        if (bc->data[i].cont) {
            fprintf(out, " -> B%08x\n", bc->data[i].cont);
        } else {
            fprintf(out, "\n");
        }
        if (verbosity > 0) {
            // alternative: fs_i32_bucket_print((fs_i32_bucket *)&(bc->data[i]), out, verbosity-1);
            fs_rid_bucket_print((fs_rid_bucket *)&(bc->data[i]), out, verbosity-1);
        }
    }
}

fs_index_node fs_chain_new_bucket(fs_chain *bc)
{
    TIME(NULL);
    if (!bc->ptr) {
        fs_error(LOG_CRIT, "attempted to get bucket from unmapped chain");

        return 0;
    }
    if (bc->header->length == 0) {
        bc->header->length = 1;
    }

    /* we can reuse a free'd bucket */
    if (bc->header->free_list) {
        fs_index_node newb = bc->header->free_list;
        fs_bucket *b = &bc->data[newb];
        bc->header->free_list = b->cont;
        b->cont = 0;
        b->length = 0;

        return newb;
    }
        
    if (bc->header->length >= bc->header->size) {
        int length = bc->header->length;
        int size = bc->header->size;
        unmap_bc(bc);
        map_bc(bc, length, size * 2);
        TIME("grow chain");
    }

    fs_bucket *b = &bc->data[bc->header->length];
    b->cont = 0;
    b->length = 0;
    TIME("clear bucket");

    return (bc->header->length)++;
}

int fs_chain_remove(fs_chain *c, fs_index_node b)
{
    if (!c->ptr) {
        fs_error(LOG_CRIT, "attempted to remove bucket from unmapped chain");

        return 1;
    }
    if (b > c->header->size) {
        fs_error(LOG_CRIT, "tried to remove bucket %08x, past end of chain\n", b);

        return 1;
    }
    if (b == 0) {
        fs_error(LOG_CRIT, "attempted to remove bucket 0");

        return 1;
    }
    do {
        fs_index_node next = c->data[b].cont;
        fs_chain_free_bucket(c, b);
        b = next;
        if (b > c->header->size) {
            fs_error(LOG_CRIT, "tried to remove bucket %08x, past end of chain\n", b);

            return 1;
        }
    } while (b != 0);

    return 0;
}

#if 0
fs_index_node fs_chain_add_single(fs_chain *bc, fs_index_node b, fs_rid val)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to write to bucket 0\n");

        return 0;
    }
    if (b > bc->size) {
        fs_error(LOG_CRIT, "tried to write off end of chain\n");

        return 0;
    }

    fs_rid_bucket *rb = (fs_rid_bucket *)&(bc->data[b]);
    fs_index_node oldb = b;
    while (rb->length + 1 > FS_RID_BUCKET_DATA_LEN) {
        oldb = b;
        b = rb->cont;
        rb = (fs_rid_bucket *)&(bc->data[b]);
    }
    if (b == 0) {
        b = fs_chain_new_bucket(bc);
        fs_rid_bucket *old = (fs_rid_bucket *)(&(bc->data[oldb]));
        old->cont = b;

        if (!b) return 0;
    }

    if (fs_rid_bucket_add_single((fs_rid_bucket *)&(bc->data[b]), val)) {
        return 0;
    } else {
        return b;
    }
}
#endif

fs_index_node fs_chain_add_quad(fs_chain *bc, fs_index_node b, fs_rid quad[4])
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to write to bucket 0\n");

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to write off end of chain\n");

        return 0;
    }

    fs_rid_bucket *rb = (fs_rid_bucket *)&(bc->data[b]);
    fs_index_node oldb;
    while (rb->length + 4 > FS_RID_BUCKET_DATA_LEN && b != 0) {
        if (rb->length > FS_RID_BUCKET_DATA_LEN) {
            fs_error(LOG_CRIT, "sanity check failed for bucket %d: length"
                     " = %d", b, rb->length);
            return 0;
        }
        oldb = b;
        b = rb->cont;
        rb = (fs_rid_bucket *)&(bc->data[b]);
    }
    if (b == 0) {
        b = fs_chain_new_bucket(bc);
        bc->data[oldb].cont = b;

        if (!b) return 0;
    }

    if (fs_rid_bucket_add_quad((fs_rid_bucket *)&(bc->data[b]), quad)) {
        return 0;
    } else {
        return b;
    }
}

fs_index_node fs_chain_add_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2])
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to write to bucket 0\n");

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to write off end of chain\n");

        return 0;
    }

    fs_rid_bucket *rb = (fs_rid_bucket *)&(bc->data[b]);
    fs_index_node oldb = b;
/* this code tries to fill in blanks, we should cache wether the chain is
 * sparse or not 
    while (rb->length + 2 > FS_RID_BUCKET_DATA_LEN && b != 0) {
        if (rb->length > FS_RID_BUCKET_DATA_LEN) {
            fs_error(LOG_CRIT, "sanity check failed for bucket %d: length"
                     " = %d", b, rb->length);
            return 0;
        }
        oldb = b;
        b = rb->cont;
        rb = (fs_rid_bucket *)&(bc->data[b]);
    }
*/
    if (rb->length + 2 > FS_RID_BUCKET_DATA_LEN) {
        b = fs_chain_new_bucket(bc);
        bc->data[b].cont = oldb;

        if (!b) return 0;
    }

    if (fs_rid_bucket_add_pair((fs_rid_bucket *)&(bc->data[b]), pair)) {
        return 0;
    } else {
        return b;
    }
}

int fs_chain_get_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2])
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read bucket 0\n");

        return 1;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of chain\n");

        return 1;
    }

    fs_rid_bucket *rb = (fs_rid_bucket *)&(bc->data[b]);
    while (b != 0) {
        if (rb->length > FS_RID_BUCKET_DATA_LEN) {
            fs_error(LOG_CRIT, "sanity check failed for bucket %d: length"
                     " = %d", b, rb->length);
            return 1;
        }
        if (!fs_rid_bucket_get_pair(rb, pair)) {
            return 0;
        }
        b = rb->cont;
        rb = (fs_rid_bucket *)&(bc->data[b]);
    }

    return 1;
}

int fs_chain_remove_pair(fs_chain *bc, fs_index_node b, fs_rid pair[2], int *removed)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read bucket 0");

        return 1;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of chain");

        return 1;
    }

    while (b != 0) {
        fs_rid_bucket *rb = (fs_rid_bucket *)&(bc->data[b]);
        if (rb->length > FS_RID_BUCKET_DATA_LEN) {
            fs_error(LOG_CRIT, "sanity check failed for bucket %d: length"
                     " = %d", b, rb->length);
            return 1;
        }
        fs_rid_bucket_remove_pair(rb, pair, removed);
        b = rb->cont;
    }

    return 1;
}

fs_index_node fs_chain_add_i32(fs_chain *bc, fs_index_node b, int32_t data, int *newlength)
{
    const fs_index_node bin = b;
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to write to bucket 0\n");

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to write off end of chain\n");

        return 0;
    }

    TIME("??? unknown ???");
    // TODO could set flag to indicate that chain is sparse
    fs_bucket *rb = &(bc->data[b]);
    while (b != 0 && rb->length >= FS_I32_BUCKET_DATA_LEN) {
        if (rb->length > FS_I32_BUCKET_DATA_LEN) {
            fs_error(LOG_CRIT, "sanity check failed for bucket %d: length = %d", b, rb->length);
            return 0;
        }
        b = rb->cont;
        rb = b ? &(bc->data[b]) : NULL;
    }
       
    TIME("add i32 walk chain");

    if (b == 0) {
        if (newlength) *newlength = fs_chain_length(bc, bin);
        b = fs_chain_new_bucket(bc);
        bc->data[b].cont = bin;

        if (!b) return 0;
    }
    TIME("chain length");

    if (fs_i32_bucket_add_i32((fs_i32_bucket *)&(bc->data[b]), data)) {
        return 0;
    } else {
        return b;
    }
    TIME("bucket add i32");
}

int fs_chain_free_bucket(fs_chain *bc, fs_index_node b)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to free bucket 0\n");

        return 1;
    }
    
    fs_bucket *bp = &(bc->data[b]);
    bp->cont = bc->header->free_list;
    bc->header->free_list = b;

    return 0;
}

unsigned int fs_chain_length(fs_chain *bc, fs_index_node b)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read from bucket 0\n");

        return 0;
    }
    if (b > bc->header->length) {
        fs_error(LOG_CRIT, "tried to read past end of chain (%x, length=%x)\n", b, bc->header->length);

        return 0;
    }
    int length = 0;

    fs_bucket *bp = &(bc->data[b]);
    while (bp->cont != 0) {
        length += bp->length;
        bp = &(bc->data[bp->cont]);
    }
    length += bp->length;

    return length;
}

int fs_chain_close(fs_chain *bc)
{
    if (!bc) return 1;

    unmap_bc(bc);
    close(bc->fd);
    g_free(bc->filename);
    free(bc);

    return 0;
}

static inline int fs_is_tree_ref(fs_index_node x)
{
    return x & (fs_index_node)(0x80000000);
}

fs_bucket *fs_chain_get_bucket(fs_chain *c, fs_index_node b)
{
    if (fs_is_tree_ref(b)) {
        fs_error(LOG_ERR, "tried to fetch tree node from chain");

        return NULL;
    }
    if (b > c->header->length) {
        fs_error(LOG_ERR, "tried to fetch bucket past end of chain");

        return NULL;
    }
    if (b == 0) return NULL;

    return &(c->data[b]);
}

/* vi:set expandtab sts=4 sw=4: */
