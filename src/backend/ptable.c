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
#include "ptable.h"
#include "../common/params.h"
#include "../common/error.h"

#define PTABLE_ID 0x4a585430 /* JXT0 */
#define PTABLE_REVISION 1

struct ptable_header {
    int32_t id;
    int32_t size;
    int32_t length;
    int32_t free_list;
    int32_t revision;
    char padding[492];
};

typedef struct _row {
    fs_row_id cont;
    fs_rid data[2];
} row;

struct _fs_ptable {
  struct ptable_header *header;
  char *filename;
  void *ptr;		/* mmap ptr + head of file */
  size_t len;		/* length of mmap'd region */
  int fd;		/* fd of mapped file */
  int flags;		/* flags used in open call */
  row *data;    	/* array of used rows, points into mmap'd space */
  fs_row_id *cons_data;
};

static char *fname_from_label(fs_backend *be, const char *label)
{
    return g_strdup_printf(FS_PTABLE, fs_backend_get_kb(be), fs_backend_get_segment(be), label);
}

static int unmap_pt(fs_ptable *pt)
{
    if (!pt) return 1;

    if (pt->ptr) {
        fs_ptable_sync(pt);
        if (munmap(pt->ptr, pt->len)) {
            fs_error(LOG_ERR, "failed to unmap ptable %s", pt->filename);
            return 1;
        }
    }

    pt->ptr = NULL;
    pt->header = NULL;
    pt->data = NULL;

    return 0;
}

static int map_pt(fs_ptable *pt, long length, long size)
{
    if (!pt) {
        fs_error(LOG_ERR, "tried to map NULL ptable");
        return 1;
    }

    pt->len = sizeof(struct ptable_header) + size * sizeof(row);
    if (ftruncate(pt->fd, pt->len)) {
        fs_error(LOG_CRIT, "failed to ftruncate ptable %s: %s", pt->filename, strerror(errno));
    }

    int mflags = PROT_READ;
    if (pt->flags & (O_RDWR | O_WRONLY)) mflags |= PROT_WRITE;

    pt->ptr = mmap(NULL, pt->len, mflags, MAP_FILE | MAP_SHARED, pt->fd, 0);
    if (pt->ptr == MAP_FAILED || pt->ptr == NULL) {
        fs_error(LOG_CRIT, "failed to mmap: %s", strerror(errno));
        return 1;
    }

    pt->header = (struct ptable_header *)(pt->ptr);
    if (mflags & PROT_WRITE) {
        pt->header->id = PTABLE_ID;
        pt->header->size = size;
        pt->header->length = length;
        pt->header->revision = PTABLE_REVISION;
    }
    pt->data = (row *)(pt->header + 1);

    return 0;
}

fs_ptable *fs_ptable_open(fs_backend *be, const char *label, int flags)
{
    char *fname = fname_from_label(be, label);
    fs_ptable *c = fs_ptable_open_filename(fname, flags);

    return c;
}

fs_ptable *fs_ptable_open_filename(const char *fname, int flags)
{
    fs_ptable *pt = calloc(1, sizeof(fs_ptable));

    if (sizeof(struct ptable_header) != 512) {
        fs_error(LOG_CRIT, "ptable header size not 512 bytes");
        return NULL;
    }
    pt->fd = open(fname, FS_O_NOATIME | flags, FS_FILE_MODE);
    if (pt->fd == -1) {
        fs_error(LOG_CRIT, "failed to open ptable %s: %s", fname, strerror(errno));
        return NULL;
    }
    pt->filename = g_strdup(fname);
    pt->flags = flags;

    if (flags & O_TRUNC && ftruncate(pt->fd, sizeof(struct ptable_header)))
        fs_error(LOG_CRIT, "ftruncate failed: %s", strerror(errno));

    int mflags = PROT_READ;
    if (flags & (O_RDWR | O_WRONLY)) mflags |= PROT_WRITE;
    if (flags & (O_TRUNC)) mflags |= PROT_WRITE;

    struct ptable_header *header = mmap(NULL, sizeof(struct ptable_header), mflags, MAP_FILE | MAP_SHARED, pt->fd, 0);
    if (header == MAP_FAILED || header == NULL) {
        fs_error(LOG_CRIT, "failed to mmap: %s", strerror(errno));
        return NULL;
    }

    if (flags & O_TRUNC) {
        header->id = PTABLE_ID;
        header->size = 1024;
        header->length = 0;
        header->revision = PTABLE_REVISION;

        if (msync(header, sizeof(*header), MS_SYNC)) {
            fs_error(LOG_CRIT, "could not msync %s: %s", pt->filename, strerror(errno));
            munmap(header, sizeof(*header));
            return NULL;
        }
    }
    if (header->id != PTABLE_ID) {
        fs_error(LOG_CRIT, "%s does not look like a ptable file", pt->filename);
        munmap(header, sizeof(*header));
        return NULL;
    }
    if (header->revision != PTABLE_REVISION) {
        fs_error(LOG_CRIT, "%s is wrong revision of ptable file", pt->filename);
        munmap(header, sizeof(*header));
        return NULL;
    }

    if (map_pt(pt, header->length, header->size))
        return NULL;

    if (munmap(header, sizeof(*header)))
        fs_error(LOG_ERR, "could not unmap %s: %s", pt->filename, strerror(errno));

    return pt;
}

int fs_ptable_sync(fs_ptable *pt)
{
    if (msync(pt->ptr, pt->len, MS_SYNC) == -1) {
        fs_error(LOG_CRIT, "failed to msync ptable: %s", strerror(errno));
        return 1;
    }

    return 0;
}

void fs_ptable_print(fs_ptable *pt, FILE *out, int verbosity)
{
    fprintf(out, "PT %p %s\n", pt, pt->filename);
    fprintf(out, "  image size: %zd bytes\n", pt->len);
    fprintf(out, "  image:      %p - %p\n", pt->ptr, pt->ptr + pt->len);
    fprintf(out, "  length:     %d rows\n", pt->header->length);
    fprintf(out, "  freed:      %d rows\n", fs_ptable_free_length(pt));
    if (verbosity > 0) {
        for (int i=1; i<pt->header->length; i++) {
            fprintf(out, " %cR%08d", i == pt->header->free_list ? 'F' : ' ', i);
            if (verbosity > 1) {
                printf("  %016llx %016llx", pt->data[i].data[0], pt->data[i].data[1]);
            }
            if (pt->data[i].cont) {
                fprintf(out, " -> R%08d\n", pt->data[i].cont);
            } else {
                fprintf(out, "\n");
            }
        }
    }
}

int fs_ptable_check_consistency(fs_ptable *pt, FILE *out, fs_row_id src, fs_row_id start, int *length)
{
    static const fs_row_id free_magic = UINT32_MAX;

    if (src == 0) {
        fprintf(out, "ERROR: tried to check consistency of source 0\n");
        return 1;
    }

    if (!pt->cons_data) {
        pt->cons_data = calloc(pt->header->length, sizeof(fs_row_id));
        if (!pt->cons_data) {
            fprintf(out, "ERROR: cannot allocate enough meory to perform consistency check\n");
            return 1;
        }
        for (fs_row_id f = pt->header->free_list; f; f=pt->data[f].cont) {
            pt->cons_data[f] = free_magic;
        }
    }

    int len = 0;
    for (fs_row_id r = start; r; r = pt->data[r].cont) {
        len++;
        if (pt->cons_data[r] != 0) {
            fprintf(out, "ERROR: some kind of badness\n");
            return 1;
        } else {
            pt->cons_data[r] = src;
        }
    }
    *length = len;

    return 0;
}

/* this can only be called after you've consistency checked all the ptree that
 * refer to a table */
int fs_ptable_check_leaks(fs_ptable *pt, FILE *out)
{
    if (!pt->cons_data) {
        fprintf(out, "ERROR: consistency checks haven't been run\n");
        return 1;
    }

    int leaks = 0;
    for (fs_row_id r = 0; r<pt->header->length; r++) {
        if (pt->cons_data[r] == 0) {
            leaks++;
        }
    }

    if (leaks) {
        fprintf(out, "ERROR: %d rows have leaked\n", leaks);
    }

    return leaks;
}

fs_row_id fs_ptable_new_row(fs_ptable *pt)
{
    if (!pt->ptr) {
        fs_error(LOG_CRIT, "attempted to get row from unmapped ptable");
        return 0;
    }
    if (pt->header->length == 0) {
        pt->header->length = 1;
    }

    /* we can reuse a free'd row */
    if (pt->header->free_list) {
        fs_row_id newr = pt->header->free_list;
        row *r = &pt->data[newr];
        pt->header->free_list = r->cont;
        r->cont = 0;
        r->data[0] = 0;
        r->data[1] = 0;

        return newr;
    }

    if (pt->header->length >= pt->header->size) {
        int length = pt->header->length;
        int size = pt->header->size;
        unmap_pt(pt);
        map_pt(pt, length, size * 2);
    }

    row *r = &pt->data[pt->header->length];
    r->cont = 0;
    r->data[0] = 0;
    r->data[1] = 0;

    return (pt->header->length)++;
}

int fs_ptable_remove_chain(fs_ptable *pt, fs_row_id b)
{
    if (!pt->ptr) {
        fs_error(LOG_CRIT, "attempted to remove row from unmapped ptable");

        return 1;
    }
    if (b > pt->header->size) {
        fs_error(LOG_CRIT, "tried to remove row %08x, past end of ptable\n", b);

        return 1;
    }
    if (b == 0) {
        fs_error(LOG_CRIT, "attempted to remove row 0");

        return 1;
    }
    do {
        fs_row_id next = pt->data[b].cont;
        fs_ptable_free_row(pt, b);
        b = next;
        if (b > pt->header->size) {
            fs_error(LOG_CRIT, "tried to remove bucket %08x, past end of ptable\n", b);

            return 1;
        }
    } while (b != 0);

    return 0;
}

fs_row_id fs_ptable_add_pair(fs_ptable *pt, fs_row_id b, fs_rid pair[2])
{
    if (!pt) {
        fs_error(LOG_CRIT, "tried to add pair to NULL ptable");

        return 0;
    }
    if (!pt->header) {
        fs_error(LOG_CRIT, "tried to add pair to ptable with NULL header");

        return 0;
    }
    if (b > pt->header->length) {
        fs_error(LOG_CRIT, "tried to write off end of ptable %s\n", pt->filename);

        return 0;
    }

    fs_row_id newrid = fs_ptable_new_row(pt);
    row *newr = &(pt->data[newrid]);
    newr->cont = b;
    newr->data[0] = pair[0];
    newr->data[1] = pair[1];

    return newrid;
}

int fs_ptable_get_row(fs_ptable *pt, fs_row_id b, fs_rid pair[2])
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read row 0\n");

        return 1;
    }
    if (b > pt->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of ptable %s (%d > %d / %d)\n", pt->filename, b, pt->header->length, pt->header->size);
        return 1;
    }

    row *r = &(pt->data[b]);
    pair[0] = r->data[0];
    pair[1] = r->data[1];

    return 0;
}

int fs_ptable_pair_exists(fs_ptable *pt, fs_row_id b, fs_rid pair[2])
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read row 0\n");

        return 0;
    }
    if (b > pt->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of ptable %s\n", pt->filename);

        return 0;
    }

    row *r = &(pt->data[b]);
    while (b != 0) {
        if (r->data[0] == pair[0] && r->data[1] == pair[1]) {
            return 1;
        }
        b = r->cont;
        r = &(pt->data[b]);
    }

    return 0;
}

/* we add models to the models set, if the matching RID is set to a wildcard */
fs_row_id fs_ptable_remove_pair(fs_ptable *pt, fs_row_id b, fs_rid pair[2], int *removed, fs_rid_set *models)
{
    fs_row_id ret = b;

    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read row 0");

        return ret;
    }
    if (b > pt->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of ptable %s (%d > %d)", pt->filename, b, pt->header->length);

        return ret;
    }

    /* NULL, NULL means remove everything */
    if (pair[0] == FS_RID_NULL && pair[1] == FS_RID_NULL) {
        /* loop over the chain, count length, remove entries, and set all
         * models to be marked as sparse */
        while (b != 0) {
            (*removed)++;
            row *r = &(pt->data[b]);
            if (models) {
                fs_rid_set_add(models, r->data[0]);
            }
            fs_row_id nextb = r->cont;
            fs_ptable_free_row(pt, b);
            b = nextb;
        }

        return 0;
    }

    row *prevr = NULL;
    while (b != 0) {
        row *r = &(pt->data[b]);
        fs_row_id nextb = r->cont;
        if (pair[0] != FS_RID_NULL && pair[1] == FS_RID_NULL) {
            if (r->data[0] == pair[0]) {
                if (prevr) {
                    prevr->cont = nextb;
                } else {
                    ret = nextb;
                }
                fs_ptable_free_row(pt, b);
                (*removed)++;
            } else {
                prevr = r;
            }
        } else if (pair[0] == FS_RID_NULL && pair[1] != FS_RID_NULL) {
            if (r->data[1] == pair[1]) {
                if (models) {
                    fs_rid_set_add(models, r->data[0]);
                }
                if (prevr) {
                    prevr->cont = nextb;
                } else {
                    ret = nextb;
                }
                fs_ptable_free_row(pt, b);
                (*removed)++;
            } else {
                prevr = r;
            }
        } else if (pair[0] != FS_RID_NULL && pair[1] != FS_RID_NULL) {
            if (r->data[0] == pair[0] && r->data[1] == pair[1]) {
                if (prevr) {
                    prevr->cont = nextb;
                } else {
                    ret = nextb;
                }
                fs_ptable_free_row(pt, b);
                (*removed)++;
            } else {
                prevr = r;
            }
        } else {
            fs_error(LOG_CRIT, "trying to remove with unsupported pattern");
        }
        b = nextb;
    }

    return ret;
}

fs_row_id fs_ptable_get_next(fs_ptable *pt, fs_row_id r)
{
    if (r > pt->header->length) {
        fs_error(LOG_CRIT, "tried to read off end of ptable %s", pt->filename);

        return 0;
    }

    return pt->data[r].cont;
}

int fs_ptable_free_row(fs_ptable *pt, fs_row_id b)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to free row 0\n");

        return 1;
    }

    row *r = &(pt->data[b]);
    r->cont = pt->header->free_list;
    pt->header->free_list = b;

    return 0;
}

unsigned int fs_ptable_chain_length(fs_ptable *pt, fs_row_id b, unsigned int max)
{
    if (b == 0) {
        fs_error(LOG_CRIT, "tried to read from chain 0\n");

        return 0;
    }
    if (b > pt->header->length) {
        fs_error(LOG_CRIT, "tried to read past end of ptable (%x, length=%x)\n", b, pt->header->length);

        return 0;
    }

    int length = 1;
    row *r = &(pt->data[b]);
    if (max) {
        while (r->cont != 0) {
            length++;
            r = &(pt->data[r->cont]);
            if (length > max) {
                fs_error(LOG_ERR, "max length (%d) exceeded", max);
                break;
            }
        }
    } else {
        while (r->cont != 0) {
            length++;
            r = &(pt->data[r->cont]);
        }
    }

    return length;
}

uint32_t fs_ptable_length(fs_ptable *pt)
{
    return pt->header->length;
}

uint32_t fs_ptable_free_length(fs_ptable *pt)
{
    uint32_t ret = 0;

    for (uint32_t i = pt->header->free_list; ret++, i; i=pt->data[i].cont);

    return ret;
}

int fs_ptable_unlink(fs_ptable *pt)
{
    if (!pt) return 1;

    return unlink(pt->filename);
}

int fs_ptable_close(fs_ptable *pt)
{
    if (!pt) return 1;

    unmap_pt(pt);
    close(pt->fd);
    g_free(pt->filename);
    pt->filename = NULL;
    pt->fd = -1;
    free(pt);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
