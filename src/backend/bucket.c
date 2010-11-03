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

#include <syslog.h>

#include "bucket.h"
#include "../common/error.h"

int fs_rid_bucket_get_pair(fs_rid_bucket *b, fs_rid pair[2])
{
    if (pair[0] != FS_RID_NULL && pair[1] != FS_RID_NULL) {
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i] == pair[0] && b->data[i+1] == pair[1]) {
                return 0;
            }
        }
    } else if (pair[0] != FS_RID_NULL) {
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i] == pair[0]) {
                return 0;
            }
        }
    } else if (pair[1] != FS_RID_NULL) {
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i+1] == pair[1]) {
                return 0;
            }
        }
    } else if (pair[0] == FS_RID_NULL && pair[1] == FS_RID_NULL) {
        if (b->length > 0) return 0;
    }

    return 1;
}

int fs_rid_bucket_remove_pair(fs_rid_bucket *b, fs_rid pair[2], int *removed)
{
    if (pair[0] != FS_RID_NULL && pair[1] != FS_RID_NULL) {
        int highest = 0;
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i] == pair[0] && b->data[i+1] == pair[1]) {
                b->data[i] = FS_RID_GONE;
                b->data[i+1] = FS_RID_GONE;
                (*removed)++;
            } else {
                highest = i;
            }
        }
        b->length = highest + 2;
    } else if (pair[0] != FS_RID_NULL) {
        int highest = 0;
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i] == pair[0]) {
                b->data[i] = FS_RID_GONE;
                b->data[i+1] = FS_RID_GONE;
                (*removed)++;
            } else {
                highest = i;
            }
        }
        b->length = highest + 2;
    } else if (pair[1] != FS_RID_NULL) {
        int highest = 0;
        for (unsigned int i=0; i<b->length; i+=2) {
            if (b->data[i+1] == pair[1]) {
                b->data[i] = FS_RID_GONE;
                b->data[i+1] = FS_RID_GONE;
                (*removed)++;
            } else {
                highest = i;
            }
        }
        b->length = highest + 2;
    } else if (pair[0] == FS_RID_NULL && pair[1] == FS_RID_NULL) {
        for (unsigned int i=0; i<b->length; i+=2) {
            b->data[i] = FS_RID_GONE;
            b->data[i+1] = FS_RID_GONE;
        }
        (*removed) += b->length / 2;
        b->length = 0;
    }

    return 0;
}

int fs_rid_bucket_add_single(fs_rid_bucket *b, fs_rid val)
{
    if (b->length + 1 > FS_RID_BUCKET_DATA_LEN) {
        return 1;
    }

    b->data[++(b->length)] = val;

    return 0;
}

int fs_rid_bucket_add_pair(fs_rid_bucket *b, fs_rid pair[2])
{
    if (b->length + 2 > FS_RID_BUCKET_DATA_LEN) {
        fs_error(LOG_ERR, "tried to write past end of bucket");

        return 1;
    }

    for (unsigned int i=0; i<2; i++) {
        b->data[b->length + i] = pair[i];
    }
    (b->length) += 2;

    return 0;
}

int fs_rid_bucket_add_quad(fs_rid_bucket *b, fs_rid quad[4])
{
    if (b->length + 4 > FS_RID_BUCKET_DATA_LEN) {
        fs_error(LOG_ERR, "tried to write past end of bucket");

        return 1;
    }

    for (unsigned int i=0; i<4; i++) {
        b->data[b->length + i] = quad[i];
    }
    (b->length) += 4;

    return 0;
}

int fs_i32_bucket_add_i32(fs_i32_bucket *b, int32_t data)
{
    if (b->length >= FS_I32_BUCKET_DATA_LEN) {
        fs_error(LOG_ERR, "tried to write past end of bucket");

        return 1;
    }

    b->data[b->length] = data;
    (b->length)++;

    return 0;
}

void fs_rid_bucket_print(fs_rid_bucket *b, FILE *out, int verbosity)
{
    if (verbosity > 0) {
        for (int i=0; i < b->length; i++) {
            if (i == 0) fprintf(out, "    row %02x", i);
            else if (i % 2 == 0) fprintf(out, "\n    row %02x", i);
            fprintf(out, " %016llx", b->data[i]);
        }
        fprintf(out, "\n");
    }
}

void fs_i32_bucket_print(fs_i32_bucket *b, FILE *out, int verbosity)
{
    if (verbosity > 0) {
        for (int i=0; i < b->length; i++) {
            if (i == 0) fprintf(out, "    row %02x", i);
            else if (i % 7 == 0) fprintf(out, "\n    row %02x", i);
            fprintf(out, " %08x", b->data[i]);
        }
        fprintf(out, "\n");
    }
}

/* vi:set expandtab sts=4 sw=4: */
