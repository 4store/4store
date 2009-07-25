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
 *  Copyright (C) 2007 Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <glib.h>

#include "query.h"
#include "query-cache.h"
#include "query-intl.h"
#include "query-datatypes.h"
#include "common/4store.h"
#include "common/datatypes.h"
#include "common/params.h"
#include "common/error.h"

#define CACHE_SIZE 128

struct _fs_bind_cache {
    int filled;     /* true if cache entry is in use */
    int hits;       /* number of times entry has been used */
    int all;
    int flags;
    int offset;
    int limit;
    fs_rid key[4];  /* the rid vector arguments to bind */
    fs_rid_vector *res[4];
};

/* calls bind as appropriate, plus checks in cache to see if results already
 * present */

int fs_bind_cache_wrapper(fs_query_state *qs, fs_query *q, int all,
                int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit)
{
    g_static_mutex_lock(&qs->cache_mutex);
    if (!qs->bind_cache) {
        qs->bind_cache = calloc(CACHE_SIZE, sizeof(struct _fs_bind_cache));
    }
    g_static_mutex_unlock(&qs->cache_mutex);

    int slots = 0;
    if (flags & FS_BIND_MODEL) slots++;
    if (flags & FS_BIND_SUBJECT) slots++;
    if (flags & FS_BIND_PREDICATE) slots++;
    if (flags & FS_BIND_OBJECT) slots++;

    /* check for no possible bindings */
    for (int s=0; s<4; s++) {
        if (rids[s]->length == 1 && rids[s]->data[0] == FS_RID_NULL) {
            *result = calloc(slots, sizeof(fs_rid_vector));
            for (int s=0; s<slots; s++) {
                (*result)[s] = fs_rid_vector_new(0);
            }

            return 0;
        }
    }

    int cachable = 0;
    fs_rid cache_hash = 0;
    fs_rid cache_key[4];

    /* only consult the cache for optimasation levels 0-2 */
    if (q->opt_level < 3) goto skip_cache;

    cachable = 1;

    cache_hash += all + flags * 2 + offset * 256 + limit * 32768;
    for (int s=0; s<4; s++) {
        if (rids[s]->length == 1) {
            cache_hash ^= (rids[s]->data[0] + s);
            cache_key[s] = rids[s]->data[0];
        } else if (rids[s]->length == 0) {
            cache_key[s] = 0;
        } else {
            cachable = 0;
            break;
        }
    }
    cache_hash %= (CACHE_SIZE - 1);

    g_static_mutex_lock(&qs->cache_mutex);
    if (cachable && qs->bind_cache[cache_hash].filled) {
        int match = 1;
        if (qs->bind_cache[cache_hash].all != all) match = 0;
        if (qs->bind_cache[cache_hash].flags != flags) match = 0;
        if (qs->bind_cache[cache_hash].offset != offset) match = 0;
        if (qs->bind_cache[cache_hash].limit != limit) match = 0;
        for (int s=0; s<4 && match; s++) {
            if (cache_key[s] != qs->bind_cache[cache_hash].key[s]) {
                match = 0;
            }
        }
        if (match) {
            *result = calloc(slots, sizeof(fs_rid_vector));
            for (int s=0; s<slots; s++) {
                (*result)[s] = fs_rid_vector_copy(qs->bind_cache[cache_hash].res[s]);
            }
            qs->bind_cache[cache_hash].hits++;

            g_static_mutex_unlock(&qs->cache_mutex);
            return 0;
        }
    }
    g_static_mutex_unlock(&qs->cache_mutex);

    int ret;

    skip_cache:

    if (all) {
        ret = fsp_bind_limit_all(q->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    } else {
        ret = fsp_bind_limit_many(q->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    }
    if (ret) {
        fs_error(LOG_ERR, "bind failed in '%s', %d segments gave errors",
                 fsp_kb_name(q->link), ret);

        exit(1);
    }
    if (cachable) {
        g_static_mutex_lock(&qs->cache_mutex);
        if (qs->bind_cache[cache_hash].filled == 1) {
          for (int s=0; s<4; s++) {
            fs_rid_vector_free(qs->bind_cache[cache_hash].res[s]);
            qs->bind_cache[cache_hash].res[s] = NULL;
          }
        }
        qs->bind_cache[cache_hash].filled = 1;
        qs->bind_cache[cache_hash].all = all;
        qs->bind_cache[cache_hash].flags = flags;
        qs->bind_cache[cache_hash].offset = offset;
        qs->bind_cache[cache_hash].limit = limit;
        for (int s=0; s<4; s++) {
            qs->bind_cache[cache_hash].key[s] = cache_key[s];
            if (s < slots) {
                qs->bind_cache[cache_hash].res[s] = fs_rid_vector_copy((*result)[s]);
            } else {
                qs->bind_cache[cache_hash].res[s] = NULL;
            }
        }
        g_static_mutex_unlock(&qs->cache_mutex);
    }

    return ret;
}

int fs_query_cache_flush(fs_query_state *qs, int verbosity)
{
    /* assumption: the cache is created once only, ie it can't be pulled out from under us */
    if (!qs->bind_cache) return 1;

    g_static_mutex_lock(&qs->cache_mutex);

    for (int i=0; i<CACHE_SIZE; i++) {
        if (qs->bind_cache[i].filled) {
            if (verbosity > 0) {
                printf("# cache entry %d\n", i);
                printf("#   hits=%d, all=%s, flags=%08x, offset=%d, limit=%d\n", qs->bind_cache[i].hits, qs->bind_cache[i].all ? "true" : "false", qs->bind_cache[i].flags, qs->bind_cache[i].offset, qs->bind_cache[i].limit);
                printf("#   bind(%016llx, %016llx, %016llx, %016llx)\n", qs->bind_cache[i].key[0], qs->bind_cache[i].key[1], qs->bind_cache[i].key[2], qs->bind_cache[i].key[3]);
            }
            qs->bind_cache[i].filled = 0;
            qs->bind_cache[i].all = 0;
            qs->bind_cache[i].flags = 0;
            qs->bind_cache[i].offset = 0;
            qs->bind_cache[i].limit = 0;
            for (int s=0; s<4; s++) {
                qs->bind_cache[i].key[s] = 0;
                fs_rid_vector_free(qs->bind_cache[i].res[s]);
                qs->bind_cache[i].res[s] = 0;
            }
        }
    }
    g_static_mutex_unlock(&qs->cache_mutex);
    
    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
