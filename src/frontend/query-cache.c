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

    Copyright (C) 2007 Steve Harris for Garlik
    Copyright (C) 2012 Manuel Salvadores for Stanford University
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
#include "../common/bit_arr.h"
#include "../common/4store.h"
#include "../common/params.h"
#include "../common/error.h"
#include "../common/4s-internals.h"
#include "../common/rdf-constants.h"

#define CACHE_SIZE 1024

struct _fs_bind_cache {
    int filled;     /* true if cache entry is in use */
    int hits;       /* number of times entry has been used */
    int all;
    int flags;
    int offset;
    int limit;      /* the soft limit which was used to do the bind */
    int limited;    /* number of times the soft limit constrained results */
    fs_rid key[4];  /* the rid vector arguments to bind */
    fs_rid_vector *res[4];
};

struct _acl_sec_tuple {
  int set_size;
  fs_rid_set *res;
  fs_rid user_key;
};

/**
* helper to count # of element bound
*/
static int fs_slots_n(int f) {
    int slots = 0;
    if (f & FS_BIND_MODEL) slots++;
    if (f & FS_BIND_SUBJECT) slots++;
    if (f & FS_BIND_PREDICATE) slots++;
    if (f & FS_BIND_OBJECT) slots++;
    return slots;
}

/**
* aux function to traverse g_hash_table of graph acls.
*/
void key_in(gpointer key, gpointer value, gpointer user_data) {
  fs_rid *g = (fs_rid *)key;
  struct _acl_sec_tuple *t = (struct _acl_sec_tuple *) user_data;
  fs_rid_set *graph_keys = (fs_rid_set *)value;
  if (!fs_rid_set_contains(graph_keys,t->user_key)) {
     fs_rid_set_add(t->res,*g);
     t->set_size++;
  }
}

/**
* decides wheter a user rid is admin or not
*/
static int is_admin(fs_rid_set *admins, fs_rid user_rid) {
  /* if no admin users then all users are admins */
  if (!admins)
    return 1;
  return fs_rid_set_contains(admins,user_rid); 
}

/**
* given an api key it returns a set of graphs not accesible by this user/api_key
*/
static fs_rid_set *no_access_for_user(fs_acl_system_info *acl_info, fs_rid user_rid) {
  if (!acl_info || is_admin(acl_info->admin_user_set, user_rid))
    return NULL;


  if (acl_info->acl_graph_hash) {
      fs_rid_set *res = fs_rid_set_new();
      struct _acl_sec_tuple t = { .set_size = 0, .res = res, .user_key = user_rid };
      g_hash_table_foreach(acl_info->acl_graph_hash, key_in , &t);
      if (t.set_size)
        return res;
      return NULL;
  }
  return NULL;
}

/**
* It discards graph rows that are not accesible by the query user 
* m is the models (column 0) result of the bind
* inv_acl is the set of graphs that the query cannot access 
* discarded returns a bit_array by reference with 0's in not accesible rows
*/
int fs_mark_discard_rows(fs_rid_vector *m, fs_rid_set *inv_acl, unsigned char **discarded) {

     int qdiscarded = 0;
     unsigned char *allow_access = NULL;

     if (inv_acl) {
        int count = fs_rid_vector_length(m);
        for (int i=0 ; i<count ; i++) {
            fs_rid rid_m = m->data[i];
            if (fs_rid_set_contains(inv_acl, rid_m)) {
                if (!allow_access) allow_access = fs_new_bit_array(count);
                fs_bit_array_set(allow_access,i,0);
                qdiscarded++;
            }
        }
      }
    
     if (allow_access)
        *discarded = allow_access;

     return qdiscarded;
}

/**
* aux functions to destroy keys and values in 
* acl_graph_hash
*/
static void acl_key_destroyed(gpointer pdata) {
    fs_rid *data = (fs_rid *) pdata;
    free(data);
}
static void acl_value_destroyed(gpointer pdata) {
    fs_rid_set *data = (fs_rid_set *) pdata;
    fs_rid_set_free(data);
}

/**
* It loads the acl system info from the system:config graph. 
* Due to link->acl_system_info manipulation this function should be
* under mutex conditions.
*/
int fs_acl_load_system_info(fsp_link *link) {
    
    if (!fsp_acl_needs_reload(link))
        return 0;

    int flags = FS_BIND_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT | FS_BIND_BY_SUBJECT;
    fs_rid_vector *mrids = fs_rid_vector_new_from_args(1, fs_c.system_config);
    fs_rid_vector *srids = fs_rid_vector_new(0);
    fs_rid_vector *prids = fs_rid_vector_new_from_args(2, fs_c.fs_acl_admin, fs_c.fs_acl_access_by);
    fs_rid_vector *orids = fs_rid_vector_new(0);
    fs_rid_vector **result = NULL;
    fsp_bind_limit_all(link, flags, mrids, srids, prids, orids, &result, -1, -1);
    fs_rid_vector_free(mrids);
    fs_rid_vector_free(srids);
    fs_rid_vector_free(prids);
    fs_rid_vector_free(orids);
    int admin_users_count = 0;
    fs_acl_system_info *acl_system_info = link->acl_system_info;
    if (result && result[0]) {
        if (!acl_system_info->acl_graph_hash || acl_system_info->admin_user_set)
            link->acl_system_info = acl_system_info;
        if (acl_system_info->acl_graph_hash) {
            g_hash_table_steal(acl_system_info->acl_graph_hash, &fs_c.system_config);
            g_hash_table_destroy(acl_system_info->acl_graph_hash);
            acl_system_info->acl_graph_hash = NULL;
        }
        acl_system_info->acl_graph_hash = g_hash_table_new_full(fs_rid_hash,fs_rid_equal,
        acl_key_destroyed, acl_value_destroyed);
        if (acl_system_info->admin_user_set) {
            fs_rid_set_free(acl_system_info->admin_user_set);
            acl_system_info->admin_user_set = NULL;
        }
        acl_system_info->admin_user_set = fs_rid_set_new();
        

        for (int row = 0; row < result[0]->length; row++) {
            if(result[1]->data[row] == fs_c.fs_acl_access_by) {
                /* if pred is acl_access_by then subject is the graph and object is the user rid */
                gpointer users_set_ref = NULL;
                fs_rid_set *users_set = NULL;
                if (!(users_set_ref=g_hash_table_lookup(acl_system_info->acl_graph_hash, &result[0]->data[row]))) {
                    users_set = fs_rid_set_new();
                    fs_rid *rid_graph = malloc(sizeof(fs_rid));
                    *rid_graph = result[0]->data[row];
                    g_hash_table_insert(acl_system_info->acl_graph_hash, rid_graph, users_set);
                } else
                    users_set = (fs_rid_set *) users_set_ref;
                fs_rid_set_add(users_set, result[2]->data[row]);
            } else if (result[1]->data[row] == fs_c.fs_acl_admin) {
                /* if admin predicate then object contains the admin user rid id */
                fs_rid_set_add(acl_system_info->admin_user_set, result[2]->data[row]);
                admin_users_count++;
            }
        }
        if (admin_users_count == 0) {
            fs_error(LOG_ERR,"Added default admin user %s",FS_ACL_DEFAULT_ADMIN);
            fs_rid_set_add(acl_system_info->admin_user_set, fs_c.fs_acl_default_admin);
        }
        /* only admin users can access system:config */
        g_hash_table_insert(acl_system_info->acl_graph_hash, &fs_c.system_config, acl_system_info->admin_user_set);
    }
    fsp_acl_reloaded(link);
    if (result) {
        for (int i=0;i<3;i++) {
            fs_rid_vector_free(result[i]);
        }
        free(result);
    }
    return 1;
}

/* calls bind as appropriate, plus checks in cache to see if results already
 * present */

int fs_bind_cache_wrapper_intl(fs_query_state *qs, fs_query *q, int all,
                int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit)
{
    g_static_mutex_lock(&qs->cache_mutex);
    if (!qs->bind_cache) {
        qs->bind_cache = calloc(CACHE_SIZE, sizeof(struct _fs_bind_cache));
    }
    if (fsp_acl_needs_reload(qs->link))
         fs_acl_load_system_info(qs->link);

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
    if (q && q->opt_level < 3) goto skip_cache;

    if (q && q->qs && q->qs->cache_stats) q->qs->bind_hits++;
    cachable = 1;

    cache_hash += all + flags * 2 + offset * 256 + limit * 32768;
    for (int s=0; s<4; s++) {
        if (rids[s]->length == 1) {
            cache_hash ^= (rids[s]->data[0] + s);
            cache_key[s] = rids[s]->data[0];
        } else if (rids[s]->length == 0) {
            cache_key[s] = 0;
        } else {
           /* bind cache does not cache binds with any
              slot containing multiple values */
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
            fsp_hit_limits_add(qs->link, qs->bind_cache[cache_hash].limited);
            qs->bind_cache[cache_hash].hits++;

            if (q && q->qs && q->qs->cache_stats) q->qs->bind_cache_success++;
            g_static_mutex_unlock(&qs->cache_mutex);
            return 0;
        }
    }
    g_static_mutex_unlock(&qs->cache_mutex);

    int ret;

    skip_cache:;

    int limited_before = fsp_hit_limits(qs->link);
    if (all) {
        ret = fsp_bind_limit_all(qs->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    } else {
        ret = fsp_bind_limit_many(qs->link, flags, rids[0], rids[1], rids[2], rids[3], result, offset, limit);
    }
    int limited = fsp_hit_limits(qs->link) - limited_before;
    if (ret) {
        fs_error(LOG_ERR, "bind failed in '%s', %d segments gave errors",
                 fsp_kb_name(qs->link), ret);

        exit(1);
    }

    int small = 1;
    for (int s=0; s<slots; s++) {
        if (fs_rid_vector_length((*result)[s]) > 10000) {
            small = 0;
            break;
        }
    }

    if (cachable && small && slots > 0) {
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
        qs->bind_cache[cache_hash].limited = limited;
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

/**
* It wraps up the bind operation to discard rows from the result that cannot be accessed.
*/
int fs_bind_cache_wrapper_intl_acl(fs_query_state *qs, fs_query *q, int all,
                int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit) {
    int flags_copy = flags;
    int ndiscarded = 0;
    if (fsp_is_acl_enabled(qs->link)) {
        flags = flags | FS_BIND_MODEL;
    }
    int ret = fs_bind_cache_wrapper_intl(qs, q, all, flags, rids, result, offset, limit);
    if (fsp_is_acl_enabled(qs->link) && (*result)) {
        unsigned char *rows_discarded = NULL;
        /* TODO probably this can be done with one iteration of results */
        fs_rid_set *inv_acl = no_access_for_user(qs->link->acl_system_info,q->apikey_rid); 
        ndiscarded = fs_mark_discard_rows((*result)[0], inv_acl, &rows_discarded);
        if (inv_acl)
            fs_rid_set_free(inv_acl);
        int slots = fs_slots_n(flags_copy);
        if (!(flags_copy & FS_BIND_MODEL) && (flags & FS_BIND_MODEL)) {
            fs_rid_vector **result_copy = calloc(slots, sizeof(fs_rid_vector));
            for (int i=0;i<slots;i++)
                result_copy[i] = (*result)[i+1];
            fs_rid_vector_free((*result)[0]);
            free(*result);
            *result = result_copy;
        }
        if (ndiscarded) {
            fs_rid_vector **rows = *result;
            int count = fs_rid_vector_length(rows[0]);
            int shifts = 0;
            for (int i=0; i < count; i++) {
                for (int s=0;s<slots;s++)
                    rows[s]->data[i-shifts] = rows[s]->data[i];
                if (!fs_bit_array_get(rows_discarded, i))
                    shifts++;
            }
            for (int s=0;s<slots;s++) {
                rows[s]->length -= ndiscarded;
            }
        }
        if (rows_discarded)
            fs_bit_array_destroy(rows_discarded);
    }

    return ret - ndiscarded;
}

/*
* A wrapper to go via the ACL graph control 
*/
int fs_bind_cache_wrapper(fs_query_state *qs, fs_query *q, int all,
                int flags, fs_rid_vector *rids[4],
                fs_rid_vector ***result, int offset, int limit) {
    int ret = fs_bind_cache_wrapper_intl_acl(qs, q, all, flags, rids, result, offset, limit);
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
    if (verbosity > 0) {
        printf("# @resolver@ cache_stats hits %u l1 %u l2 %u fails %u (%.4f perc. success)\n",
            qs->cache_hits,qs->cache_success_l1,qs->cache_success_l2,qs->cache_fail,
            ((((double)qs->cache_success_l1)+((double)qs->cache_success_l2))/((double)qs->cache_hits))*100.0);
        printf("# @resolver@ cache_stats items pre cached %u calls %u elapse %.3f\n",
        qs->pre_cache_total,qs->resolve_all_calls,qs->resolve_all_elapse);
        printf("# @resolver@ cache_stats resolve single calls %u elapse %.3f\n",
        qs->cache_fail,qs->resolve_unique_elapse);
    }

    g_static_mutex_unlock(&qs->cache_mutex);
    
    return 0;
}

int fs_query_bind_cache_count_slots(fs_query_state *qs) {
    unsigned int xc=0;
    unsigned int count_bind=0;
    if (qs->bind_cache) {
        for (xc=0; xc < CACHE_SIZE; xc++) {
            if (qs->bind_cache[xc].filled != 0)
                count_bind += 1;
        }
    }
    return count_bind;
}

int fs_query_bind_cache_size(void) {
    return CACHE_SIZE;
}
/* vi:set expandtab sts=4 sw=4: */
