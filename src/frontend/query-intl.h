#ifndef QUERY_INTL_H
#define QUERY_INTL_H

#include "results.h"
#include "query-cache.h"
#include "../common/4store.h"

#include <raptor.h>
#include <rasqal.h>
#include <glib.h>

struct _fs_query_state {
    fsp_link *link;
    fs_bind_cache *bind_cache;
    GHashTable *freq_s, *freq_o;

    /* mutex protecting the bind_cache */
    GStaticMutex cache_mutex;

    /* features supported by the backend */
    int freq_available;

    /* raptor + rasqal state */
    rasqal_world *rasqal_world;
    raptor_world *raptor_world;

    int verbosity;
    /* the following cache stats are filled only if verbosity > 0 */
    unsigned int cache_hits; /* total queries to the cache */
    unsigned int cache_success_l1; /* number of l1 success hits */
    unsigned int cache_success_l2;  /* number of l2 success hits */
    unsigned int cache_fail;  /* number of cache hits with no data on l1 or l2 */
    unsigned int pre_cache_total; /* number of items pre cached for the query */
    unsigned int resolve_all_calls; /* total num of resolve_all calls */
    double resolve_all_elapse;  /* total sum of elapsed time on resolve_all calls */
    double resolve_unique_elapse; /* total sum of elapsed time on resolve(single rid) calls */
};

struct _fs_query {
    fs_query_state *qs;
    fsp_link *link;
    fs_binding *bt;			/* main binding table, used in FILTER handling */
    fs_binding *bb[FS_MAX_BLOCKS];	/* per block binding table */
    int segments;
    int num_vars;			/* number of projected variables */
    int num_vars_total;			/* number of total variables */
    int expressions;			/* number of projected expressions */
    int construct;
    int describe;
    int ask;
    int length;
    int order;				/* true if there are ORDER BYs */
    int limit;				/* a user specified limit, or -1 */
    int soft_limit;			/* a limit chosen by the system
					   to prevent complexity explostion */
    int offset;
    int opt_level;			/* optimisation level in [0,3] */
    int boolean;			/* true if the query succeeded */
    int block;
    int unions;
    int row; 				/* current row in results */
    int lastrow;			/* last row that was resolved */
    int rows_output;			/* number of rows returned */
    int errors;				/* number of parse/execution errors */
    fs_row *resrow;
    fs_p_vector blocks[FS_MAX_BLOCKS];
    fs_join_type join_type[FS_MAX_BLOCKS];
    int parent_block[FS_MAX_BLOCKS];
    int union_group[FS_MAX_BLOCKS];
    raptor_sequence *constraints[FS_MAX_BLOCKS];
    int flags;
    fs_rid_vector **pending;
    rasqal_query *rq;
    raptor_serializer *ser;
    raptor_uri *base;
    GSList *free_list;			/* list of pointers to be freed
					 * with g_free */
    GSList *warnings;
    int *ordering;
    double start_time;
    fs_rid_vector *default_graphs;
    int console;			/* true if the query is being used from a console app */
    int aggregate;			/* true if the query uses aggregates */
    long group_length;			/* number of rows in the current group */
    uint64_t *group_rows;		/* row numbers of the rows in the current group */
    unsigned char *apply_constraints; /* bit array initialized to 1s, 
                                        position x shifts to 0 if no apply cons */
    int group_by;
};

#endif
