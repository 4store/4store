#ifndef QUERY_INTL_H
#define QUERY_INTL_H

#include "results.h"
#include "query-cache.h"
#include "common/4store.h"

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
#ifdef HAVE_RASQAL_WORLD

    /* rasqal state */
    rasqal_world* rasqal_world;
#endif /* HAVE_RASQAL_WORLD */
};

struct _fs_query {
    fs_query_state *qs;
    fsp_link *link;
    fs_binding *bb[FS_MAX_BLOCKS];	/* per block binding table */
    int segments;
    int num_vars;			/* number of projected variables */
    int expressions;			/* number of projected expressions */
    int construct;
    int describe;
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
};

#endif
