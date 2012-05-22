#ifndef QUERY_CACHE_H
#define QUERY_CACHE_H

#include "query-datatypes.h"

typedef struct _fs_bind_cache fs_bind_cache;

int fs_bind_cache_wrapper(fs_query_state *qs, fs_query *q, int all,
    int flags, fs_rid_vector *rids[4], fs_rid_vector ***result,
    int offset, int limit);

int fs_query_cache_flush(fs_query_state *qs, int verbosity);
int fs_query_bind_cache_count_slots(fs_query_state *qs);
int fs_query_bind_cache_size(void);
#endif
