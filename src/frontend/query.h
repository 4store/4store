#ifndef QUERY_H
#define QUERY_H

#include <raptor.h>

#include "query-datatypes.h"
#include "query-data.h"
#include "query-cache.h"
#include "common/4store.h"

fs_query_state *fs_query_init(fsp_link *link);
int fs_query_fini(fs_query_state *qs);

fs_query *fs_query_execute(fs_query_state *qs, fsp_link *link, raptor_uri *bu,
                           const char *query, unsigned int flags, int opt_level, int soft_limit);
void fs_query_free(fs_query *q);
double fs_query_start_time(fs_query *q);
int fs_query_flags(fs_query *q);
int fs_query_errors(fs_query *q);
int fs_bind_slot(fs_query *q, int block, fs_binding *b, 
        rasqal_literal *l, fs_rid_vector *v, int *bind, char **vname,
        int lit_allowed);
int fq_query_have_laqrs(void);

#endif
