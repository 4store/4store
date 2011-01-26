#ifndef QUERY_H
#define QUERY_H

#include <raptor2/raptor.h>
#include <rasqal.h>

#include "query-datatypes.h"
#include "query-data.h"
#include "query-cache.h"
#include "../common/4store.h"

fs_query_state *fs_query_init(fsp_link *link, rasqal_world *rasworld, raptor_world *rapworld);
int fs_query_fini(fs_query_state *qs);

/* Execute a SPARQL query, see results.h for how to read results from the fs_query */
fs_query *fs_query_execute(fs_query_state *qs, fsp_link *link, raptor_uri *bu,
                           const char *query, unsigned int flags, int opt_level, int soft_limit);

/* internal function used to process WHERE clauses */
int fs_query_process_pattern(fs_query *q, rasqal_graph_pattern *pattern, raptor_sequence *vars);

void fs_query_free(fs_query *q);
double fs_query_start_time(fs_query *q);
int fs_query_flags(fs_query *q);
int fs_query_errors(fs_query *q);
int fs_bind_slot(fs_query *q, int block, fs_binding *b, 
        rasqal_literal *l, fs_rid_vector *v, int *bind, rasqal_variable **var,
        int lit_allowed);
int fs_query_have_laqrs(void);

void fs_check_cons_slot(fs_query *q, raptor_sequence *vars, rasqal_literal *l);

#endif
