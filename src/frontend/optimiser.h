#ifndef OPTIMISER_H
#define OPTIMISER_H

#include "query-datatypes.h"
#include "query-intl.h"

/* returns the number of values plus 1 for the expression, or INT_MAX if it's
 * unbound */
int fs_opt_num_vals(fs_binding *b, rasqal_literal *l);

/* returns true if the expression can be hashed */
int fs_opt_is_const(fs_binding *b, rasqal_literal *l);

/* sort a vector of triples into a good order to bind them, based on some
 * heuristics */
int fs_optimise_triple_pattern(fs_query_state *qs, fs_query *q, int block, rasqal_triple *patt[], int length, int start);

/* return an estimated number of results from a bind */
int fs_bind_freq(fs_query_state *qs, fs_query *q, int block, rasqal_triple *t);

/* dump the contents of the quad frequency cache to stdout */
void fs_optimiser_freq_print(fs_query_state *qs);

/* vi:set expandtab sts=4 sw=4: */

#endif
