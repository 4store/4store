#ifndef FILTER_H
#define FILTER_H

#include "query-datatypes.h"
#include "filter-datatypes.h"

/* unary operators */
fs_value fn_plus(fs_query *q, fs_value a);
fs_value fn_minus(fs_query *q, fs_value a);

/* binary generic operators */
fs_value fn_equal(fs_query *q, fs_value a, fs_value b);
fs_value fn_not_equal(fs_query *q, fs_value a, fs_value b);
fs_value fn_less_than(fs_query *q, fs_value a, fs_value b);
fs_value fn_greater_than(fs_query *q, fs_value a, fs_value b);
fs_value fn_less_than_equal(fs_query *q, fs_value a, fs_value b);
fs_value fn_greater_than_equal(fs_query *q, fs_value a, fs_value b);

/* binary maths operators */
fs_value fn_numeric_add(fs_query *q, fs_value a, fs_value b);
fs_value fn_numeric_subtract(fs_query *q, fs_value a, fs_value b);
fs_value fn_numeric_multiply(fs_query *q, fs_value a, fs_value b);
fs_value fn_numeric_divide(fs_query *q, fs_value a, fs_value b);

fs_value fn_numeric_equal(fs_query *q, fs_value a, fs_value b);
fs_value fn_numeric_less_than(fs_query *q, fs_value a, fs_value b);
fs_value fn_numeric_greater_than(fs_query *q, fs_value a, fs_value b);

fs_value fn_datetime_equal(fs_query *q, fs_value a, fs_value b);
fs_value fn_datetime_less_than(fs_query *q, fs_value a, fs_value b);
fs_value fn_datetime_greater_than(fs_query *q, fs_value a, fs_value b);
fs_value fn_rdfterm_equal(fs_query *q, fs_value a, fs_value b);

fs_value fn_compare(fs_query *q, fs_value a, fs_value b);
fs_value fn_matches(fs_query *q, fs_value str, fs_value pat, fs_value flags);
fs_value fn_lang_matches(fs_query *q, fs_value a, fs_value l);

fs_value fn_logical_and(fs_query *q, fs_value a, fs_value b);
fs_value fn_logical_or(fs_query *q, fs_value a, fs_value b);
fs_value fn_not(fs_query *q, fs_value a);

fs_value fn_bound(fs_query *q, fs_value a);
fs_value fn_is_iri(fs_query *q, fs_value a);
fs_value fn_is_blank(fs_query *q, fs_value a);
fs_value fn_is_literal(fs_query *q, fs_value a);
fs_value fn_str(fs_query *q, fs_value a);
fs_value fn_uri(fs_query *q, fs_value a);
fs_value fn_lang(fs_query *q, fs_value a);
fs_value fn_datatype(fs_query *q, fs_value a);

/* --end-process-- */

/* casts */
fs_value fn_cast(fs_query *q, fs_value v, fs_value d);
fs_value fn_cast_intl(fs_query *q, fs_value v, fs_rid dt);
fs_value fn_ebv(fs_value a);

#endif
