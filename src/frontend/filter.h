#ifndef FILTER_H
#define FILTER_H

#include "query-datatypes.h"
#include "filter-datatypes.h"

/* utility functions to handle bit array */
unsigned char *fs_new_bit_array(long n);
void fs_bit_array_set(unsigned char *p,long i,int value);
short fs_bit_array_get(unsigned char *p,long i);
void fs_bit_array_destroy(unsigned char *p);

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
fs_value fn_bnode(fs_query *q, fs_value a);
fs_value fn_lang(fs_query *q, fs_value a);
fs_value fn_datatype(fs_query *q, fs_value a);

/* SPARQL 1.1 functions */

fs_value fn_substring(fs_query *q, fs_value str, fs_value start, fs_value length);
fs_value fn_ucase(fs_query *q, fs_value v);
fs_value fn_lcase(fs_query *q, fs_value v);
fs_value fn_encode_for_uri(fs_query *q, fs_value v);
fs_value fn_year(fs_query *q, fs_value v);
fs_value fn_month(fs_query *q, fs_value v);
fs_value fn_day(fs_query *q, fs_value v);
fs_value fn_hours(fs_query *q, fs_value v);
fs_value fn_minutes(fs_query *q, fs_value v);
fs_value fn_seconds(fs_query *q, fs_value v);
fs_value fn_timezone(fs_query *q, fs_value v);
fs_value fn_strstarts(fs_query *q, fs_value arg1, fs_value arg2);
fs_value fn_strends(fs_query *q, fs_value arg1, fs_value arg2);
fs_value fn_contains(fs_query *q, fs_value arg1, fs_value arg2);

/* casts and similar */
fs_value fn_cast(fs_query *q, fs_value v, fs_value d);
fs_value fn_cast_intl(fs_query *q, fs_value v, fs_rid dt);
fs_value fn_ebv(fs_value a);

#endif
