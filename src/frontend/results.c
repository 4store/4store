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
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "4store-config.h"
#include "results.h"
#include "order.h"
#include "query-datatypes.h"
#include "query.h"
#include "query-intl.h"
#include "filter.h"
#include "debug.h"
#include "../common/hash.h"
#include "../common/error.h"
#include "../common/rdf-constants.h"

#define CACHE_SIZE 65536
#define CACHE_MASK (CACHE_SIZE-1)

#define RESOURCE_LOOKUP_BUFFER 1800

/* glib 2.x headers must match the architecture we're building. If the size of a pointer
 * is smaller in the provided glibconfig.h than in our target architecture, the resulting typedef
 * should be invalid, preventing the user from building a 4store that won't work properly at runtime
 * we can't use the pre-processor to detect this problem because it doesn't grok sizeof() */
typedef char wrong_glib_headers[1 + GLIB_SIZEOF_VOID_P - sizeof(void *)];

/* used for sorting */
static const char *NULL_PROXY = " ";
static const char *BNODE_PROXY = "*";

static GStaticMutex cache_mutex = G_STATIC_MUTEX_INIT;

static fs_resource res_l2_cache[CACHE_SIZE];

static GHashTable *res_l1_cache = NULL;

/* must hold mutex to call this function */
static void setup_l1_cache()
{
    res_l1_cache = g_hash_table_new_full(fs_rid_hash, fs_rid_equal, NULL, NULL);
}

static int resolve(fs_query *q, fs_rid rid, fs_resource *res)
{
    res->rid = FS_RID_NULL;
    res->attr = FS_RID_NULL;
    res->lex = NULL;

    if (rid == FS_RID_NULL) {
	res->rid = rid;
        res->attr = FS_RID_NULL;
	res->lex = "NULL";

        return 0;
    }
    if (FS_IS_BNODE(rid)) {
	res->rid = rid;
	res->attr = FS_RID_NULL;
	res->lex = g_strdup_printf("_:b%llx", FS_BNODE_NUM(rid));
        fs_query_add_freeable(q, res->lex);

	return 0;
    }

    q->qs->cache_hits++;
    g_static_mutex_lock(&cache_mutex);
    if (res_l2_cache[rid & CACHE_MASK].rid == rid) {
        q->qs->cache_success_l2++;
	memcpy(res, &res_l2_cache[rid & CACHE_MASK], sizeof(fs_resource));
        g_static_mutex_unlock(&cache_mutex);

	return 0;
    }

    if (!res_l1_cache) {
        setup_l1_cache();
    }
    gpointer hit;
    if ((hit = g_hash_table_lookup(res_l1_cache, &rid))) {
        q->qs->cache_success_l1++;
	memcpy(res, hit, sizeof(fs_resource));
        g_static_mutex_unlock(&cache_mutex);

        return 0;
    }
    g_static_mutex_unlock(&cache_mutex);

GTimer *tmr=NULL;
    if (q->qs->verbosity) {
        tmr = g_timer_new();
        q->qs->cache_fail++;
    }

    fs_rid_vector *r = fs_rid_vector_new(1);
    r->data[0] = rid;
#ifdef DEBUG_FILTER
printf("resolving %016llx\n", rid);
#endif
    fsp_resolve(q->link, FS_RID_SEGMENT(rid, q->segments), r, res);
    fs_rid *trid = malloc(sizeof(fs_rid));
    fs_resource *tres = malloc(sizeof(fs_resource));
    *trid = rid;
    tres->rid = res->rid;
    tres->attr = res->attr;
    tres->lex = res->lex;
    g_static_mutex_lock(&cache_mutex);
    g_hash_table_insert(res_l1_cache, trid, tres);
    g_static_mutex_unlock(&cache_mutex);
    fs_rid_vector_free(r);

    if (q->qs->verbosity) {
        q->qs->resolve_unique_elapse += g_timer_elapsed(tmr,NULL);
        g_timer_destroy(tmr);
    }

    return 0;
}

static fs_value literal_to_value(fs_query *q, int row, int block, rasqal_literal *l)
{
    fs_value v;
    fs_rid attr = fs_c.empty;

    switch (l->type) {
	case RASQAL_LITERAL_BLANK:
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "unhandled bNode in FILTER expression");

	case RASQAL_LITERAL_URI:
	    return fs_value_uri((char *)raptor_uri_as_string(l->value.uri));

        case RASQAL_LITERAL_XSD_STRING:
        case RASQAL_LITERAL_UDT:
	case RASQAL_LITERAL_STRING:
            if (l->language) {
                attr = fs_hash_literal(l->language, 0);
            } else if (l->datatype) {
                attr = fs_hash_uri((char *)raptor_uri_as_string(l->datatype));
            }
	    v = fs_value_plain((char *)l->string);
            v.attr = attr;
            v = fn_cast_intl(q, v, attr);
            return v;

	case RASQAL_LITERAL_BOOLEAN:
	    return fs_value_boolean(l->value.integer);

	case RASQAL_LITERAL_INTEGER:
	case RASQAL_LITERAL_INTEGER_SUBTYPE:
	    return fs_value_integer(l->value.integer);

	case RASQAL_LITERAL_DOUBLE:
	    return fs_value_double(l->value.floating);

	case RASQAL_LITERAL_FLOAT:
	    return fs_value_float(l->value.floating);

	case RASQAL_LITERAL_DECIMAL:
	    return fs_value_decimal_from_string((char *)l->string);

	case RASQAL_LITERAL_DATETIME:
	    return fs_value_datetime_from_string((char *)l->string);

	case RASQAL_LITERAL_PATTERN:
	    return fs_value_plain((char *)l->string);

	case RASQAL_LITERAL_QNAME:
	    return fs_value_error(FS_ERROR_INVALID_TYPE,
		    "unhandled qname in FILTER expression");

	case RASQAL_LITERAL_VARIABLE:
	    {
#ifdef DEBUG_FILTER
		char *name = (char *)l->value.variable->name;
                printf("getting value of ?%s, row %d from B%d\n", name, row, block);
#endif
		fs_binding *b = fs_binding_get(q->bt, l->value.variable);
                /* TODO this code needs to be tested when the parser handles
                 * { FILTER() } correctly, but not block can be -1 if we dont
                 * care about scope */
#if 0
                if (!b->bound_in_block[block]) {
#ifdef DEBUG_FILTER
                    printf("?%s not bound in B%d, appears in B%d\n", name, block, b->appears);
#endif
                    q->warnings = g_slist_prepend(q->warnings,
                        "variable used in expression block where "
                        "it is not bound");

		    return fs_value_rid(FS_RID_NULL);
                }
#endif
		if (!b || row >= b->vals->length) {
		    return fs_value_rid(FS_RID_NULL);
		}
		fs_resource r;
                resolve(q, b->vals->data[row], &r);
		fs_value v = fs_value_resource(q, &r);

		return v;
	    }

	case RASQAL_LITERAL_UNKNOWN:
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad literal FILTER expression");

    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "unhandled literal");
}

fs_value fs_expression_eval(fs_query *q, int row, int block, rasqal_expression *e)
{
    if (!e) {
	return fs_value_rid(FS_RID_NULL);
    }
    if (block < 0) {
        fs_error(LOG_ERR, "block was less than zero, changing to 0");
        block = 0;
    }
    
    switch (e->op) {
	case RASQAL_EXPR_AND:
	    return fn_logical_and(q, fs_expression_eval(q, row, block, e->arg1),
			             fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_OR:
	    return fn_logical_or(q, fs_expression_eval(q, row, block, e->arg1),
			            fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_EQ:
	case RASQAL_EXPR_STR_EQ:
	    return fn_equal(q, fs_expression_eval(q, row, block, e->arg1),
			       fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_NEQ:
	case RASQAL_EXPR_STR_NEQ:
	    return fn_not_equal(q, fs_expression_eval(q, row, block, e->arg1),
			           fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_LT:
	    return fn_less_than(q, fs_expression_eval(q, row, block, e->arg1),
			           fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_GT:
	    return fn_greater_than(q, fs_expression_eval(q, row, block, e->arg1),
			              fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_LE:
	    return fn_less_than_equal(q, fs_expression_eval(q, row, block, e->arg1),
			                 fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_GE:
	    return fn_greater_than_equal(q, 
		    fs_expression_eval(q, row, block, e->arg1),
		    fs_expression_eval(q, row, block, e->arg2));
	
	case RASQAL_EXPR_UMINUS:
	    return fn_minus(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_PLUS:
	    return fn_numeric_add(q, fs_expression_eval(q, row, block, e->arg1),
                                     fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_MINUS:
	    return fn_numeric_subtract(q, fs_expression_eval(q, row, block, e->arg1),
                                          fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_STAR:
	    return fn_numeric_multiply(q, fs_expression_eval(q, row, block, e->arg1),
				          fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_SLASH:
	    return fn_numeric_divide(q, fs_expression_eval(q, row, block, e->arg1),
				        fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_REM:
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "unhandled REM operator");

	case RASQAL_EXPR_REGEX:
	    return fn_matches(q, fs_expression_eval(q, row, block, e->arg1),
		              fs_expression_eval(q, row, block, e->arg2),
		              fs_expression_eval(q, row, block, e->arg3));

	case RASQAL_EXPR_STR_MATCH:
	    return fn_matches(q, fs_expression_eval(q, row, block, e->arg1),
		              literal_to_value(q, row, block, e->literal),
			      fs_value_plain((char *)e->literal->flags));

	case RASQAL_EXPR_STR_NMATCH:
	    return fn_not(q, fn_matches(q, fs_expression_eval(q, row, block, e->arg1),
		              literal_to_value(q, row, block, e->literal),
			      fs_value_plain((char *)e->literal->flags)));

	case RASQAL_EXPR_TILDE:
	case RASQAL_EXPR_BANG:
	    return fn_not(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_FUNCTION:
            if (raptor_sequence_size(e->args) == 1 &&
                !strncmp((char *)raptor_uri_as_string(e->name), XSD_NAMESPACE, strlen(XSD_NAMESPACE))) {
                return fn_cast(q, fs_expression_eval(q, row,
                                    block, (rasqal_expression *)raptor_sequence_get_at(e->args, 0)),
		           fs_value_uri((char *)raptor_uri_as_string(e->name)));
            }
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "unhandled extension function");

	case RASQAL_EXPR_BOUND:
	    return fn_bound(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_STR:
	    return fn_str(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_LANG:
	    return fn_lang(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_LANGMATCHES:
	    return fn_lang_matches(q, fs_expression_eval(q, row, block, e->arg1),
		                   fs_expression_eval(q, row, block, e->arg2));

	case RASQAL_EXPR_DATATYPE:
	    return fn_datatype(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_ISURI:
	    return fn_is_iri(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_ISBLANK:
	    return fn_is_blank(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_ISLITERAL:
	    return fn_is_literal(q, fs_expression_eval(q, row, block, e->arg1));

	case RASQAL_EXPR_CAST:
	    return fn_cast(q, fs_expression_eval(q, row, block, e->arg1),
		           fs_value_uri((char *)raptor_uri_as_string(e->name)));

	case RASQAL_EXPR_ORDER_COND_DESC: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
            v.valid |= fs_valid_bit(FS_V_DESC);
	    return v;
        }

	case RASQAL_EXPR_ORDER_COND_ASC: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
            v.valid &= ~fs_valid_bit(FS_V_DESC);
	    return v;
        }

        case RASQAL_EXPR_GROUP_COND_DESC: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
	    return v;
        }

        case RASQAL_EXPR_GROUP_COND_ASC: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
	    return v;
        }

        case RASQAL_EXPR_COUNT: {
            if (e->arg1->op == RASQAL_EXPR_VARSTAR && !q->apply_constraints) {
                return fs_value_integer(q->group_length);
            }
            int count = 0;
            for (int r=0; r<q->group_length; r++) {
                if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[r])) continue;
                    
                fs_value v = fs_expression_eval(q, q->group_rows[r], block, e->arg1);
                if (v.valid & fs_valid_bit(FS_V_TYPE_ERROR) ||
                    ((v.attr == fs_c.empty || v.attr == FS_RID_NULL) &&
                      v.rid == FS_RID_NULL)) {
                   /* do nothing */
                } else {
                    count++;
                }
            }

            return fs_value_integer(count);
        }

	case RASQAL_EXPR_IRI:
	case RASQAL_EXPR_URI:
            return fn_uri(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_BNODE:
            return fn_bnode(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_STRLANG: {
            fs_value str = fs_expression_eval(q, row, block, e->arg1);
            if (fs_is_error(str)) {
                return str;
            }
            if (str.valid & fs_valid_bit(FS_V_RID) &&
                (FS_IS_BNODE(str.rid) || FS_IS_URI(str.rid))) {
                return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            }
            str = fs_value_fill_lexical(q, str);

            fs_value lang = fs_expression_eval(q, row, block, e->arg2);
            if (fs_is_error(lang)) {
                return lang;
            }
            if (lang.valid & fs_valid_bit(FS_V_RID) &&
                (FS_IS_BNODE(lang.rid) || FS_IS_URI(lang.rid))) {
                return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            }
            lang = fs_value_fill_lexical(q, lang);

            return fs_value_plain_with_lang(str.lex, lang.lex);
        }

        case RASQAL_EXPR_STRDT: {
            fs_value str = fs_expression_eval(q, row, block, e->arg1);
            if (fs_is_error(str)) {
                return str;
            }
            if (str.valid & fs_valid_bit(FS_V_RID) &&
                (FS_IS_BNODE(str.rid) || FS_IS_URI(str.rid))) {
                return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            }
            str = fs_value_fill_lexical(q, str);

            fs_value dt = fs_expression_eval(q, row, block, e->arg2);
            if (fs_is_error(dt)) {
                return dt;
            }
            if (dt.valid & fs_valid_bit(FS_V_RID)) {
                if (!(FS_IS_BNODE(dt.rid) || FS_IS_URI(dt.rid))) {
                    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
                }
            } else {
                return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            }
            dt = fs_value_fill_lexical(q, dt);

            return fs_value_plain_with_dt(str.lex, dt.lex);
        }

	case RASQAL_EXPR_NOT_IN:
	case RASQAL_EXPR_IN: {
            fs_value comp = fs_expression_eval(q, row, block, e->arg1);
            for (int i=0; i < raptor_sequence_size(e->args); i++) {
                fs_value v = fs_expression_eval(q, q->group_rows[row], block, raptor_sequence_get_at(e->args, i));
                if (fs_value_equal(comp, v)) {
                    return fs_value_boolean(e->op == RASQAL_EXPR_IN ? 1 : 0);
                }
            }

            return fs_value_boolean(e->op == RASQAL_EXPR_IN ? 0 : 1);
        }

	case RASQAL_EXPR_NOW:
	case RASQAL_EXPR_CURRENT_DATETIME:
            return fs_value_datetime(q->start_time);

        case RASQAL_EXPR_FROM_UNIXTIME: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
            if (fs_is_numeric(&v)) {
                v = fn_cast_intl(q, v, fs_c.xsd_integer);

                return fs_value_datetime(v.in);
            }

            return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
        }

        case RASQAL_EXPR_TO_UNIXTIME: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
            if (v.attr == fs_c.xsd_datetime) {
                return fs_value_integer(v.in);
            }

            return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
        }

        case RASQAL_EXPR_STRLEN: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);
            if (!fs_is_plain_or_string(v)) {
                return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            }
            v = fs_value_fill_lexical(q, v);

            return fs_value_integer(strlen(v.lex));
        }

        case RASQAL_EXPR_ISNUMERIC: {
            fs_value v = fs_expression_eval(q, row, block, e->arg1);

            return fs_value_boolean(fs_is_numeric(&v));
        }

	case RASQAL_EXPR_IF: {
            fs_value cond = fn_ebv(fs_expression_eval(q, row, block, e->arg1));
            /* if arg1 is false or error */
            if (fs_is_error(cond)) {
                return cond;
            } else if (cond.in) {
                return fs_expression_eval(q, row, block, e->arg2);
            }

            return fs_expression_eval(q, row, block, e->arg3);
        }

        case RASQAL_EXPR_SUBSTR: {
            return fn_substring(q, fs_expression_eval(q, row, block, e->arg1),
                                fs_expression_eval(q, row, block, e->arg2),
                                fs_expression_eval(q, row, block, e->arg3));
        }

        case RASQAL_EXPR_UCASE:
            return fn_ucase(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_LCASE:
            return fn_lcase(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_ENCODE_FOR_URI:
            return fn_encode_for_uri(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_CONCAT: {
            GString *concat = g_string_new("");
            for (int i=0; i < raptor_sequence_size(e->args); i++) {
                fs_value v = fs_expression_eval(q, row, block, raptor_sequence_get_at(e->args, i));
                if (fs_is_error(v)) {
                    g_string_free(concat, TRUE);

                    return v;
                }
                v = fs_value_fill_lexical(q, v);
                g_string_append(concat, v.lex);
            }

            char *str = g_string_free(concat, FALSE);
            fs_query_add_freeable(q, str);

            return fs_value_plain(str);
        }

        case RASQAL_EXPR_YEAR:
            return fn_year(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_MONTH:
            return fn_month(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_DAY:
            return fn_day(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_HOURS:
            return fn_hours(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_MINUTES:
            return fn_minutes(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_SECONDS:
            return fn_seconds(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_TIMEZONE:
            return fn_timezone(q, fs_expression_eval(q, row, block, e->arg1));

        case RASQAL_EXPR_STRSTARTS:
            return fn_strstarts(q, fs_expression_eval(q, row, block, e->arg1),
                                   fs_expression_eval(q, row, block, e->arg2));

        case RASQAL_EXPR_STRENDS:
            return fn_strends(q, fs_expression_eval(q, row, block, e->arg1),
                                 fs_expression_eval(q, row, block, e->arg2));

        case RASQAL_EXPR_CONTAINS:
            return fn_contains(q, fs_expression_eval(q, row, block, e->arg1),
                                  fs_expression_eval(q, row, block, e->arg2));

        /* aggregates */

        case RASQAL_EXPR_SUM: {
            fs_value v = fs_value_integer(0);
            for (int r=0; r<q->group_length; r++) {
                if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[r])) continue;
                v = fn_numeric_add(q, v, fs_expression_eval(q, q->group_rows[r], block, e->arg1));
            }

            return v;
        }

        case RASQAL_EXPR_AVG: {
            fs_value sum = fs_value_integer(0);
            int count = 0;
            for (int r=0; r<q->group_length; r++) {
                if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[r])) continue;
                fs_value expr = fs_expression_eval(q, q->group_rows[r], block, e->arg1);
                sum = fn_numeric_add(q, sum, expr);
                if (sum.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
                    return sum;
                }
                if (expr.valid & fs_valid_bit(FS_V_TYPE_ERROR) ||
                    ((expr.attr == fs_c.empty || expr.attr == FS_RID_NULL) &&
                      expr.rid == FS_RID_NULL)) {
                   /* do nothing */
                } else {
                    count++;
                }
            }

            return fn_numeric_divide(q, sum, fs_value_integer(count));
        }

        case RASQAL_EXPR_SAMPLE: {
            if (q->group_length > 0) {
                return fs_expression_eval(q, q->group_rows[0], block, e->arg1);
            }

            return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
        }

        case RASQAL_EXPR_MIN: {
            fs_value m;
            int set = 0;
            int pre=0;
            if (q->group_length > 0) {
                for (int pre=0; !set && pre<q->group_length; pre++) {
                    if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[pre])) continue;
                    m = fs_expression_eval(q, q->group_rows[pre], block, e->arg1);
                    set = 1;
                }
            }
            if (!set)
                m = fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            else {
                for (int r=pre; r<q->group_length; r++) {
                    if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[r])) continue;
                    fs_value expr = fs_expression_eval(q, q->group_rows[r], block, e->arg1);
                    int order = fs_order_by_cmp(m, expr);
                    if (order > 0) {
                        m = expr;
                    }
                }
            }
            return m;
        }

        case RASQAL_EXPR_MAX: {
            fs_value m;
            int set = 0;
            int pre=0;
            if (q->group_length > 0) {
                for (int pre=0; !set && pre<q->group_length; pre++) {
                    if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[pre])) continue;
                    m = fs_expression_eval(q, q->group_rows[pre], block, e->arg1);
                    set = 1;
                }
            }
            if (!set)
                m = fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
            else {
                for (int r=pre; r<q->group_length; r++) {
                    if (q->apply_constraints && !fs_bit_array_get(q->apply_constraints,q->group_rows[r])) continue;
                    fs_value expr = fs_expression_eval(q, q->group_rows[r], block, e->arg1);
                    int order = fs_order_by_cmp(m, expr);
                    if (order < 0) {
                        m = expr;
                    }
                }
            }
            return m;

            return m;
        }

        case RASQAL_EXPR_GROUP_CONCAT: {
            char *sep = " ";
            GString *concat = g_string_new("");
            if (e->literal) {
                sep = (char *)rasqal_literal_as_string(e->literal);
            }
            int count = 0;
            for (int r=0; r<q->group_length; r++) {
                for (int i=0; i < raptor_sequence_size(e->args); i++) {
                    fs_value v = fs_expression_eval(q, q->group_rows[r], block, raptor_sequence_get_at(e->args, i));
                    if (fs_is_error(v)) {
                        g_string_free(concat, TRUE);

                        return v;
                    }
                    v = fs_value_fill_lexical(q, v);
                    if (count++ > 0) {
                        g_string_append(concat, sep);
                    }
                    g_string_append(concat, v.lex);
                }
            }

            char *str = g_string_free(concat, FALSE);
            fs_query_add_freeable(q, str);

            return fs_value_plain(str);
        }

        case RASQAL_EXPR_SAMETERM:
            return fn_rdfterm_equal(q, fs_expression_eval(q, row, block, e->arg1),
                                       fs_expression_eval(q, row, block, e->arg2));

        case RASQAL_EXPR_COALESCE: {
            fs_value v;
            for (int i=0; raptor_sequence_get_at(e->args, i); i++) {
                rasqal_expression *ea = (rasqal_expression *)raptor_sequence_get_at(e->args, i);
                v = fs_expression_eval(q, row, block, ea);
                if (!fs_is_error(v) &&
                    !(v.valid & fs_valid_bit(FS_V_RID) && v.rid == FS_RID_NULL)) {
                    return v;
                }
            }
            return fs_value_error(FS_ERROR_INVALID_TYPE, "no valid arguments to COALESCE");
        }

        case RASQAL_EXPR_VARSTAR: {
            fs_value v = fs_value_integer(1);
            return v;
        }

	case RASQAL_EXPR_UNKNOWN:
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad value in expression");
	case RASQAL_EXPR_LITERAL:
	    break;
    }

    if (e->literal) {
	return literal_to_value(q, row, block, e->literal);
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "unhandled operator");
}

static int resolve_precache_all(fsp_link *l, fs_rid_vector *rv[], int segments)
{
    g_static_mutex_lock(&cache_mutex);
    if (!res_l1_cache) {
        setup_l1_cache();
    }
    g_static_mutex_unlock(&cache_mutex);

    fs_resource *res[segments];
    for (int s=0; s<segments; s++) {
        fs_rid_vector_sort(rv[s]);
        fs_rid_vector_uniq(rv[s], 0);
        res[s] = malloc(rv[s]->length * sizeof(fs_resource));
    }
    int ret = fsp_resolve_all(l, rv, res);
    if (ret) {
        fs_error(LOG_CRIT, "resolve_all failed");

        return 1;
    }

    g_static_mutex_lock(&cache_mutex);
    for (int s=0; s<segments; s++) {
        for (int i=0; i<rv[s]->length; i++) {
            if (res[s][i].rid == FS_RID_NULL) break;
            fs_rid *trid = malloc(sizeof(fs_rid));
            fs_resource *tres = malloc(sizeof(fs_resource));
            *trid = res[s][i].rid;
            tres->rid = res[s][i].rid;
            tres->attr = res[s][i].attr;
            tres->lex = res[s][i].lex;
            g_hash_table_insert(res_l1_cache, trid, tres);
        }
    }
    g_static_mutex_unlock(&cache_mutex);

    for (int s=0; s<segments; s++) {
        free(res[s]);
    }

    return 0;
}

static int resolve_precache_all_with_stats(fs_query *q) {
    GTimer *tmr=NULL;
    if (q->qs->verbosity) tmr = g_timer_new();
    int res = resolve_precache_all(q->link, q->pending, q->segments);
    if (q->qs->verbosity) {
        q->qs->resolve_all_elapse += g_timer_elapsed(tmr,NULL);
        q->qs->resolve_all_calls++;
        g_timer_destroy(tmr);
    }
    return res;
}

static raptor_term *slot_fill_from_rid(fs_query *q, fs_rid rid)
{
    fs_resource r;
    resolve(q, rid, &r);

    if (FS_IS_BNODE(rid)) {
	return raptor_new_term_from_blank(q->qs->raptor_world, (unsigned char *)r.lex+2);
    } else if (FS_IS_URI(rid)) {
        return raptor_new_term_from_uri_string(q->qs->raptor_world, (unsigned char *)r.lex);
    } else if (FS_IS_LITERAL(rid)) {
        raptor_uri *dt = NULL;
        char *tag = NULL;
        if (r.attr && r.attr != FS_RID_NULL) {
            fs_resource ar;
            resolve(q, r.attr, &ar);
            if (FS_IS_URI(r.attr)) {
                dt = raptor_new_uri(q->qs->raptor_world, (unsigned char *)ar.lex);
            } else {
                tag = ar.lex;
            }
        }
	return raptor_new_term_from_literal(q->qs->raptor_world, (unsigned char *)r.lex, dt, (unsigned char *)tag);
    }

    return raptor_new_term_from_blank(q->qs->raptor_world, (unsigned char *)"unknown");
}

static raptor_term *slot_fill(fs_query *q, rasqal_literal *l, fs_row *row)
{
    switch (l->type) {
    case RASQAL_LITERAL_URI:
	return raptor_new_term_from_uri(q->qs->raptor_world, l->value.uri);

    case RASQAL_LITERAL_BLANK:
    {
        char *label = g_strdup_printf("%s_%d", l->string, q->row);
        raptor_term *term = raptor_new_term_from_blank(q->qs->raptor_world,
            (unsigned char *)label);
        g_free(label);

	return term;
    }

    case RASQAL_LITERAL_XSD_STRING:
    case RASQAL_LITERAL_UDT:
    case RASQAL_LITERAL_STRING:
    case RASQAL_LITERAL_BOOLEAN:
	return raptor_new_term_from_literal(q->qs->raptor_world,
            (unsigned char *)l->string, NULL, NULL);

    case RASQAL_LITERAL_INTEGER:
    case RASQAL_LITERAL_INTEGER_SUBTYPE:
    {
        raptor_uri *dt = raptor_new_uri(q->qs->raptor_world,
            (unsigned char *)XSD_INTEGER);
        raptor_term *term = raptor_new_term_from_literal(q->qs->raptor_world,
            l->string, dt, NULL);
        raptor_free_uri(dt);

	return term;
    }

    case RASQAL_LITERAL_DECIMAL:
    {
        raptor_uri *dt = raptor_new_uri(q->qs->raptor_world,
            (unsigned char *)XSD_DECIMAL);
        raptor_term *term = raptor_new_term_from_literal(q->qs->raptor_world,
            l->string, dt, NULL);
        raptor_free_uri(dt);

	return term;
    }

    case RASQAL_LITERAL_DOUBLE:
    case RASQAL_LITERAL_FLOAT:
    {
        raptor_uri *dt = raptor_new_uri(q->qs->raptor_world,
            (unsigned char *)XSD_DOUBLE);
        raptor_term *term = raptor_new_term_from_literal(q->qs->raptor_world,
            l->string, dt, NULL);
        raptor_free_uri(dt);

	return term;
    }

    case RASQAL_LITERAL_DATETIME:
    {
        raptor_uri *dt = raptor_new_uri(q->qs->raptor_world,
            (unsigned char *)XSD_DATETIME);
        raptor_term *term = raptor_new_term_from_literal(q->qs->raptor_world,
            l->string, dt, NULL);
        raptor_free_uri(dt);

	return term;
    }

    case RASQAL_LITERAL_VARIABLE:
    {
	fs_row *b = NULL;
	int col;
	for (col=0; row[col].name; col++) {
	    if (!strcmp((char *)l->value.variable->name, row[col].name)) {
		b = row+col;
                break;
	    }
	}
        if (FS_IS_BNODE(b->rid)) {
            return raptor_new_term_from_blank(q->qs->raptor_world, (unsigned char *)b->lex+2);
        } else if (FS_IS_URI(b->rid)) {
            return raptor_new_term_from_uri_string(q->qs->raptor_world, (unsigned char *)b->lex);
        } else if (FS_IS_LITERAL(b->rid)) {
            raptor_uri *dt = NULL;
            const unsigned char *tag = NULL;
            if (b->dt) {
                dt = raptor_new_uri(q->qs->raptor_world, (unsigned char *)b->dt);
            }
            if (b->lang) {
                tag = (unsigned char *)b->lang;
            }
            return raptor_new_term_from_literal(q->qs->raptor_world, (unsigned char *)b->lex, dt, tag);
        }
    }

    /* this should never happen */
    case RASQAL_LITERAL_PATTERN:
    case RASQAL_LITERAL_QNAME:
    case RASQAL_LITERAL_UNKNOWN:
        fs_error(LOG_CRIT, "fell through result handler");
	break;
    }

    return raptor_new_term_from_blank(q->qs->raptor_world, (unsigned char *)"unknown");
}

static void insert_slot_fill(fs_query *q, fs_rid *rid,
                             rasqal_literal *l, fs_row *row)
{
    fs_resource res;

    switch (l->type) {
    case RASQAL_LITERAL_URI:
	res.lex = (char *)raptor_uri_as_string(l->value.uri);
        res.rid = fs_hash_uri(res.lex);
        res.attr = FS_RID_NULL;

        break;

    case RASQAL_LITERAL_BLANK:
        res.lex = g_strdup_printf("%s_%d", l->string, q->row);
	/* TODO this should be a bNode, but it's tricky to summon a bNode RID
         * from here */
        res.rid = fs_hash_uri(res.lex);
        res.attr = FS_RID_NULL;
        fs_query_add_freeable(q, res.lex);

	break;

    case RASQAL_LITERAL_XSD_STRING:
    case RASQAL_LITERAL_UDT:
    case RASQAL_LITERAL_STRING:
	res.lex = (char *)l->string;
        res.attr = 0;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_BOOLEAN:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_boolean;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_INTEGER:
    case RASQAL_LITERAL_INTEGER_SUBTYPE:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_integer;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_DOUBLE:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_double;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_FLOAT:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_float;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_DECIMAL:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_decimal;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_DATETIME:
	res.lex = (char *)l->string;
        res.attr = fs_c.xsd_datetime;
        res.rid = fs_hash_literal(res.lex, res.attr);

	break;

    case RASQAL_LITERAL_VARIABLE: {
	fs_row *b = NULL;
	for (int col=0; row[col].name; col++) {
	    if (!strcmp((char *)l->value.variable->name, row[col].name)) {
		b = row+col;
                break;
	    }
	}
	if (b == NULL || b->rid == FS_RID_NULL) {
	    *rid = FS_RID_NULL;

	    return;
	}
        *rid = b->rid;

	return;
    }

    /* this should never happen */
    case RASQAL_LITERAL_PATTERN:
    case RASQAL_LITERAL_QNAME:
    case RASQAL_LITERAL_UNKNOWN:
        *rid = FS_RID_NULL;

	return;
    }

    fsp_res_import(q->link, FS_RID_SEGMENT(res.rid, q->segments), 1, &res);
    *rid = res.rid;
}

/* NB row in this case must be the row in the binding structure, not the
 * incremental row number */
static int apply_constraints(fs_query *q, int row)
{
    for (int block=q->block; block >= 0; block--) {
	if (!(q->constraints[block])) continue;
        /* expressions that have been optimised out will be replaces with NULL,
         * so we have to be careful here */
	for (int c=0; c<raptor_sequence_size(q->constraints[block]); c++) {
	    rasqal_expression *e =
		raptor_sequence_get_at(q->constraints[block], c);
	    if (!e) continue;

	    fs_value v = fs_expression_eval(q, row, block, e);
#ifdef DEBUG_FILTER
            printf("FILTERs for B%d\n", block);
	    rasqal_expression_print(e, stdout);
	    printf(" -> ");
	    fs_value_print(v);
	    printf("\n");
#endif
	    if (v.valid & fs_valid_bit(FS_V_TYPE_ERROR) && v.lex) {
                /* only add a warning if it's not already in there */
                if (!q->warnings || !g_slist_find(q->warnings, v.lex)) {
		    q->warnings = g_slist_prepend(q->warnings, v.lex);
                }
	    }
	    fs_value result = fn_ebv(v);
	    if (result.valid & fs_valid_bit(FS_V_TYPE_ERROR) || !result.in) {
                return 0;
	    }
	}
    }

    return 1;
}

static int uri_needs_escape(const char *str, int *escaped_length)
{
    int esc_len = 0;
    int escape = 0;

    if (!str) return 0;

    for (const char *c = str; *c; c++) {
        switch (*c) {
        case ' ':
        case '\t':
        case '\r':
        case '\n':
        case '<':
        case '>':
            esc_len += 3;
            escape = 1;
            break;
        default:
            esc_len += 1;
        }
    }
    *escaped_length = esc_len;

    return escape;
}

static char *uri_escape(const char *str, int length)
{
    char *to = malloc(length+1);
    char *outp = to;

    for (const char *inp = str; *inp; inp++) {
        switch (*inp) {
        case ' ':
            *outp++ = '%';
            *outp++ = '2';
            *outp++ = '0';
            break;
        case '\t':
            *outp++ = '%';
            *outp++ = '0';
            *outp++ = '9';
            break;
        case '\n':
            *outp++ = '%';
            *outp++ = '0';
            *outp++ = 'A';
            break;
        case '\r':
            *outp++ = '%';
            *outp++ = '0';
            *outp++ = 'D';
            break;
        case '<':
            *outp++ = '%';
            *outp++ = '3';
            *outp++ = 'C';
            break;
        case '>':
            *outp++ = '%';
            *outp++ = '3';
            *outp++ = 'E';
            break;
        default:
            *outp++ = *inp;
            break;
        }
    }
    *outp = '\0';

    return to;
}

char *fs_uri_escape(const char *str)
{
    int esclen;
    if (uri_needs_escape(str, &esclen)) {
        return uri_escape(str, esclen);
    }

    return g_strdup(str);
}

static int tsv_needs_escape(const char *str, int *escaped_length)
{
    int esc_len = 0;
    int escape = 0;

    if (!str) return 0;

    for (const char *c = str; *c; c++) {
        switch (*c) {
        case '\\':
        case '\t':
        case '\r':
        case '\n':
        case '"':
            esc_len += 2;
            escape = 1;
            break;
        default:
            esc_len += 1;
        }
    }
    *escaped_length = esc_len;

    return escape;
}

static char *tsv_escape(const char *str, int length)
{
    char *to = malloc(length+1);
    char *outp = to;

    for (const char *inp = str; *inp; inp++) {
        switch (*inp) {
        case '\t':
            *outp++ = '\\';
            *outp++ = 't';
            break;
        case '\r':
            *outp++ = '\\';
            *outp++ = 'r';
            break;
        case '\n':
            *outp++ = '\\';
            *outp++ = 'n';
            break;
        case '\\':
        case '"':
            *outp++ = '\\';
            *outp++ = *inp;
            break;
        default:
            *outp++ = *inp;
            break;
        }
    }
    *outp = '\0';

    return to;
}

static int json_needs_escape(const char *str, int *escaped_length)
{
    int esc_len = 0;
    int escape = 0;

    if (!str) return 0;

    for (const char *c = str; *c; c++) {
        switch (*c) {
        case '\\':
        case '\t':
        case '\r':
        case '\n':
        case '"':
            esc_len += 2;
            escape = 1;
            break;
        default:
            esc_len += 1;
        }
    }
    *escaped_length = esc_len;

    return escape;
}

static char *json_escape(const char *str, int length)
{
    char *to = malloc(length+1);
    char *outp = to;

    for (const char *inp = str; *inp; inp++) {
        switch (*inp) {
        case '\t':
            *outp++ = '\\';
            *outp++ = 't';
            break;
        case '\r':
            *outp++ = '\\';
            *outp++ = 'r';
            break;
        case '\n':
            *outp++ = '\\';
            *outp++ = 'n';
            break;
        case '\\':
        case '"':
            *outp++ = '\\';
            *outp++ = *inp;
            break;
        default:
            *outp++ = *inp;
            break;
        }
    }
    *outp = '\0';

    return to;
}

static int xml_needs_escape(const char *str, int *escaped_length)
{
    int esc_len = 0;
    int escape = 0;

    if (!str) return 0;

    for (const char *c = str; *c; c++) {
	switch (*c) {
	    case '<':
		esc_len += 4;
		escape = 1;
		break;
	    case '>':
		esc_len += 4;
		escape = 1;
		break;
	    case '&':
		esc_len += 5;
		escape = 1;
		break;
	    default:
		esc_len++;
		break;
	}
    }

    *escaped_length = esc_len;

    return escape;
}

static char *xml_escape(const char *from, int len)
{
    char *to = malloc(len+1);
    char *outp = to;

    for (const char *inp = from; *inp; inp++) {
	switch (*inp) {
	    case '<':
		strcpy(outp, "&lt;");
		outp += 4;
		break;
	    case '>':
		strcpy(outp, "&gt;");
		outp += 4;
		break;
	    case '&':
		strcpy(outp, "&amp;");
		outp += 5;
		break;
	    default:
		*outp++ = *inp;
		break;
	}
    }

    *outp++ = '\0';

    return to;
}

static void describe_uri(fs_query *q, fs_rid rid, raptor_uri *uri)
{
    if (rid == FS_RID_NULL) {
        if (uri) {
            rid = fs_hash_uri((char *)raptor_uri_as_string(uri));
        }
    }
    raptor_statement st;
    raptor_statement_init(&st, q->qs->raptor_world);
    st.graph = NULL;
    fs_rid_vector *es = fs_rid_vector_new(0);
    fs_rid_vector *ss = fs_rid_vector_new_from_args(1, rid);
    fs_rid_vector **result = NULL;
    fsp_bind_limit(q->link, FS_RID_SEGMENT(rid, q->segments),
        FS_BIND_BY_SUBJECT | FS_BIND_PREDICATE | FS_BIND_OBJECT, es, ss,
        es, es, &result, 0, q->soft_limit);
    fs_rid_vector_free(es);
    fs_rid_vector_free(ss);
    raptor_term *subject;
    if (FS_IS_BNODE(rid)) {
        if (uri) {
            subject = raptor_new_term_from_blank(q->qs->raptor_world,
                raptor_uri_as_string(uri));
        } else {
            char tmp[256];
            sprintf(tmp, "b%016llx", rid);
            subject = raptor_new_term_from_blank(q->qs->raptor_world, (unsigned char *)tmp);
        }
    } else {
        subject = raptor_new_term_from_uri(q->qs->raptor_world, uri);
    }
    for (int row = 0; row < result[0]->length; row++) {
        st.subject = subject;
        st.predicate = slot_fill_from_rid(q, result[0]->data[row]);
        st.object = slot_fill_from_rid(q, result[1]->data[row]);
        raptor_serializer_serialize_statement(q->ser, &st);
        raptor_free_term(st.predicate);
        raptor_free_term(st.object);
    }
    raptor_free_term(subject);
    for (int col=0; col<2; col++) {
        fs_rid_vector_free(result[col]);
    }
    free(result);
}

static void handle_describe(fs_query *q, const char *type, FILE *output)
{
    q->ser = raptor_new_serializer(q->qs->raptor_world, type);
    if (!q->ser) {
        fs_error(LOG_ERR, "no such serialiser '%s'", type);

        return;
    }
    for (int i=0; 1; i++) {
        rasqal_prefix *p = rasqal_query_get_prefix(q->rq, i);
        if (!p) break;
        raptor_serializer_set_namespace(q->ser, p->uri, p->prefix);
    }
    raptor_serializer_start_to_file_handle(q->ser, q->base, output);

    fs_p_vector *vars = fs_p_vector_new(0);
    raptor_sequence *desc = rasqal_query_get_describe_sequence(q->rq);
    for (int i=0; 1; i++) {
        rasqal_literal *l = raptor_sequence_get_at(desc, i);
        if (!l) break;
        if (l->type == RASQAL_LITERAL_URI) {
            describe_uri(q, FS_RID_NULL, l->value.uri);
        } else if (l->type == RASQAL_LITERAL_VARIABLE) {
            fs_p_vector_append(vars, l);
        }
    }

    fs_row *row;
    while ((row = fs_query_fetch_row(q))) {
        for (int i=0; i<vars->length; i++) {
            raptor_term *dterm = slot_fill(q, vars->data[i], row);
            /* can only describe URIs or bNodes */
            if (dterm->type == RAPTOR_TERM_TYPE_URI) {
                describe_uri(q, row[i].rid, dterm->value.uri);
            } else if (dterm->type == RAPTOR_TERM_TYPE_BLANK) {
                describe_uri(q, row[i].rid, NULL);
            }
            raptor_free_term(dterm);
        }
    }
    raptor_serializer_serialize_end(q->ser);
    raptor_free_serializer(q->ser);
    fs_p_vector_free(vars);
}

static void handle_construct(fs_query *q, const char *type, FILE *output)
{
    const int cols = fs_query_get_columns(q);
    fs_row *row;
    fs_rid quad[4] = { FS_RID_NULL, FS_RID_NULL, FS_RID_NULL,
                       FS_RID_NULL };

    if (q->flags & FS_RESULT_FLAG_CONSTRUCT_AS_INSERT) {
        fsp_start_import_all(q->link);
        quad[0] = fs_hash_uri((char *)raptor_uri_as_string(q->base));
        fs_resource model = { .rid = quad[0],
                              .lex = (char *)raptor_uri_as_string(q->base),
                              .attr = FS_RID_NULL };
        fsp_res_import(q->link, FS_RID_SEGMENT(quad[0], q->segments), 1,
                       &model);
        fs_rid_vector *models = fs_rid_vector_new(1);
        models->data[0] = quad[0];
        fsp_new_model_all(q->link, models);
    } else {
        q->ser = raptor_new_serializer(q->qs->raptor_world, type);
        for (int i=0; 1; i++) {
            rasqal_prefix *p = rasqal_query_get_prefix(q->rq, i);
            if (!p) break;
            raptor_serializer_set_namespace(q->ser, p->uri, p->prefix);
        }
        raptor_serializer_start_to_file_handle(q->ser, q->base, output);
    }

    while ((row = fs_query_fetch_row(q))) {
        if (q->flags & FS_RESULT_FLAG_CONSTRUCT_AS_INSERT) {
            for (int i=0; 1; i++) {
                rasqal_triple *trip =
                   rasqal_query_get_construct_triple(q->rq, i);
                if (!trip) break;

                insert_slot_fill(q, quad+1, trip->subject, row);
                insert_slot_fill(q, quad+2, trip->predicate, row);
                insert_slot_fill(q, quad+3, trip->object, row);
                fsp_quad_import(q->link, FS_RID_SEGMENT(quad[1], q->segments), FS_BIND_BY_SUBJECT, 1, &quad);
            }
        } else {
            raptor_statement st;
            raptor_statement_init(&st, q->qs->raptor_world);
            st.graph = NULL;

            for (int i=0; 1; i++) {
                rasqal_triple *trip =
                   rasqal_query_get_construct_triple(q->rq, i);
                if (!trip) break;

                st.subject = slot_fill(q, trip->subject, row);
                st.predicate = slot_fill(q, trip->predicate, row);
                st.object = slot_fill(q, trip->object, row);
                raptor_serializer_serialize_statement(q->ser, &st);
                raptor_free_term(st.subject);
                raptor_free_term(st.predicate);
                raptor_free_term(st.object);
            }
        }
    }

    /* if were CONSTRUCTing a constant expression, and the query is true */
    if (cols == 0 && q->boolean) {
        if (q->flags & FS_RESULT_FLAG_CONSTRUCT_AS_INSERT) {
            for (int i=0; 1; i++) {
                rasqal_triple *trip =
                   rasqal_query_get_construct_triple(q->rq, i);
                if (!trip) break;

                insert_slot_fill(q, quad+1, trip->subject, row);
                insert_slot_fill(q, quad+2, trip->predicate, row);
                insert_slot_fill(q, quad+3, trip->object, row);
                fsp_quad_import(q->link, FS_RID_SEGMENT(quad[1], q->segments), FS_BIND_BY_SUBJECT, 1, &quad);
            }
        } else {
            raptor_statement st;
            raptor_statement_init(&st, q->qs->raptor_world);
            st.graph = NULL;

            for (int i=0; 1; i++) {
                rasqal_triple *trip =
                   rasqal_query_get_construct_triple(q->rq, i);
                if (!trip) break;

                st.subject = slot_fill(q, trip->subject, row);
                st.predicate = slot_fill(q, trip->predicate, row);
                st.object = slot_fill(q, trip->object, row);
                raptor_serializer_serialize_statement(q->ser, &st);
                raptor_free_term(st.subject);
                raptor_free_term(st.predicate);
                raptor_free_term(st.object);
            }
        }
    }

    if (q->flags & FS_RESULT_FLAG_CONSTRUCT_AS_INSERT) {
        for (fs_segment s=0; s<q->segments; s++) {
            fsp_res_import_commit(q->link, s);
            fsp_quad_import_commit(q->link, s, FS_BIND_BY_SUBJECT);
        }
        fsp_stop_import_all(q->link);
    } else {
        raptor_serializer_serialize_end(q->ser);
        raptor_free_serializer(q->ser);
    }
}

static void output_sparql(fs_query *q, int flags, FILE *out)
{
    if (!q) return;

    if (flags & FS_RESULT_FLAG_HEADERS) {
	if (q->construct || q->describe) {
	    fprintf(out, "Content-Type: application/rdf+xml; charset=utf-8\r\n\r\n");
	} else {
	    fprintf(out, "Content-Type: application/sparql-results+xml\r\n\r\n");
	}
    }

    fs_row *row;
    int cols = fs_query_get_columns(q);
    if (q->construct) {
        handle_construct(q, "rdfxml", out);
    } else if (q->describe) {
        handle_describe(q, "rdfxml", out);
    } else {
	/* XML output */

	fprintf(out, "<?xml version=\"1.0\"?>\n"
		"<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">\n");
	row = fs_query_fetch_header_row(q);
	fprintf(out, "  <head>\n");
	for (int c=0; c<cols; c++) {
	    fprintf(out, "    <variable name=\"%s\"/>\n", row[c].name);
	}
	fprintf(out, "  </head>\n");
        if (q->warnings) {
            GSList *it;
            for (it = q->warnings; it; it = it->next) {
                int esc_len;
		if (xml_needs_escape(it->data, &esc_len)) {
		    char *lex = xml_escape(it->data, esc_len);
                    fprintf(out, "<!-- %s -->\n", lex);
                    free(lex);
                } else {
                    fprintf(out, "<!-- %s -->\n", (char *)it->data);
                }
            }
            g_slist_free(q->warnings);
            q->warnings = NULL;
        }
        if (q->ask) {
            while (q->boolean && fs_query_fetch_row(q));
            if (q->boolean) {
                fprintf(out, "  <boolean>true</boolean>\n");
            } else {
                fprintf(out, "  <boolean>false</boolean>\n");
            }
        } else {
            fprintf(out, "  <results>\n");
            while (!row->stop && (row = fs_query_fetch_row(q))) {
                fprintf(out, "    <result>\n");
                for (int c=0; c<cols; c++) {
                    int esc_len;
                    char *lex;
                    int need_free = 0;
                    if (xml_needs_escape(row[c].lex, &esc_len)) {
                        lex = xml_escape(row[c].lex, esc_len);
                        need_free = 1;
                    } else {
                        lex = (char *)row[c].lex;
                    }
                    if (row[c].type == FS_TYPE_NONE) continue;
                    fprintf(out, "      <binding name=\"%s\">", row[c].name);
                    switch (row[c].type) {
                        case FS_TYPE_NONE:
                            break;
                        case FS_TYPE_URI:
                            fprintf(out, "<uri>%s</uri>", lex);
                            break;
                        case FS_TYPE_LITERAL:
                            if (row[c].lang) {
                                fprintf(out, "<literal xml:lang=\"%s\">%s</literal>", row[c].lang, lex);
                            } else if (row[c].dt) {
                                fprintf(out, "<literal datatype=\"%s\">%s</literal>", row[c].dt, lex);
                            } else {
                                fprintf(out, "<literal>%s</literal>", lex);
                            }
                            break;
                        case FS_TYPE_BNODE:
                            fprintf(out, "<bnode>%s</bnode>", lex+2);
                    }
                    fprintf(out, "</binding>\n");
                    if (need_free) {
                        free(lex);
                    }
                }
                fprintf(out, "    </result>\n");
            }
            fprintf(out, "  </results>\n");
        }
        if (q->warnings) {
            GSList *it;
            char *last = NULL;
            for (it = q->warnings; it; it = it->next) {
                if (it->data == last) continue;
                last = it->data;
                int esc_len;
                if (xml_needs_escape(it->data, &esc_len)) {
                    char *lex = xml_escape(it->data, esc_len);
                    fprintf(out, "<!-- warning: %s -->\n", lex);
                    free(lex);
                } else {
                    fprintf(out, "<!-- warning: %s -->\n", (char *)it->data);
                }
            }
        }
	fprintf(out, "</sparql>\n");
    }
}

static void output_text(fs_query *q, int flags, FILE *out)
{
    if (!q) return;

    if (flags & FS_RESULT_FLAG_HEADERS) {
	if (q->construct) {
	    fprintf(out, "Content-Type: text/rdf+n3; charset=utf-8\r\n\r\n");
	} else {
	    fprintf(out, "Content-Type: text/tab-separated-values; charset=utf-8\r\n\r\n");
	}
    }

    if (q->ask) {
        while (q->boolean && fs_query_fetch_row(q));
        if (q->boolean) {
            fprintf(out, "true\n");
        } else {
            fprintf(out, "false\n");
        }

        return;
    }

    if (q->describe) {
        handle_describe(q, "turtle", out);

        return;
    }

    fs_row *row;

    int cols = fs_query_get_columns(q);
    if (!q->construct) {
	row = fs_query_fetch_header_row(q);
	for (int i=0; i<cols; i++) {
	    if (i) fputc('\t', out);
	    fprintf(out, "?%s", row[i].name);
	}
	fprintf(out, "\n");
    }

    if (q->warnings) {
        GSList *it;
        for (it = q->warnings; it; it = it->next) {
            if (it->data) {
                fprintf(out, "# %s\n", (char *)it->data);
            } else {
                fs_error(LOG_ERR, "found NULL warning");
            }
        }
        g_slist_free(q->warnings);
        q->warnings = NULL;
    }

    if (q->construct) {
        handle_construct(q, "ntriples", out);
    } else {
	while (!row->stop && (row = fs_query_fetch_row(q))) {
	    for (int c=0; c<cols; c++) {
		int esclen = 0;
		char *escd = NULL;
		const char *lex = row[c].lex;
		if (c) fputc('\t', out);
		switch (row[c].type) {
		    case FS_TYPE_NONE:
			fprintf(out, "NULL");
			break;
		    case FS_TYPE_URI:
                        if (uri_needs_escape(row[c].lex, &esclen)) {
                            escd = uri_escape(row[c].lex, esclen);
                            lex = escd;
                        }
			fprintf(out, "<%s>", lex);
                        if (escd) free(escd);
			break;
		    case FS_TYPE_LITERAL:
                        if (tsv_needs_escape(row[c].lex, &esclen)) {
                            escd = tsv_escape(row[c].lex, esclen);
                            lex = escd;
                        }
                        if (row[c].lang) {
			    fprintf(out, "\"%s\"@%s", lex, row[c].lang);
                        } else if (row[c].dt) {
			    fprintf(out, "\"%s\"^^<%s>", lex, row[c].dt);
                        } else {
			    fprintf(out, "\"%s\"", lex);
                        }
                        if (escd) free(escd);
			break;
		    case FS_TYPE_BNODE:
			fprintf(out, "%s", row[c].lex);
		}
	    }
	    fprintf(out, "\n");
	}
    }

    if (q->warnings) {
        GSList *it;
        for (it = q->warnings; it; it = it->next) {
            if (it->data) {
                fprintf(out, "# %s\n", (char *)it->data);
            } else {
                fs_error(LOG_ERR, "found NULL warning");
            }
        }
        g_slist_free(q->warnings);
        q->warnings = NULL;
    }
}

static void output_json(fs_query *q, int flags, FILE *out)
{
    if (!q) return;

    if (flags & FS_RESULT_FLAG_HEADERS) {
	if (q->construct) {
	    fprintf(out, "Content-Type: text/turtle\r\n\r\n");
	} else {
	    fprintf(out, "Content-Type: application/sparql-results+json\r\n\r\n");
	}
    }

    if (q->construct) {
        if (q->warnings) {
            GSList *it;
            for (it = q->warnings; it; it = it->next) {
                fprintf(out, "# %s\n", (char *)it->data);
            }
            g_slist_free(q->warnings);
            q->warnings = NULL;
        }
        handle_construct(q, "ntriples", out);
        if (q->warnings) {
            GSList *it;
            for (it = q->warnings; it; it = it->next) {
                fprintf(out, "# %s\n", (char *)it->data);
            }
            g_slist_free(q->warnings);
            q->warnings = NULL;
        }

        return;
    } else if (q->describe) {
        handle_describe(q, "json", out);

        return;
    }

    const int cols = fs_query_get_columns(q);

    fs_row *header = fs_query_fetch_header_row(q);
    fprintf(out, "{\"head\":{\"vars\":[");
    for (int i=0; i<cols; i++) {
        if (i) fputs(",", out);
        fprintf(out, "\"%s\"", header[i].name);
    }
    fprintf(out, "]},\n");

    if (q->ask) {
        while (q->boolean && fs_query_fetch_row(q));
        if (q->boolean) {
            fprintf(out, "\"boolean\": true");
        } else {
            fprintf(out, "\"boolean\": false");
        }
    } else {
        fprintf(out, " \"results\": {\n");
        fprintf(out, "  \"bindings\":[");
        fs_row *row;
        int rownum = 0;
        while ((!row || !row->stop) && (row = fs_query_fetch_row(q))) {
            if (rownum++ > 0) {
                fprintf(out, ",\n");
            } else {
                fprintf(out, "\n");
            }
            fprintf(out, "   {");
            for (int c=0; c<cols; c++) {
                if (c) fputs(",\n    ", out);
                fprintf(out, "\"%s\":{", header[c].name);
                int esclen = 0;
                char *escd = NULL;
                const char *lex = row[c].lex;
                switch (row[c].type) {
                    case FS_TYPE_NONE:
                        break;
                    case FS_TYPE_URI:
                        if (json_needs_escape(row[c].lex, &esclen)) {
                            escd = json_escape(row[c].lex, esclen);
                            lex = escd;
                        }
                        fprintf(out, "\"type\":\"uri\",\"value\":\"%s\"", lex);
                        if (escd) free(escd);
                        break;
                    case FS_TYPE_LITERAL:
                        if (json_needs_escape(row[c].lex, &esclen)) {
                            escd = json_escape(row[c].lex, esclen);
                            lex = escd;
                        }
                        if (row[c].lang) {
                            fprintf(out, "\"type\":\"literal\",\"value\":\"%s\",\"xml:lang\":\"%s\"", lex, row[c].lang);
                        } else if (row[c].dt) {
                            fprintf(out, "\"type\":\"literal\",\"value\":\"%s\",\"datatype\":\"%s\"", lex, row[c].dt);
                        } else {
                            fprintf(out, "\"type\":\"literal\",\"value\":\"%s\"", lex);
                        }
                        if (escd) free(escd);
                        break;
                    case FS_TYPE_BNODE:
                        fprintf(out, "\"type\":\"bnode\",\"value\":\"%s\"", row[c].lex + 2);
                }
                fputs("}", out);
            }
            fputs("}", out);
        }
        if (rownum) {
            fputs("\n  ", out);
        }
        fprintf(out, "]\n }");
    }

    if (q->warnings) {
        fprintf(out, ",\n \"warnings\": [");
        GSList *it;
        int count = 0;
        for (it = q->warnings; it; it = it->next) {
            if (count++) {
                printf(", ");
            }
            char *text = it->data;
            char *escd = NULL;
            int esclen = 0;
            if (json_needs_escape(text, &esclen)) {
                escd = json_escape(text, esclen);
                text = escd;
            }
            fprintf(out, "\"%s\"", text);
            if (escd) free(escd);
        }
        g_slist_free(q->warnings);
        q->warnings = NULL;
        fprintf(out, "]\n");
    }

    fprintf(out, "}\n");
}

static void output_testcase(fs_query *q, int flags, FILE *out)
{
    if (!q) return;

    if (q->errors) {
        if (q->warnings) {
            GSList *p = q->warnings;
            while (p) {
                fprintf(stderr, "# %s\n", (char *)p->data);
                p = p->next;
            }
        }

        return;
    }

    if (q->construct) {
	output_text(q, flags, out);

	return;
    } else if (q->describe) {
        handle_describe(q, "turtle", out);

        return;
    }

    if (flags & FS_RESULT_FLAG_HEADERS) {
	fprintf(out, "Content-Type: application/x-turtle\r\n\r\n");
    }
    fprintf(out, "@prefix rs: <http://www.w3.org/2001/sw/DataAccess/tests/result-set#> .\n");
    fprintf(out, "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
    fprintf(out, "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
    if (q->warnings) {
	GSList *p = q->warnings;
	while (p) {
	    fprintf(out, "# %s\n", (char *)p->data);
	    p = p->next;
	}
    }
    fs_row *row;
    int cols = fs_query_get_columns(q);
    fprintf(out, "\n[] rdf:type rs:ResultSet ;\n");
    if (q->num_vars > 0) {
	fprintf(out, "   rs:resultVariable ");
	row = fs_query_fetch_header_row(q);
	for (int c=0; c<cols; c++) {
	    if (c > 0) fprintf(out, ", ");
	    fprintf(out, "\"%s\"", row[c].name);
	}
    } else {
        /* apply the filters until we find one that matches or run out of
         * rows */
        while (q->boolean && fs_query_fetch_row(q));
        if (q->boolean) {
            fprintf(out, "   rs:boolean \"true\"^^xsd:boolean .\n");
        } else {
            fprintf(out, "   rs:boolean \"false\"^^xsd:boolean .\n");
        }

        return;
    }

    while (!row->stop && (row = fs_query_fetch_row(q))) {
	fprintf(out, " ;\n   rs:solution [\n");
        if (q->ordering) {
            static int index = 0;
            fprintf(out, "      rs:index %d ;\n", ++index);
        }

	int count = 0;
	for (int c=0; c<cols; c++) {
	    if (row[c].type == FS_TYPE_NONE) continue;

	    if (count++ > 0) fprintf(out, " ;\n");

	    fprintf(out, "      rs:binding [ rs:variable \"%s\" ;\n", row[c].name);
	    switch (row[c].type) {
		case FS_TYPE_NONE:
		    break;
		case FS_TYPE_URI:
		    fprintf(out, "                   rs:value <%s> ]", row[c].lex);
		    break;
		case FS_TYPE_LITERAL:
		    if (row[c].lang) {
			fprintf(out, "                   rs:value \"%s\"@%s ]", row[c].lex, row[c].lang);
		    } else if (row[c].dt) {
			fprintf(out, "                   rs:value \"%s\"^^<%s> ]", row[c].lex, row[c].dt);
		    } else {
			fprintf(out, "                   rs:value \"%s\" ]", row[c].lex);
		    }
		    break;
		case FS_TYPE_BNODE:
		    fprintf(out, "                   rs:value %s ]", row[c].lex);
	    }
	}
	fprintf(out, "\n   ]");
    }

    fprintf(out, " .\n");
}

int fs_query_get_columns(fs_query *q)
{
    if (!q) return 0;

    return q->num_vars;
}

fs_row *fs_query_fetch_header_row(fs_query *q)
{
    if (!q->resrow) {
	q->resrow = calloc(q->num_vars + 1, sizeof(fs_row));
	for (int col=0; col < q->num_vars; col++) {
	    q->resrow[col].name = q->bb[0][col+1].name;
	}
    }

    return q->resrow;
}

/* call with mutex held only */
static gboolean cache_dump(gpointer key, gpointer value, gpointer user_data)
{
    fs_resource *res = value;

    free(key);
    const int cpos = res->rid & CACHE_MASK;
    if (res_l2_cache[cpos].lex) {
        free(res_l2_cache[cpos].lex);
    }
    res_l2_cache[cpos] = *res;
    free(res);

    return 1;
}

static void prefetch_lexical_data(fs_query *q, long int next_row,const int rows) {

    /* ms8: aggregates prefetch everything in one go */
    if (q->aggregate > 2)
        return;
    if (q->aggregate) q->aggregate = 3; 

    for (int i=0; i<q->segments; i++) {
        fs_rid_vector_clear(q->pending[i]);
    }

    /* dump L1 cache into L2 */
    g_static_mutex_lock (&cache_mutex);
    if (res_l1_cache) g_hash_table_foreach_steal(res_l1_cache, cache_dump, NULL);

    int lookup_buffer_size = RESOURCE_LOOKUP_BUFFER;
    if (q->limit > 0 && q->limit < RESOURCE_LOOKUP_BUFFER) {
        lookup_buffer_size = q->limit * 2;
    }

    unsigned int pre_cache_len = 0;
    if (q->aggregate) {
        /* in case this is an aggregate we need to cache all of it
        because next_row has been moved to the last solution
        TODO: all RIDs are cached in one go */
       lookup_buffer_size = next_row;
    }
    for (int row=q->row; row < q->row + lookup_buffer_size && row < rows; row++) {
        for (int col=1; col <= q->num_vars_total; col++) {
            if (!q->bt[col].need_val && !q->bt[col].expression) continue;
            fs_rid rid;
            if (row < q->bt[col].vals->length) {
                if (q->ordering) {
                    rid = q->bt[col].vals->data[q->ordering[row]];
                } else {
                    rid = q->bt[col].vals->data[row];
                }
            } else {
                rid = FS_RID_NULL;
            }
            if (FS_IS_BNODE(rid)) continue;
            if (res_l2_cache[rid & CACHE_MASK].rid == rid) continue;
            pre_cache_len++;
            fs_rid_vector_append(q->pending[FS_RID_SEGMENT(rid, q->segments)], rid);
        }
    }
    g_static_mutex_unlock(&cache_mutex);
    if (pre_cache_len)
        resolve_precache_all_with_stats(q);
    q->qs->pre_cache_total += pre_cache_len;
    q->lastrow = q->row + lookup_buffer_size;
}



fs_row *fs_query_fetch_row(fs_query *q)
{
    if (!q) return NULL;

nextrow: ;
    long int next_row = q->row + 1;
    fs_rid_vector *grows = NULL;

    /* handle aggregates */
    if (q->aggregate && (q->aggregate < 2 || q->group_by)) {
        if (q->row >= q->length) {
            return NULL;
        }
        fs_rid_vector *groups = fs_binding_get_vals(q->bt, "_group", NULL);
        fs_rid_vector *ord = q->bt[0].vals;
        if (groups) {
            q->group_by = 1;

            next_row--;
            fs_rid group;

            int constrain_group = 0;
            do {
                group = groups->data[ord->data[next_row]];
                constrain_group = apply_constraints(q,ord->data[next_row]);
                if (constrain_group) {
                    grows = fs_rid_vector_new(0);
                    fs_rid_vector_append(grows, ord->data[next_row]);
                }
                next_row++;
            } while (next_row < q->length && !constrain_group);

            if (constrain_group) {
                while (next_row < q->length && groups->data[ord->data[next_row]] == group) {
                    if (apply_constraints(q,ord->data[next_row]))
                        fs_rid_vector_append(grows, ord->data[next_row]);
                    next_row++;
                }
            } else
                return NULL;
        } else {
            long len = fs_binding_length(q->bt);
            grows = fs_rid_vector_new(len);
            for (long i=0; i<len; i++) {
                grows->data[i] = i;
            }
            next_row = len;
        }
        q->group_length = grows->length;
        q->group_rows = (uint64_t *)grows->data;
    }

    const int rows = q->length;
    if (q->limit >= 0 && q->rows_output >= q->limit) {
        if (grows) fs_rid_vector_free(grows);
	return NULL;
    }
    if (q->row >= rows) {
	if (fsp_hit_limits(q->link)) {
	    fs_error(LOG_ERR, "hit soft limit %d times", fsp_hit_limits(q->link));
	    char *msg = g_strdup_printf("hit complexity limit %d times, increasing soft limit may give more results", fsp_hit_limits(q->link));
	    q->warnings = g_slist_prepend(q->warnings, msg);
	    fs_query_add_freeable(q, msg);
	}
        if (grows) fs_rid_vector_free(grows);
	return NULL;
    }

    if (!q->resrow) {
        fs_query_fetch_header_row(q);
    }

    if (q->row == q->lastrow && (q->aggregate < 3)) {
        prefetch_lexical_data(q, next_row, rows);
    }

    int row = q->row;
    /* consquence of this if is that you can't have grouping and ordering */
    if (q->aggregate) {
        /* set row number to first row in group */
        row = q->group_rows[0];
    } else if (q->ordering) {
        row = q->ordering[q->row];
    }

    int row_agg = 0;
    if (q->row < q->length && !q->group_by) {
        consnext: ;
        if (q->aggregate && !q->group_by) row = row_agg;
        if (!apply_constraints(q, row)) {
            q->boolean = 0;
            /* if we dont need any bindings we may as well stop */
            if (q->num_vars == 0) {
                if (grows) fs_rid_vector_free(grows);
                return NULL;
            }
            q->row = next_row;
            if (!q->aggregate)
                goto nextrow;
            else {
                if(!q->apply_constraints) q->apply_constraints = fs_new_bit_array(q->group_length);
                fs_bit_array_set(q->apply_constraints,row_agg,0);
            }
        } else if (q->aggregate) {
            q->row = next_row;
        }    
    }

    int repeat_row = 1;
    for (int i=0; i<q->num_vars; i++) {
        fs_rid last_rid = q->resrow[i].rid;
        q->resrow[i].rid = q->bt[i+1].bound && row < q->bt[i+1].vals->length ?
                           q->bt[i+1].vals->data[row] : FS_RID_NULL;
        if (last_rid != q->resrow[i].rid) repeat_row = 0;
        if (q->bt[i+1].expression) {
            fs_value val;
            if (!q->aggregate || q->group_by) {
                val = fs_expression_eval(q, row, 0, q->bt[i+1].expression);
                if (fs_is_error(val)) {
                    if (val.lex) {
                        if (!q->warnings || !g_slist_find(q->warnings, val.lex)) {
                            q->warnings = g_slist_prepend(q->warnings, val.lex);
                        }
                    }
                    q->row = next_row;
                    goto nextrow;
                }
            }
            
            if (!q->group_by) {
                if (q->aggregate && (row_agg+1) < q->length) {
                    row_agg++;
                    goto consnext;
                } else { 
                    val = fs_expression_eval(q, row, 0, q->bt[i+1].expression);
                }
            }
            fs_value_to_row(q, val, q->resrow+i);
        } else {
            fs_resource r;
            resolve(q, q->resrow[i].rid, &r);
            q->resrow[i].lex = r.lex;
            q->resrow[i].dt = NULL;
            q->resrow[i].lang = NULL;
            if (q->resrow[i].rid == FS_RID_NULL) {
                q->resrow[i].type = FS_TYPE_NONE;
            } else if (FS_IS_BNODE(q->resrow[i].rid)) {
                q->resrow[i].type = FS_TYPE_BNODE;
            } else if (FS_IS_URI(q->resrow[i].rid)) {
                q->resrow[i].type = FS_TYPE_URI;
            } else  {
                q->resrow[i].type = FS_TYPE_LITERAL;
                if (r.attr != FS_RID_NULL && r.attr != fs_c.empty) {
                    fs_rid attr = r.attr;
                    resolve(q, attr, &r);
                    if (FS_IS_URI(attr)) {
                        q->resrow[i].dt = r.lex;
                    } else {
                        q->resrow[i].lang = r.lex;
                    }
                }
            }
        }
    }

    /* if it's DISTINCT and there are FILTERS then we might have more
     * distincting to do */
    /* TODO could add REDUCED handling here too */
    if (q->flags & FS_BIND_DISTINCT && repeat_row && !q->aggregate) {
        q->row = next_row;
        goto nextrow;
    }

    q->row = next_row;
    q->rows_output++;
    if (grows) fs_rid_vector_free(grows);

    if (!q->group_by && q->aggregate) {
        q->resrow->stop=1;
        if (q->apply_constraints)
            fs_bit_array_destroy(q->apply_constraints);
    }

    return q->resrow;
}

void fs_query_results_output(fs_query *q, const char *fmt, int flags, FILE *out)
{
    if (!fmt) fmt = "sparql";

    if (!strcmp(fmt, "sparql")) {
	output_sparql(q, flags, out);
    } else if (!strcmp(fmt, "text") || !strcmp(fmt, "ascii")) {
	output_text(q, flags, out);
    } else if (!strcmp(fmt, "json")) {
	output_json(q, flags, out);
    } else if (!strcmp(fmt, "testcase")) {
	output_testcase(q, flags, out);
    } else {
	fprintf(out, "unknown format: %s\n", fmt);
    }
}

void fs_value_to_row(fs_query *q, fs_value v, fs_row *r)
{
    if (v.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
        r->rid = 1;
        r->dt = NULL;
        r->lang = NULL;
        r->lex = g_strdup_printf("error: %s", v.lex);
        fs_query_add_freeable(q, (char *)r->lex);

	return;
    }

    if (v.attr == fs_c.xsd_double) {
        r->rid = 1;
        r->dt = XSD_DOUBLE;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        if (v.lex) {
            r->lex = g_strdup(v.lex);
        } else {
            r->lex = g_strdup_printf("%g", v.fp);
        }
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_float) {
        r->rid = 1;
        r->dt = XSD_FLOAT;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        if (v.lex) {
            r->lex = g_strdup(v.lex);
        } else {
            r->lex = g_strdup_printf("%g", v.fp);
        }
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_decimal) {
        r->rid = 1;
        r->dt = XSD_DECIMAL;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        if (v.lex) {
            r->lex = g_strdup(v.lex);
        } else {
            r->lex = fs_decimal_to_lex(&v.de);
        }
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_integer) {
        r->rid = 1;
        r->dt = XSD_INTEGER;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        if (v.lex) {
            r->lex = g_strdup(v.lex);
        } else {
            r->lex = g_strdup_printf("%lld", (long long)v.in);
        }
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_boolean) {
        r->rid = 1;
        r->dt = XSD_BOOLEAN;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        if (v.lex) {
            r->lex = g_strdup(v.lex);
        } else {
            r->lex = g_strdup_printf("%lld", (long long)v.in);
        }
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_string) {
        r->rid = 1;
        r->dt = XSD_STRING;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        r->lex = g_strdup(v.lex);
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.xsd_datetime) {
        r->rid = 1;
        r->dt = XSD_DATETIME;
        r->lang = NULL;
        r->type = FS_TYPE_LITERAL;
        r->lex = g_strdup(v.lex);
        fs_query_add_freeable(q, (char *)r->lex);
    } else if (v.attr == fs_c.empty || v.attr == FS_RID_NULL) {
	if (v.rid == FS_RID_NULL) {
	} else if (FS_IS_BNODE(v.rid)) {
            r->rid = v.rid;
            r->dt = NULL;
            r->lang = NULL;
            r->type = FS_TYPE_BNODE;
            r->lex = g_strdup_printf("_:b%llx", FS_BNODE_NUM(v.rid));
        } else if (FS_IS_URI(v.rid)) {
            r->rid = 1;
            r->dt = NULL;
            r->lang = NULL;
            r->type = FS_TYPE_URI;
            r->lex = g_strdup(v.lex);
            fs_query_add_freeable(q, (char *)r->lex);
	} else {
            r->rid = 1;
            r->dt = NULL;
            r->lang = NULL;
            r->type = FS_TYPE_LITERAL;
            r->lex = g_strdup(v.lex);
            fs_query_add_freeable(q, (char *)r->lex);
	}
    } else {
        fs_resource res;
        resolve(q, v.attr, &res);
        r->rid = 1;
        if (FS_IS_URI(v.attr)) {
            r->dt = res.lex;
            r->lang = NULL;
        } else {
            r->dt = NULL;
            r->lang = res.lex;
        }
        r->type = FS_TYPE_LITERAL;
        r->lex = g_strdup(v.lex);
        fs_query_add_freeable(q, (char *)r->lex);
    }
}

struct simple_sort {
    int row;
    const char *str;
};

static int simple_sort_cmp(const void *va, const void *vb)
{
    const struct simple_sort *a = va, *b = vb;

    return strcmp(a->str, b->str);
}

static void rl_free(gpointer key, gpointer value, gpointer user_data)
{
    if (value != NULL_PROXY && value != BNODE_PROXY) {
        free(value);
    }
}

/* sort column by ORDER BY rules, only works on NULL, bNodes, URIs, simple and
 * xsd:strings */
int fs_sort_column(fs_query *q, fs_binding *b, int col, int **sorted)
{
    *sorted = NULL;

    fs_rid_vector *rv[q->segments];
    for (int s=0; s<q->segments; s++) {
        rv[s] = fs_rid_vector_new(0);
    }

    const int length = b[col].vals->length;

    /* resolve all the resources */
    for (int row=0; row < length; row++) {
        fs_rid rid = b[col].vals->data[row];
        if (FS_IS_BNODE(rid)) continue;
        fs_rid_vector_append(rv[FS_RID_SEGMENT(rid, q->segments)], rid);
    }
    fs_resource *res[q->segments];
    for (int s=0; s<q->segments; s++) {
        fs_rid_vector_sort(rv[s]);
        fs_rid_vector_uniq(rv[s], 0);
        res[s] = malloc(rv[s]->length * sizeof(fs_resource));
    }
    int ret = fsp_resolve_all(q->link, rv, res);
    if (ret) {
        fs_error(LOG_CRIT, "resolve_all failed");

        return 1;
    }

    /* store strings that will strcmp into the correct order */
    GHashTable *rl = g_hash_table_new_full(fs_rid_hash, fs_rid_equal, g_free, NULL);
    for (int s=0; s<q->segments; s++) {
        for (int i=0; i<rv[s]->length; i++) {
            fs_rid *rid = g_malloc(sizeof(fs_rid));
            *rid = res[s][i].rid;
            if (res[s][i].attr == fs_c.xsd_integer ||
                res[s][i].attr == fs_c.xsd_float ||
                res[s][i].attr == fs_c.xsd_double ||
                res[s][i].attr == fs_c.xsd_decimal ||
                res[s][i].attr == fs_c.xsd_datetime) {
                /* unsupported type */
                for (int s=0; s<q->segments; s++) {
                    for (int i=0; i<rv[s]->length; i++) {
                        free(res[s][i].lex);
                    }
                    free(res[s]);
                    fs_rid_vector_free(rv[s]);
                }
                g_free(rid);
                g_hash_table_foreach(rl, rl_free, NULL);
                g_hash_table_destroy(rl);

                return 1;
            }
            int len = strlen(res[s][i].lex);
            char *sort = malloc(len+2);
            memcpy(sort+1, res[s][i].lex, len);
            sort[len+1] = '\0';
            if (FS_IS_URI(res[s][i].rid)) {
                sort[0] = 'U';
            } else {
                if (res[s][i].attr == fs_c.xsd_string) {
                    sort[0] = 's';
                } else {
                    sort[0] = 'l';
                }
            }
            g_hash_table_insert(rl, rid, sort);
        }
    }
    for (int s=0; s<q->segments; s++) {
        for (int i=0; i<rv[s]->length; i++) {
            free(res[s][i].lex);
        }
        free(res[s]);
        fs_rid_vector_free(rv[s]);
    }

    struct simple_sort *sortable = malloc(sizeof(struct simple_sort) * length);
    for (int row=0; row < length; row++) {
        sortable[row].row = row;
        const fs_rid rid = b[col].vals->data[row];
        if (rid == FS_RID_NULL) {
            sortable[row].str = NULL_PROXY;
            continue;
        } else if (FS_IS_BNODE(rid)) {
            sortable[row].str = BNODE_PROXY;
            continue;
        }
        char *sort = g_hash_table_lookup(rl, &rid);
        if (!sort) {
            /* this shouldn't happen, but just incase */
            fs_error(LOG_ERR, "cannot get sortable string for %016llx", rid);

            return 1;
        }
        sortable[row].str = sort;
    }

    qsort(sortable, length, sizeof(struct simple_sort), simple_sort_cmp);

    g_hash_table_foreach(rl, rl_free, NULL);
    g_hash_table_destroy(rl);

    int *order = malloc(length * sizeof(int));
    for (int i=0; i<length; i++) {
        order[i] = sortable[i].row;
    }
    *sorted = order;

    free(sortable);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
