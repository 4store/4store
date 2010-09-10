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

    Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <pcre.h>

#include "filter-datatypes.h"
#include "filter.h"
#include "query.h"
#include "common/error.h"
#include "common/hash.h"
#include "common/rdf-constants.h"

static fs_value cast_lexical(fs_query *q, fs_value a);

static fs_value cast_double(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_FP)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_DE)) {
	a.valid |= fs_valid_bit(FS_V_FP);
	fs_decimal_to_double(&a.de, &a.fp);

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_IN)) {
	a.valid |= fs_valid_bit(FS_V_FP);
	a.fp = a.in;

	return a;
    }
    if (a.lex) {
        char *end = NULL;
        a.fp = strtod(a.lex, &end);
        if (*end == '\0') {
            a.valid |= fs_valid_bit(FS_V_FP);

            return a;
        }
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad cast to double");
}

static fs_value cast_decimal(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_DE)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_FP)) {
	a.valid |= fs_valid_bit(FS_V_DE);
        fs_decimal_init_from_double(&a.de, a.fp);

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_IN)) {
	a.valid |= fs_valid_bit(FS_V_DE);
        fs_decimal_init_from_int64(&a.de, a.in);

	return a;
    }
    if (a.lex) {
	a.valid |= fs_valid_bit(FS_V_DE);
        if (!fs_decimal_init_from_str(&a.de, a.lex)) {
            return a;
        }
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad cast to decimal");
}

static fs_value cast_datetime(fs_query *q, fs_value a)
{
    if (a.lex) {
        fs_value ret = fs_value_datetime_from_string(a.lex);
        ret.lex = g_strdup(a.lex);
        fs_query_add_freeable(q, ret.lex);

	return ret;
    }

    if (a.valid & fs_valid_bit(FS_V_IN) && a.attr == fs_c.xsd_datetime) {
        fs_value b = cast_lexical(q, a);

        return b;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad cast to datetime");
}

static fs_value cast_lexical(fs_query *q, fs_value a)
{
    if (a.lex) {
	return a;
    }	
    if (a.valid & fs_valid_bit(FS_V_FP)) {
	a.lex = g_strdup_printf("%f", a.fp);
        fs_query_add_freeable(q, a.lex);

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_DE)) {
	a.lex = fs_decimal_to_lex(&a.de);
        fs_query_add_freeable(q, a.lex);

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_IN)) {
	if (a.attr == fs_c.xsd_integer) {
	    a.lex = g_strdup_printf("%lld", (long long)a.in);
            fs_query_add_freeable(q, a.lex);

	    return a;
	}

	if (a.attr == fs_c.xsd_datetime) {
	    struct tm t;
	    time_t clock = a.in;
	    gmtime_r(&clock, &t);
	    a.lex = g_strdup_printf("%04d-%02d-%02dT%02d:%02d:%02d", 
		    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
		    t.tm_hour, t.tm_min, t.tm_sec);
            fs_query_add_freeable(q, a.lex);

	    return a;
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad lexical cast");
}

static fs_value cast_integer(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_IN)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_FP)) {
	a.valid |= fs_valid_bit(FS_V_IN);
	a.in = a.fp;

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_DE)) {
	a.valid |= fs_valid_bit(FS_V_IN);
	fs_decimal_to_int64(&a.de, &a.in);

	return a;
    }
    if (a.lex) {
        char *end = NULL;
        a.in = strtoll(a.lex, &end, 10);
        if (*end == '\0') {
            a.valid |= fs_valid_bit(FS_V_IN);

            return a;
        }
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad integer cast");
}

static fs_value cast_boolean(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_IN)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_FP)) {
	a.valid |= fs_valid_bit(FS_V_IN);
	a.in = a.fp;

	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_DE)) {
	a.valid |= fs_valid_bit(FS_V_IN);
	fs_decimal_to_int64(&a.de, &a.in);

	return a;
    }
    if (a.lex) {
        if (!strcmp(a.lex, "true")) {
            a.valid |= fs_valid_bit(FS_V_IN);
            a.in = 1;

            return a;
        } else if (!strcmp(a.lex, "false")) {
            a.valid |= fs_valid_bit(FS_V_IN);
            a.in = 0;

            return a;
        }
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad integer cast");
}

fs_value fn_plus(fs_query *q, fs_value a)
{
#if 0
fs_value_print(a);
#endif
    if (fs_is_numeric(&a)) {
	return a;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:plus");
}

fs_value fn_minus(fs_query *q, fs_value a)
{
    if (fs_is_numeric(&a)) {
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    a.fp = -a.fp;
	} else if (a.attr == fs_c.xsd_decimal) {
            fs_decimal_negate(&a.de, &a.de);
	} else if (a.attr == fs_c.xsd_integer) {
	    a.in = -a.in;
	} else {
	    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:minus");
	}

	return a;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:minus");
}

fs_value fn_numeric_add(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" + ");
fs_value_print(b);
printf("\n");
#endif
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);
#if 0
fs_value_print(a);
printf(" P+ ");
fs_value_print(b);
printf("\n");
#endif

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	fs_value v = fs_value_blank();
	v.attr = a.attr;
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    v.fp = a.fp + b.fp;
	    v.valid = fs_valid_bit(FS_V_FP);

	    return v;
	} else if (a.attr == fs_c.xsd_decimal) {
            fs_decimal_add(&a.de, &b.de, &v.de);
	    v.valid = fs_valid_bit(FS_V_DE);

	    return v;
	} else if (a.attr == fs_c.xsd_integer) {
	    v.in = a.in + b.in;
	    v.valid = fs_valid_bit(FS_V_IN);

	    return v;
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:add");
}

fs_value fn_numeric_subtract(fs_query *q, fs_value a, fs_value b)
{
    return fn_numeric_add(q, a, fn_minus(q, b));
}

fs_value fn_numeric_multiply(fs_query *q, fs_value a, fs_value b)
{
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	fs_value v = fs_value_blank();
	v.attr = a.attr;
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    v.fp = a.fp * b.fp;
	    v.valid = fs_valid_bit(FS_V_FP);

	    return v;
	} else if (a.attr == fs_c.xsd_decimal) {
            fs_decimal_multiply(&a.de, &b.de, &v.de);
	    v.valid = fs_valid_bit(FS_V_DE);

	    return v;
	} else if (a.attr == fs_c.xsd_integer) {
	    v.in = a.in * b.in;
	    v.valid = fs_valid_bit(FS_V_IN);

	    return v;
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:numeric-multiply");
}

fs_value fn_numeric_divide(fs_query *q, fs_value a, fs_value b)
{
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	fs_value v = fs_value_blank();
	if (a.attr == fs_c.xsd_integer) {
	    fs_decimal_init_from_int64(&a.de, a.in);
	    fs_decimal_init_from_int64(&b.de, b.in);
	    a.attr = fs_c.xsd_decimal;
	}
	v.attr = a.attr;
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    v.fp = a.fp / b.fp;
	    v.valid = fs_valid_bit(FS_V_FP);

	    return v;
	} else if (a.attr == fs_c.xsd_decimal) {
            fs_decimal_divide(&a.de, &b.de, &v.de);
	    v.valid = fs_valid_bit(FS_V_DE);

	    return v;
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:numeric-divide");
}

fs_value fn_equal(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" = ");
fs_value_print(b);
printf("\n");
#endif
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

    fs_value term_equal = fn_rdfterm_equal(q, a, b);
    if (term_equal.in == 1) {
        return term_equal;
    }

    if (a.attr == fs_c.xsd_datetime)
        return fn_datetime_equal(q, a, b);
    if (fs_is_numeric(&a) && fs_is_numeric(&b))
        return fn_numeric_equal(q, a, b);

    if (FS_IS_LITERAL(a.rid) && FS_IS_LITERAL(b.rid) &&
        (a.attr == fs_c.empty || a.attr == fs_c.xsd_string) &&
        (b.attr == fs_c.empty || b.attr == fs_c.xsd_string)) {
        return fs_value_boolean(!strcmp(a.lex, b.lex));
    }

    return fs_value_boolean(0);
}

fs_value fn_not_equal(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" != ");
fs_value_print(b);
printf("\n");
#endif
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

    if (a.attr == fs_c.xsd_datetime)
        return fn_not(q, fn_datetime_equal(q, a, b));

    if (fs_is_numeric(&a) && fs_is_numeric(&b))
        return fn_not(q, fn_numeric_equal(q, a, b));

    if ((a.attr == fs_c.empty || a.attr == fs_c.xsd_string) &&
        (b.attr == fs_c.empty || b.attr == fs_c.xsd_string)) {
        return fs_value_boolean(strcmp(a.lex, b.lex));
    }

    if ((FS_IS_URI_BN(a.rid) && FS_IS_LITERAL(b.rid)) ||
        (FS_IS_LITERAL(a.rid) && FS_IS_URI_BN(b.rid))) {
        /* ones a URI/bNode and ones a literal, definatly different */
        return fs_value_boolean(1);
    }

    if ((!FS_IS_URI(a.rid) && a.attr != fs_c.empty && FS_IS_LITERAL(a.attr) &&
         !FS_IS_LITERAL(b.attr)) ||
        (!FS_IS_URI(a.rid) && !FS_IS_LITERAL(a.attr) && b.attr != fs_c.empty &&
         FS_IS_LITERAL(b.attr))) {
        /* one has a lang tag and one doesn't, definatly different */
        return fs_value_boolean(1);
    }

    if (FS_IS_URI(a.attr) || FS_IS_URI(b.attr)) {
        /* at least one argument has an unknown datatype */
        return fs_value_boolean(0);
    }

    return fn_not(q, fn_rdfterm_equal(q, a, b));
}

fs_value fn_greater_than(fs_query *q, fs_value a, fs_value b)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime)
        return fn_datetime_greater_than(q, a, b);
    if (fs_is_numeric(&a)) return fn_numeric_greater_than(q, a, b);
    if (a.lex && b.lex)
        return fn_numeric_equal(q, fn_compare(q, a, b), fs_value_integer(1));

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

fs_value fn_less_than(fs_query *q, fs_value a, fs_value b)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }
#if 0
fs_value_print(a);
printf(" < ");
fs_value_print(b);
printf("\n");
#endif

    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime) return fn_datetime_less_than(q, a, b);
    if (fs_is_numeric(&a)) return fn_numeric_less_than(q, a, b);
    if (a.lex && b.lex) return fn_numeric_equal(q, fn_compare(q, a, b), fs_value_integer(-1));

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

fs_value fn_greater_than_equal(fs_query *q, fs_value a, fs_value b)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

    /* URIs and bNodes don't compare */
    if (FS_IS_URI_BN(a.rid) || FS_IS_URI_BN(b.rid)) {
        return fs_value_boolean(0);
    }

    /* If it's simply the same term it must be <= itsself */
    fs_value term_eq = fn_rdfterm_equal(q, a, b);
    if (term_eq.in == 1) {
        return term_eq;
    }

    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime)
        return fn_not(q, fn_datetime_less_than(q, a, b));

    if (fs_is_numeric(&a))
        return fn_logical_or(q, fn_numeric_greater_than(q, a, b), fn_numeric_equal(q, a, b));

    if (FS_IS_URI_BN(a.rid) || FS_IS_URI_BN(b.rid))
        return fs_value_boolean(0);

    if (a.lex && b.lex)
        return fn_not(q, fn_numeric_equal(q,fn_compare(q, a, b), fs_value_integer(-1)));

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

fs_value fn_less_than_equal(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" <= ");
fs_value_print(b);
printf("\n");
#endif
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

    /* URIs and bNodes don't compare */
    if (FS_IS_URI_BN(a.rid) || FS_IS_URI_BN(b.rid)) {
        return fs_value_boolean(0);
    }

    /* If it's simply the same term it must be <= itsself */
    fs_value term_eq = fn_rdfterm_equal(q, a, b);
    if (term_eq.in == 1) {
        return term_eq;
    }

    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime) return fn_not(q, fn_datetime_greater_than(q, a, b));
    if (fs_is_numeric(&a) && fs_is_numeric(&b)) return fn_logical_or(q, fn_numeric_less_than(q, a, b), fn_numeric_equal(q, a, b));

    if (a.lex && b.lex)
        return fn_not(q, fn_numeric_equal(q, fn_compare(q, a, b), fs_value_integer(1)));

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

fs_value fn_numeric_equal(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" ");
fs_value_print(b);
printf("\n");
#endif
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    return fs_value_boolean(a.fp == b.fp);
	}
	if (a.attr == fs_c.xsd_decimal) {
	    return fs_value_boolean(fs_decimal_equal(&a.de, &b.de));
	}
	if (a.attr == fs_c.xsd_integer) {
	    return fs_value_boolean(a.in == b.in);
	}
	if (a.attr == fs_c.xsd_boolean) {
	    return fs_value_boolean((a.in ? 1 : 0) == (b.in ? 1 : 0));
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:numeric-equal");
}

fs_value fn_numeric_less_than(fs_query *q, fs_value a, fs_value b)
{
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);
#if 0
fs_value_print(a);
printf(" < ");
fs_value_print(b);
printf("\n");
#endif

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    return fs_value_boolean(a.fp < b.fp);
	}
	if (a.attr == fs_c.xsd_decimal) {
	    return fs_value_boolean(fs_decimal_less_than(&a.de, &b.de));
	}
	if (a.attr == fs_c.xsd_integer) {
	    return fs_value_boolean(a.in < b.in);
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:numeric-less-than");
}

fs_value fn_numeric_greater_than(fs_query *q, fs_value a, fs_value b)
{
    a = fs_value_promote(q, a, b);
    b = fs_value_promote(q, b, a);

    if (a.attr == b.attr && a.attr != FS_RID_NULL && a.attr != fs_c.empty) {
	if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	    return fs_value_boolean(a.fp > b.fp);
	}
	if (a.attr == fs_c.xsd_decimal) {
	    return fs_value_boolean(fs_decimal_greater_than(&a.de, &b.de));
	}
	if (a.attr == fs_c.xsd_integer) {
	    return fs_value_boolean(a.in > b.in);
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:numeric-greater-than");
}

fs_value fn_datetime_equal(fs_query *q, fs_value a, fs_value b)
{
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime)
	return fs_value_boolean(a.in == b.in);

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:datetime-equal");
}

fs_value fn_datetime_less_than(fs_query *q, fs_value a, fs_value b)
{
#if 0
fs_value_print(a);
printf(" < ");
fs_value_print(b);
printf(" [dT]\n");
#endif
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime)
	return fs_value_boolean(a.in < b.in);

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:datetime-less-than");
}

fs_value fn_datetime_greater_than(fs_query *q, fs_value a, fs_value b)
{
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime)
	return fs_value_boolean(a.in > b.in);

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:datetime-greater-than");
}

fs_value fn_compare(fs_query *q, fs_value a, fs_value b)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (b.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return b;
    }

#if 0
fs_value_print(a);
printf(" <=> ");
fs_value_print(b);
printf("\n");
#endif
    if ((FS_IS_LITERAL(a.attr) && FS_IS_LITERAL(b.attr)) ||
	(a.attr == fs_c.empty && b.attr == fs_c.empty)) {
	if (a.lex && b.lex) {
	    int diff = strcmp(a.lex, b.lex);
	    if (diff > 0) {
		return fs_value_integer(1);
	    } else if (diff < 0) {
		return fs_value_integer(-1);
	    }
	    return fs_value_integer(0);
	}
    } else if (a.attr == fs_c.xsd_string && b.attr == fs_c.xsd_string) {
	if (a.lex && b.lex) {
	    int diff = strcmp(a.lex, b.lex);
	    if (diff > 0) {
		return fs_value_integer(1);
	    } else if (diff < 0) {
		return fs_value_integer(-1);
	    }
	    return fs_value_integer(0);
	}
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:compare");
}

fs_value fn_rdfterm_equal(fs_query *q, fs_value a, fs_value b)
{
    if (fs_is_error(a)) {
	return a;
    }
    if (fs_is_error(b)) {
	return b;
    }

    return fs_value_boolean(fs_value_equal(a, b));
}

fs_value fn_logical_and(fs_query *q, fs_value a, fs_value b)
{
    fs_value ea = fn_ebv(a), eb = fn_ebv(b);

    if (ea.in && fs_is_error(b)) {
	return b;
    } else if (eb.in && fs_is_error(a)) {
	return a;
    } else if (fs_is_error(a) && fs_is_error(b)) {
	return a;
    }

    return fs_value_boolean(ea.in && eb.in);
}

fs_value fn_logical_or(fs_query *q, fs_value a, fs_value b)
{
    fs_value ea = fn_ebv(a), eb = fn_ebv(b);

    if (!ea.in && fs_is_error(b)) {
	return b;
    } else if (!eb.in && fs_is_error(a)) {
	return a;
    } else if (fs_is_error(a) && fs_is_error(b)) {
	return a;
    }

    return fs_value_boolean(ea.in || eb.in);
}

fs_value fn_not(fs_query *q, fs_value a)
{
#if 0
printf("! ");
fs_value_print(a);
printf("\n");
#endif
    if (fs_is_error(a)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_RID) && a.rid == FS_RID_NULL) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    fs_value ebv = fn_ebv(a);

    if (fs_is_error(ebv)) {
	return ebv;
    }

    return fs_value_boolean(!ebv.in);
}

fs_value fn_lang_matches(fs_query *q, fs_value l, fs_value p)
{
    if (fs_is_error(l)) {
	return l;
    }
    if (fs_is_error(p)) {
	return p;
    }
    if (l.valid & fs_valid_bit(FS_V_RID) && l.rid == FS_RID_NULL) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (l.lex && l.lex[0] == '\0') {
        return fs_value_boolean(0);
    }

    if (p.lex && p.lex[0] == '*' && p.lex[1] == '\0') {
	return fs_value_boolean(1);
    }

    if (l.lex && p.lex) {
        /* TODO implement RFC3066 */
        return fs_value_boolean(!strncasecmp(l.lex, p.lex, strlen(p.lex)));
    }

    return fs_value_boolean(0);
}

fs_value fn_bound(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_RID)) {
	return fs_value_boolean(a.rid != FS_RID_NULL);
    }

    return fs_value_boolean(1);
}

fs_value fn_is_iri(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_RID)) {
	return fs_value_boolean(FS_IS_URI(a.rid));
    }
    if (a.attr == FS_RID_NULL) {
	return fs_value_boolean(1);
    }

    return fs_value_boolean(0);
}

fs_value fn_is_blank(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_RID)) {
	if (a.rid == FS_RID_NULL) return fs_value_boolean(0);
	
	return fs_value_boolean(FS_IS_BNODE(a.rid));
    }

    return fs_value_boolean(0);
}

fs_value fn_is_literal(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (a.valid & fs_valid_bit(FS_V_RID)) {
	return fs_value_boolean(FS_IS_LITERAL(a.rid));
    }

    return fs_value_boolean(a.attr != FS_RID_NULL);
}

fs_value fn_str(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }

    if (a.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(a.rid)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (a.lex) {
	return fs_value_plain(a.lex);
    }

    fs_value v = fs_value_plain(NULL);
    a = cast_lexical(q, a);
    v.lex = a.lex;

    return v;
}

fs_value fn_uri(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }

    if (a.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(a.rid)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (a.lex) {
	return fs_value_uri(a.lex);
    }

    a = cast_lexical(q, a);
    fs_value v = fs_value_uri(a.lex);

    return v;
}

fs_value fn_lang(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }

    if (a.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI_BN(a.rid)) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (a.attr == FS_RID_NULL) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (FS_IS_URI(a.attr)) {
	return fs_value_plain("");
    } else {
	if (a.attr == fs_c.lang_en) {
	    return fs_value_plain("en");
	} else if (a.attr == fs_c.lang_fr) {
	    return fs_value_plain("fr");
	} else if (a.attr == fs_c.lang_de) {
	    return fs_value_plain("de");
	} else if (a.attr == fs_c.lang_es) {
	    return fs_value_plain("es");
	} else if (a.attr == fs_c.empty) {
	    return fs_value_plain("");
	}
    }

    fs_rid_vector *r = fs_rid_vector_new(1);
    r->data[0] = a.attr;
    fs_resource res;
    if (fs_query_link(q)) {
        fsp_resolve(fs_query_link(q), FS_RID_SEGMENT(a.attr,
                    fsp_link_segments(fs_query_link(q))), r, &res);
        fs_rid_vector_free(r);

        return fs_value_plain(res.lex);
    }

    return fs_value_plain("???");
}

fs_value fn_datatype(fs_query *q, fs_value a)
{
#if 0
printf("datatype(");
fs_value_print(a);
printf(")\n");
#endif
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }

    if (a.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI_BN(a.rid)) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (a.attr == FS_RID_NULL) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    if (FS_IS_LITERAL(a.attr) && a.attr != fs_c.empty) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    } else {
	if (a.attr == fs_c.xsd_string || a.attr == fs_c.empty) {
	    return fs_value_uri(XSD_STRING);
	} else if (a.attr == fs_c.xsd_double) {
	    return fs_value_uri(XSD_DOUBLE);
	} else if (a.attr == fs_c.xsd_float) {
	    return fs_value_uri(XSD_FLOAT);
	} else if (a.attr == fs_c.xsd_decimal) {
	    return fs_value_uri(XSD_DECIMAL);
	} else if (a.attr == fs_c.xsd_integer) {
	    return fs_value_uri(XSD_INTEGER);
	} else if (a.attr == fs_c.xsd_boolean) {
	    return fs_value_uri(XSD_BOOLEAN);
	} else if (a.attr == fs_c.xsd_datetime) {
	    return fs_value_uri(XSD_DATETIME);
	}
    }

    fs_rid_vector *r = fs_rid_vector_new(1);
    r->data[0] = a.attr;
    fs_resource res;
    if (fs_query_link(q)) {
        fsp_resolve(fs_query_link(q), FS_RID_SEGMENT(a.attr,
                    fsp_link_segments(fs_query_link(q))), r, &res);
        fs_rid_vector_free(r);

        return fs_value_uri(res.lex);
    }

    return fs_value_uri("error:unresloved");
}

fs_value fn_matches(fs_query *q, fs_value str, fs_value pat, fs_value flags)
{
    if (str.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return str;
    }
    if (pat.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return pat;
    }
    if (flags.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return flags;
    }

    if (!str.lex || !pat.lex) {
	return fs_value_error(FS_ERROR_INVALID_TYPE,
                              "argument to fn:matches has no lexical value");
    }

    if (str.valid & fs_valid_bit(FS_V_RID) && FS_IS_URI(str.rid)) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
#if 0
printf("REGEX ");
fs_value_print(str);
printf(", ");
fs_value_print(pat);
printf(", ");
fs_value_print(flags);
printf("\n");
#endif

    int reflags = PCRE_UTF8;
    if (flags.lex) {
	for (char *c = flags.lex; *c; c++) {
	    switch (*c) {
		case 's':
		    reflags |= PCRE_DOTALL;
		    break;
		case 'm':
		    reflags |= PCRE_MULTILINE;
		    break;
		case 'i':
		    reflags |= PCRE_CASELESS;
		    break;
		case 'x':
		    reflags |= PCRE_EXTENDED;
		    break;
		default:
		    fs_error(LOG_ERR, "unknown regex flag '%c'", *c);
		    return fs_value_error(FS_ERROR_INVALID_TYPE, "unrecognised flag in fn:matches");
	    }
	}
    }

    const char *error;
    int erroroffset;
    pcre *re = pcre_compile(pat.lex, reflags, &error, &erroroffset, NULL);
    if (!re) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, error);
    }
    int rc = pcre_exec(re, NULL, str.lex, strlen(str.lex), 0, 0, NULL, 0);
    if (rc == PCRE_ERROR_NOMATCH) {
	return fs_value_boolean(0);
    }
    if (rc < 0) {
        fs_error(LOG_ERR, "internal error %d in pcre_exec", rc);

	return fs_value_error(FS_ERROR_INVALID_TYPE, "internal error in fn:matches");
    }

    return fs_value_boolean(1);
}

fs_value fn_cast_intl(fs_query *q, fs_value v, fs_rid dt)
{
    if (dt == fs_c.xsd_double || dt == fs_c.xsd_float) {
	v = cast_double(v);
    } else if (dt == fs_c.xsd_integer) {
	v = cast_integer(v);
    } else if (dt == fs_c.xsd_decimal) {
	v = cast_decimal(v);
    } else if (dt == fs_c.xsd_string) {
	v = cast_lexical(q, v);
    } else if (dt == fs_c.xsd_datetime) {
	v = cast_datetime(q, v);
    } else if (dt == fs_c.xsd_boolean) {
        v = cast_boolean(v);
    }
    v.attr = dt;

    return v;
}

fs_value fn_cast(fs_query *q, fs_value v, fs_value d)
{
#if 0
printf("CAST ");
fs_value_print(v);
printf(" -> ");
fs_value_print(d);
printf("\n");
#endif
    if (FS_IS_URI(d.rid) && FS_IS_LITERAL(v.rid)) {
	return fn_cast_intl(q, v, d.rid);
    }
    if (d.rid == fs_c.xsd_string && FS_IS_URI(v.rid)) {
        fs_value v2 = fn_cast_intl(q, v, d.rid);
        v2.rid = fs_hash_literal(v.lex, d.rid);
	return v2;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "cast on URI/bNode");
}

fs_value fn_ebv(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }
    if (a.rid == FS_RID_NULL) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    if (a.attr == fs_c.xsd_boolean || a.attr == fs_c.xsd_integer) {
	return fs_value_boolean(a.in);
    }
    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	return fs_value_boolean(fabs(a.fp) != 0.0);
    }
    if (a.attr == fs_c.xsd_decimal) {
	return fs_value_boolean(!fs_decimal_equal(&a.de, fs_decimal_zero));
    }
    if (a.lex && (a.attr == fs_c.xsd_string || a.attr == fs_c.empty)) {
	return fs_value_boolean(a.lex && a.lex[0]);
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

/* vi:set expandtab sts=4 sw=4: */
