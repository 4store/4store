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
#include <time.h>
#include <errno.h>

#include "filter-datatypes.h"
#include "filter.h"
#include "query.h"
#include "../common/uuid.h"
#include "../common/4s-hash.h"
#include "../common/error.h"
#include "../common/rdf-constants.h"
#include "../libs/mt19937-64/mt64.h"

typedef struct _fs_date_fields {
    int year;
    int month;
    int day;
} fs_date_fields;

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
    if (a.lex && strlen(a.lex)) {
        char *end = NULL;
        a.fp = strtod(a.lex, &end);
        /* this is bad, but some code somewhere is checking errno when a
         * function is returning sucess and getting tripped up by this */
        errno = 0;
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
    if (a.lex && strlen(a.lex)) {
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
        fs_value b = fs_value_fill_lexical(q, a);

        return b;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad cast to datetime");
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
    if (a.lex && strlen(a.lex)) {
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

        if (a.lex != NULL) {
            a.lex = NULL;
        }
        return a;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric arguments to fn:minus");
}

fs_value fn_numeric_abs(fs_query *q, fs_value a)
{
    if (!fs_is_numeric(&a)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric argument to fn:abs");
    }

    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
        a.fp = fabs(a.fp);
    } else if (a.attr == fs_c.xsd_decimal) {
        if (fs_decimal_less_than(&a.de, fs_decimal_zero)) {
            fs_decimal_negate(&a.de, &a.de);
        }
    } else if (a.attr == fs_c.xsd_integer) {
        if (a.in < 0) {
            a.in = -a.in;
        }
    } else {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:abs");
    }

    if (a.lex != NULL) {
        a.lex = NULL;
    }

    return a;
}

fs_value fn_numeric_floor(fs_query *q, fs_value a)
{
    if (!fs_is_numeric(&a)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric argument to fn:floor");
    }

    if (a.attr == fs_c.xsd_integer) {
        /* do nothing for integers */
        return a;
    }

    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
        a.fp = floor(a.fp);
    } else if (a.attr == fs_c.xsd_decimal) {
        fs_decimal r;

        if (fs_decimal_less_than(&a.de, fs_decimal_zero)) {
            fs_decimal one;
            fs_decimal_init_from_double(&one, (double)1.0);
            fs_decimal_subtract(&a.de, &one, &r);
        }
        else {
            fs_decimal_copy(&a.de, &r);
        }

        for (int i=FS_D_OVER_DIGITS+FS_D_INT_DIGITS; i < FS_D_DIGITS; i++) {
            r.digit[i] = 0;
        }

        fs_decimal_copy(&r, &a.de);
    } else {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:floor");
    }

    if (a.lex != NULL) {
        a.lex = NULL;
    }

    return a;
}

fs_value fn_numeric_ceil(fs_query *q, fs_value a)
{
    if (!fs_is_numeric(&a)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric argument to fn:ceil");
    }

    if (a.attr == fs_c.xsd_integer) {
        /* do nothing for integers */
        return a;
    }

    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
        a.fp = ceil(a.fp);
    } else if (a.attr == fs_c.xsd_decimal) {
        fs_decimal r;

        if (fs_decimal_greater_than(&a.de, fs_decimal_zero)) {
            fs_decimal one;
            fs_decimal_init_from_double(&one, (double)1.0);
            fs_decimal_add(&a.de, &one, &r);
        }
        else {
            fs_decimal_copy(&a.de, &r);
        }

        for (int i=FS_D_OVER_DIGITS+FS_D_INT_DIGITS; i < FS_D_DIGITS; i++) {
            r.digit[i] = 0;
        }

        fs_decimal_copy(&r, &a.de);
    } else {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:ceil");
    }

    if (a.lex != NULL) {
        a.lex = NULL;
    }

    return a;
}

fs_value fn_numeric_round(fs_query *q, fs_value a)
{
    if (!fs_is_numeric(&a)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "non-numeric argument to fn:round");
    }

    if (a.attr == fs_c.xsd_integer) {
        /* do nothing for integers */
        return a;
    }

    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
        if (a.fp > 0.0) {
            /* rounding +ve numbers same as c round */
            a.fp = round(a.fp);
        }
        else if (a.fp < 0.0) {
            /* -2.5 should round to -2.0 */
            double i;
            double f = modf(a.fp, &i);
            a.fp -= f;
            if (f < -0.5) {
                a.fp -= 1.0;
            }
        }
    } else if (a.attr == fs_c.xsd_decimal) {
        /* 0 = round to num,        e.g. 2.x -> 2.0, -2.x -> -2.0
           1 = round away from num, e.g. 2.x -> 3.0, -2.x -> -3.0 */
        int round_dir = -1; 
        int positive = fs_decimal_greater_than_equal(&a.de, fs_decimal_zero);

        int start_pos = FS_D_OVER_DIGITS+FS_D_INT_DIGITS;
        if ((&a.de)->digit[start_pos] > 5) {
            round_dir = 1;
        } else if ((&a.de)->digit[start_pos] < 5) {
            round_dir = 0;
        } else {
            /* first digit should be a 5 */
            for (int i=start_pos+1; i < FS_D_DIGITS; i++) {
                if ((&a.de)->digit[i] > 0) {
                    /* abs(fractional part) is > 0.5 */
                    round_dir = 1;
                    break;
                }
            }

            if (round_dir == -1) {
                /* abs(fractional part) == 0.5, round depending on sign */
                if (positive) {
                    round_dir = 1;
                }
                else {
                    round_dir = 0;
                }
            }
        }

        fs_decimal one;
        fs_decimal_init_from_double(&one, (double)1.0);
        fs_decimal r;

        if (round_dir == 1) {
            /* Round integral part of number away from zero */
            if (positive) {
                fs_decimal_add(&a.de, &one, &r);
            }
            else {
                fs_decimal_subtract(&a.de, &one, &r);
            }
        }
        else {
            /* Keep integral part of number unchanged */
            fs_decimal_copy(&a.de, &r);
        }

        /* Zero all fractional digits of number */
        for (int i=start_pos; i < FS_D_DIGITS; i++) {
            r.digit[i] = 0;
        }

        fs_decimal_copy(&r, &a.de);
    } else {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:round");
    }

    if (a.lex != NULL) {
        a.lex = NULL;
    }

    return a;
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
            if (fs_decimal_divide(&a.de, &b.de, &v.de)) {
                return fs_value_error(FS_ERROR_INVALID_TYPE, "divide by zero");
            }
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
    if (fs_is_numeric(&a) && fs_is_numeric(&b)) 
        return fn_numeric_greater_than(q, a, b);
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
    if (fs_is_numeric(&a) && fs_is_numeric(&b))
        return fn_numeric_less_than(q, a, b);
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

    if (fs_is_numeric(&a) && fs_is_numeric(&b))
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

static int iso8601_compare(const char *a, const char *b)
{
    if (a == NULL && b == NULL) {
        return 0;
    } else if (a == NULL) {
        return -1;
    } else if (b == NULL) {
        return 1;
    }

    if (*a == '-' && *b == '-') {
        int cmp = strcmp(a, b);
        if (cmp < 0) {
            return 1;
        } else if (cmp > 0) {
            return -1;
        }

        return 0;
    }

    if (*a == '-') {
        return -1;
    }
    if (*b == '-') {
        return 1;
    }

    int cmp = strcmp(a, b);
    if (cmp < 0) {
        return -1;
    } else if (cmp > 0) {
        return 1;
    }

    return 0;
}

fs_value fn_datetime_equal(fs_query *q, fs_value a, fs_value b)
{
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime &&
        (a.in != -1 || b.in != -1))
	return fs_value_boolean(a.in == b.in);

    if (a.lex && b.lex) {
        return fs_value_boolean(iso8601_compare(a.lex, b.lex) == 0);
    }

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
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime &&
        (a.in != -1 || b.in != -1))
	return fs_value_boolean(a.in < b.in);

    if (a.lex && b.lex) {
        return fs_value_boolean(iso8601_compare(a.lex, b.lex) == -1);
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, "bad arguments to fn:datetime-less-than");
}

fs_value fn_datetime_greater_than(fs_query *q, fs_value a, fs_value b)
{
    if (a.attr == fs_c.xsd_datetime && b.attr == fs_c.xsd_datetime &&
        (a.in != -1 || b.in != -1))
	return fs_value_boolean(a.in > b.in);

    if (a.lex && b.lex) {
        return fs_value_boolean(iso8601_compare(a.lex, b.lex) == 1);
    }

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
    a = fs_value_fill_lexical(q, a);
    v.lex = a.lex;

    return v;
}

fs_value fn_uri(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return a;
    }

    if (a.valid & fs_valid_bit(FS_V_RID) && FS_IS_BNODE(a.rid)) {
        fs_value v = fs_value_blank();
        v.lex = g_strdup_printf("bnode:b%llx", FS_BNODE_NUM(a.rid));
        fs_query_add_freeable(q, v.lex);
        v.rid = fs_hash_uri_ignore_bnode(v.lex);
        v.valid = fs_valid_bit(FS_V_RID);
        v.attr = FS_RID_NULL;

        return v;
    }

    if (a.lex) {
	return fs_value_uri(a.lex);
    }

    a = fs_value_fill_lexical(q, a);
    fs_value v = fs_value_uri(a.lex);

    return v;
}

fs_value fn_bnode(fs_query *q, fs_value a)
{
    a = fs_value_fill_rid(q, a);
    /* scramble the RID number a bit */
    fs_value b = fs_value_blank();
    b.rid = a.rid + q->block * 39916801;
    b.rid += q->row;
    b.rid += FS_NUM_BNODE(a.rid & ~0xC000000000000000LL);
    b.valid = fs_valid_bit(FS_V_RID);
    b.attr = FS_RID_NULL;

    return b;
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
        fs_query_add_freeable(q, res.lex);

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

static int regex_flags(const char *flags)
{
    int reflags = PCRE_UTF8;
    if (flags) {
	for (const char *c = flags; *c; c++) {
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

                    return 0;
	    }
	}
    }

    return reflags;
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

    str = fs_value_fill_lexical(q, str);
    pat = fs_value_fill_lexical(q, pat);
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

    int reflags = regex_flags(flags.lex);
    if (!reflags) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "unrecognised flag in fn:matches");
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
    } else if (dt == fs_c.xsd_integer || dt == fs_c.xsd_int) {
	v = cast_integer(v);
    } else if (dt == fs_c.xsd_decimal) {
	v = cast_decimal(v);
    } else if (dt == fs_c.xsd_string) {
	v = fs_value_fill_lexical(q, v);
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

fs_value fn_substring(fs_query *q, fs_value str, fs_value start, fs_value length)
{
    if (!fs_is_plain_or_string(str)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    if (!fs_is_numeric(&start)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    start = cast_integer(start);
    if (start.in < 1) start.in = 1;
    /* 2 arg form */
    if (length.rid == FS_RID_NULL) {
        length = fs_value_integer(INT_MAX);
    } else {
        if (!fs_is_numeric(&length)) {
            return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
        }
        length = cast_integer(length);
    }
    str = fs_value_fill_lexical(q, str);
    const int slen = g_utf8_strlen(str.lex, -1);
    if (start.in > slen || length.in <= 0) {
        fs_value ret = fs_value_plain("");
        ret.attr = str.attr;

        return ret;
    }
    gchar *spos = g_utf8_offset_to_pointer(str.lex, start.in - 1);
    int retlen_utf8 = g_utf8_strlen(spos, -1);
    if (retlen_utf8 > length.in) {
        retlen_utf8 = length.in;
    }
    gchar *epos = g_utf8_offset_to_pointer(spos, retlen_utf8);
    int retlen_bytes = epos - spos + 1;
    char *retstr = g_malloc(retlen_bytes+1);
    retstr[retlen_bytes] = '\0';
    g_utf8_strncpy(retstr, spos, retlen_utf8);
    fs_query_add_freeable(q, retstr);
    fs_value ret = fs_value_plain(retstr);
    ret.attr = str.attr;

    return ret;
}

fs_value fn_ucase(fs_query *q, fs_value v)
{
    if (!fs_is_plain_or_string(v)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    char *lex = g_utf8_strup(v.lex, -1);
    fs_query_add_freeable(q, lex);
    fs_value ret = fs_value_plain(lex);
    ret.attr = v.attr;

    return ret;
}

fs_value fn_lcase(fs_query *q, fs_value v)
{
    if (!fs_is_plain_or_string(v)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    char *lex = g_utf8_strdown(v.lex, -1);
    fs_query_add_freeable(q, lex);
    fs_value ret = fs_value_plain(lex);
    ret.attr = v.attr;

    return ret;
}

fs_value fn_encode_for_uri(fs_query *q, fs_value v)
{
    if (!fs_is_plain_or_string(v)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
#ifdef HAVE_G_URI_ESCAPE_STRING
    char *lex = g_uri_escape_string(v.lex, NULL, TRUE);
#else
    char *lex = fs_uri_escape(v.lex);
#endif
    fs_query_add_freeable(q, lex);
    fs_value ret = fs_value_plain(lex);

    return ret;
}

/* make a date struct from an ISO8601 string, returns non-0 on failure */
static int date_from_iso8601(char *iso, fs_date_fields *df)
{
    int matches = sscanf(iso, "%d-%d-%d", &(df->year), &(df->month), &(df->day));
    if (matches != 3) {
        return 1;
    }
    if (df->month < 1 || df->month > 12) {
        return 1;
    }
    if (df->day < 1 || df->day > 31) {
        return 1;
    }

    return 0;
}

fs_value fn_year(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    fs_date_fields df;
    if (date_from_iso8601(v.lex, &df)) {
        char *err = g_strdup_printf("cannot get year from xsd:dateTime %s", v.lex);
        fs_query_add_freeable(q, err);

        return fs_value_error(FS_ERROR_INVALID_TYPE, err);
    }

    return fs_value_integer(df.year);
}

fs_value fn_month(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    fs_date_fields df;
    if (date_from_iso8601(v.lex, &df)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot get month from xsd:dateTime");
    }

    return fs_value_integer(df.month);
}

fs_value fn_day(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    fs_date_fields df;
    if (date_from_iso8601(v.lex, &df)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot get day from xsd:dateTime");
    }

    return fs_value_integer(df.day);
}

fs_value fn_hours(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    GTimeVal tv;
    if (!g_time_val_from_iso8601(v.lex, &tv)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot get hours from xsd:date");
    }

    return fs_value_integer((tv.tv_sec / 3600) % 24);
}

fs_value fn_minutes(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    GTimeVal tv;
    if (!g_time_val_from_iso8601(v.lex, &tv)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot get minutes from xsd:date");
    }

    return fs_value_integer((tv.tv_sec / 60) % 60);
}

fs_value fn_seconds(fs_query *q, fs_value v)
{
    if (v.attr != fs_c.xsd_datetime) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    v = fs_value_fill_lexical(q, v);
    GTimeVal tv;
    if (!g_time_val_from_iso8601(v.lex, &tv)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot get seconds from xsd:date");
    }

    return fs_value_integer(tv.tv_sec % 60);
}

fs_value fn_timezone(fs_query *q, fs_value v)
{
    return fs_value_error(FS_ERROR_INVALID_TYPE, "TIMEZONE() function not suported");
}

fs_value fn_strstarts(fs_query *q, fs_value arg1, fs_value arg2)
{
    if (!fs_arg_compatible(arg1, arg2)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    arg1 = fs_value_fill_lexical(q, arg1);
    arg2 = fs_value_fill_lexical(q, arg2);

    return fs_value_boolean(strncmp(arg1.lex, arg2.lex, strlen(arg2.lex)) == 0);
}

fs_value fn_strends(fs_query *q, fs_value arg1, fs_value arg2)
{
    if (!fs_arg_compatible(arg1, arg2)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    arg1 = fs_value_fill_lexical(q, arg1);
    arg2 = fs_value_fill_lexical(q, arg2);

    const int a1l = strlen(arg1.lex);
    const int a2l = strlen(arg2.lex);

    if (a2l > a1l) {
        return fs_value_boolean(0);
    }

    return fs_value_boolean(strncmp(arg1.lex + a1l - a2l, arg2.lex, a2l) == 0);
}

fs_value fn_contains(fs_query *q, fs_value arg1, fs_value arg2)
{
    if (!fs_arg_compatible(arg1, arg2)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    arg1 = fs_value_fill_lexical(q, arg1);
    arg2 = fs_value_fill_lexical(q, arg2);

    return fs_value_boolean(strstr(arg1.lex, arg2.lex) != NULL);
}

fs_value fn_strbefore(fs_query *q, fs_value arg1, fs_value arg2)
{
    if (!fs_arg_compatible(arg1, arg2)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    arg1 = fs_value_fill_lexical(q, arg1);
    arg2 = fs_value_fill_lexical(q, arg2);

    char *pos = strstr(arg1.lex, arg2.lex);
    if (!pos) {
        fs_value ret = fs_value_plain("");
        ret.attr = arg1.attr;

        return ret;
    }

    char *new = g_strndup(arg1.lex, pos-arg1.lex);
    fs_query_add_freeable(q, new);

    fs_value ret = fs_value_plain(new);
    ret.attr = arg1.attr;

    return ret;
}

fs_value fn_strafter(fs_query *q, fs_value arg1, fs_value arg2)
{
    if (!fs_arg_compatible(arg1, arg2)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }

    arg1 = fs_value_fill_lexical(q, arg1);
    arg2 = fs_value_fill_lexical(q, arg2);

    char *pos = strstr(arg1.lex, arg2.lex);
    if (!pos) {
        fs_value ret = fs_value_plain("");
        ret.attr = arg1.attr;

        return ret;
    }

    char *new = g_strdup(pos + strlen(arg2.lex));
    fs_query_add_freeable(q, new);

    fs_value ret = fs_value_plain(new);
    ret.attr = arg1.attr;

    return ret;
}

fs_value fn_rand(fs_query *q)
{
    static int seeded = 0;
    if (!seeded) {
        init_genrand64(time(NULL));
        seeded = 1;
    }
    
    fs_value v = fs_value_double(genrand64_real2());
    v.lex = g_strdup_printf("%.17f", v.fp);
    fs_query_add_freeable(q, v.lex);

    return v;
}

#if GLIB_MAJOR_VERSION >= 2 && GLIB_MINOR_VERSION >= 16
fs_value fn_hash_intl(fs_query *q, fs_value arg, GChecksumType type)
{
    if (!fs_is_plain_or_string(arg)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
    }
    arg = fs_value_fill_lexical(q, arg);

    GChecksum *sum = g_checksum_new(type);
    g_checksum_update(sum, (guchar *)arg.lex, -1);
    char *str = g_strdup(g_checksum_get_string(sum));
    g_checksum_free(sum);
    fs_query_add_freeable(q, str);

    return fs_value_plain(str);
}
#endif

fs_value fn_md5(fs_query *q, fs_value arg)
{
#if GLIB_MAJOR_VERSION >= 2 && GLIB_MINOR_VERSION >= 16
    return fn_hash_intl(q, arg, G_CHECKSUM_MD5);
#else
    return fs_value_error(FS_ERROR_INVALID_TYPE, "glib version does not support hash functions, at least 2.16.0 required");
#endif
}

fs_value fn_sha1(fs_query *q, fs_value arg)
{
#if GLIB_MAJOR_VERSION >= 2 && GLIB_MINOR_VERSION >= 16
    return fn_hash_intl(q, arg, G_CHECKSUM_SHA1);
#else
    return fs_value_error(FS_ERROR_INVALID_TYPE, "glib version does not support hash functions, at least 2.16.0 required");
#endif
}

fs_value fn_sha256(fs_query *q, fs_value arg)
{
#if GLIB_MAJOR_VERSION >= 2 && GLIB_MINOR_VERSION >= 16
    return fn_hash_intl(q, arg, G_CHECKSUM_SHA256);
#else
    return fs_value_error(FS_ERROR_INVALID_TYPE, "glib version does not support hash functions, at least 2.16.0 required");
#endif
}

fs_value fn_uuid(fs_query *q)
{
#if defined(USE_LINUX_UUID)
    uuid_t uu;
    uuid_string_t uus;
    uuid_generate(uu);
    uuid_unparse(uu, uus);
#elif defined(USE_BSD_UUID)
    uuid_t uu;
    char *uus = NULL;
    int status = -1;
    uuid_create(&uu, &status);
    if (status) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    uuid_to_string(&uu, &uus, &status);
    if (status || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_to_string"); exit(1); }
#elif defined(USE_OSSP_UUID)
    uuid_t *uu = NULL;
    char *uus = NULL;
    if (uuid_create(&uu)) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    if (uuid_make(uu, UUID_MAKE_V1)) { fs_error(LOG_ERR, "bad return from uuid_make"); exit(1); }
    if (uuid_export(uu, UUID_FMT_STR, &uus, NULL) || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_export"); exit(1); }
#endif
    char *str = g_strdup_printf("urn:uuid:%s", uus);
#if defined(USE_OSSP_UUID)
    uuid_destroy(uu);
#endif
    fs_query_add_freeable(q, str);

    return fs_value_uri(str);
}

fs_value fn_struuid(fs_query *q)
{
#if defined(USE_LINUX_UUID)
    uuid_t uu;
    uuid_string_t uus;
    uuid_generate(uu);
    uuid_unparse(uu, uus);
#elif defined(USE_BSD_UUID)
    uuid_t uu;
    char *uus = NULL;
    int status = -1;
    uuid_create(&uu, &status);
    if (status) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    uuid_to_string(&uu, &uus, &status);
    if (status || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_to_string"); exit(1); }
#elif defined(USE_OSSP_UUID)
    uuid_t *uu = NULL;
    char *uus = NULL;
    if (uuid_create(&uu)) { fs_error(LOG_ERR, "bad return from uuid_create"); exit(1); }
    if (uuid_make(uu, UUID_MAKE_V1)) { fs_error(LOG_ERR, "bad return from uuid_make"); exit(1); }
    if (uuid_export(uu, UUID_FMT_STR, &uus, NULL) || uus == NULL) { fs_error(LOG_ERR, "bad return from uuid_export"); exit(1); }
#endif
    char *str = g_strdup(uus);
#if defined(USE_OSSP_UUID)
    uuid_destroy(uu);
#endif
    fs_query_add_freeable(q, str);

    return fs_value_plain(str);
}

/* vi:set expandtab sts=4 sw=4: */
