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

#define _GNU_SOURCE

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "filter.h"
#include "filter-datatypes.h"
#include "query-data.h"
#include "../common/4s-hash.h"
#include "../common/error.h"

gboolean fs_time_from_iso8601(const char *iso_date, time_t *time_);

fs_value fs_value_blank()
{
    fs_value v;
    memset(&v, 0, sizeof(v));
    v.attr = FS_RID_NULL;

    return v;
}

fs_value fs_value_rid(fs_rid r)
{
    fs_value v = fs_value_blank();
    if (r == FS_RID_GONE) {
        fs_error(LOG_ERR, "found RID_GONE value, optimiser elimination bug");
        r = FS_RID_NULL;
    }
    v.rid = r;
    v.valid = fs_valid_bit(FS_V_RID);

    return v;
}

fs_value fs_value_resource(fs_query *q, fs_resource *r)
{
    fs_value v = fs_value_blank();

    v.lex = r->lex;

    if (r->rid == FS_RID_NULL) {
        return fs_value_rid(FS_RID_NULL);
    } if (r->attr == fs_c.xsd_integer) {
        v = fn_cast_intl(q, v, fs_c.xsd_integer);
    } else if (r->attr == fs_c.xsd_float || r->attr == fs_c.xsd_double) {
        v = fn_cast_intl(q, v, fs_c.xsd_double);
    } else if (r->attr == fs_c.xsd_decimal) {
        v = fn_cast_intl(q, v, fs_c.xsd_decimal);
    } else if (r->attr == fs_c.xsd_boolean) {
        if (!strcmp(r->lex, "true") || !strcmp(r->lex, "1")) {
            v = fs_value_boolean(1);
        } else {
            v = fs_value_boolean(0);
        }
    } else if (r->attr == fs_c.xsd_datetime) {
        v = fs_value_datetime_from_string(r->lex);
    }
    if (fs_is_error(v)) {
        v = fs_value_blank();
        v.lex = r->lex;
    }
    v.rid = r->rid;
    if (FS_IS_URI(v.rid) || FS_IS_BNODE(v.rid))
        v.attr = fs_c.empty;
    else
        v.attr = r->attr;
    v.valid |= fs_valid_bit(FS_V_RID) | fs_valid_bit(FS_V_ATTR);

    return v;
}

fs_resource *fs_resource_value(fs_query *q, fs_value v)
{
    v = fs_value_fill_lexical(q, v);
    v = fs_value_fill_rid(q, v);
#if DEBUG_FILTER
    printf("resource->value ");
    fs_value_print(v);
    printf("\n");
#endif
    fs_resource *res = malloc(sizeof(fs_resource));
    res->rid = v.rid;
    res->attr = v.attr;
    res->lex = strdup(v.lex);

    return res;
}

fs_value fs_value_uri(const char *s)
{
    fs_value v = fs_value_blank();
    v.rid = fs_hash_uri(s);
    v.lex = (char *)s;
    v.valid = fs_valid_bit(FS_V_RID);
    v.attr = FS_RID_NULL;

    return v;
}

fs_value fs_value_plain(const char *s)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.empty;
    v.lex = (char *)s;

    return v;
}

fs_value fs_value_plain_with_lang(const char *s, const char *l)
{
    fs_value v = fs_value_blank();
    if (!l || *l == '\0') {
	v.attr = fs_c.empty;
    } else {
	v.attr = fs_hash_literal(l, 0);
    }
    v.lex = (char *)s;

    return v;
}

fs_value fs_value_plain_with_dt(const char *s, const char *d)
{
    fs_value v = fs_value_blank();
    if (!d || *d == '\0') {
	v.attr = fs_c.empty;
    } else {
	v.attr = fs_hash_uri(d);
    }
    v.lex = (char *)s;

    return v;
}

fs_value fs_value_string(const char *s)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_string;
    v.lex = (char *)s;

    return v;
}

fs_value fs_value_double(double f)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_double;
    v.valid = fs_valid_bit(FS_V_FP);
    v.fp = f;

    return v;
}

fs_value fs_value_float(double f)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_float;
    v.valid = fs_valid_bit(FS_V_FP);
    v.fp = f;

    return v;
}

fs_value fs_value_decimal(double d)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_decimal;
    v.valid = fs_valid_bit(FS_V_DE);

    fs_decimal_init_from_double(&v.de, d);

    return v;
}

fs_value fs_value_decimal_from_string(const char *s)
{
    fs_value v = fs_value_blank();

    int ret = fs_decimal_init_from_str(&v.de, s);
    if (ret) {
	return fs_value_error(FS_ERROR_INVALID_TYPE, "cannot convert value to decimal");
    }

    v.attr = fs_c.xsd_decimal;
    v.valid = fs_valid_bit(FS_V_DE);

    return v;
}

fs_value fs_value_integer(long long i)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_integer;
    v.valid = fs_valid_bit(FS_V_IN);
    v.in = i;

    return v;
}

fs_value fs_value_boolean(int b)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_boolean;
    v.valid = fs_valid_bit(FS_V_IN);
    v.in = b ? 1 : 0;
    if (v.in) {
	v.lex = "true";
    } else {
	v.lex = "false";
    }

    return v;
}

fs_value fs_value_datetime(time_t d)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_datetime;
    v.valid = fs_valid_bit(FS_V_IN);
    v.in = d;

    return v;
}

fs_value fs_value_datetime_from_string(const char *s)
{
    fs_value v = fs_value_blank();
    v.attr = fs_c.xsd_datetime;

    struct tm td;

    memset(&td, 0, sizeof(struct tm));

    time_t utc;
    if (fs_time_from_iso8601(s, &utc)) {
        v.in = utc;
        v.valid = fs_valid_bit(FS_V_IN);
        v.lex = (char *)s;

        return v;
    }
    char *ret = strptime(s, "%Y-%m-%d", &td);
    if (ret) {
	v.in = timegm(&td);
	v.valid = fs_valid_bit(FS_V_IN);

	return v;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE,
	    "cannot convert value to xsd:dateTime");
}

fs_value fs_value_error(fs_error e, const char *msg)
{
    fs_value v = fs_value_blank();
    v.valid = fs_valid_bit(FS_V_TYPE_ERROR);
    v.lex = (char *)msg;

    return v;
}

fs_value fs_value_promote(fs_query *q, fs_value a, fs_value b)
{
    if (!fs_is_numeric(&a) || !fs_is_numeric(&b)) {
        return fs_value_error(FS_ERROR_INVALID_TYPE,
                                "cannot promote non-numeric value");
    }
    if (a.attr == b.attr) {
	return a;
    }

    if (a.attr == fs_c.xsd_double || b.attr == fs_c.xsd_double) {
	if (a.attr != fs_c.xsd_double) {
	    a = fn_cast_intl(q, a, fs_c.xsd_double);
	}

	return a;
    }
    if (a.attr == fs_c.xsd_float || b.attr == fs_c.xsd_float) {
	if (a.attr != fs_c.xsd_float) {
	    a = fn_cast_intl(q, a, fs_c.xsd_float);
	}

	return a;
    }
    if (a.attr == fs_c.xsd_decimal || b.attr == fs_c.xsd_decimal) {
	if (a.attr != fs_c.xsd_decimal) {
	    a.valid |= fs_valid_bit(FS_V_DE);
	    if (a.attr == fs_c.xsd_integer || a.attr == fs_c.xsd_boolean) {
                fs_decimal_init_from_int64(&a.de, a.in);
	    } else {
		fs_error(LOG_ERR, "cannot convert type for dt %lld", a.attr);
                fs_decimal_init_from_int64(&a.de, 0);
	    }
	    a.attr = fs_c.xsd_decimal;
	}

	return a;
    }
    if (a.attr == fs_c.xsd_integer || b.attr == fs_c.xsd_integer) {
	if (a.attr != fs_c.xsd_integer) {
            if (a.valid & fs_valid_bit(FS_V_IN)) {
                a.attr = fs_c.xsd_integer;
            } else {
                fs_error(LOG_ERR, "cannot convert type for dt %lld", a.attr);
                a.in = 0;
            }
        }

	return a;
    }

    return fs_value_error(FS_ERROR_INVALID_TYPE, NULL);
}

int fs_is_numeric(fs_value *a)
{
    if (a->valid & fs_valid_bit(FS_V_FP) &&
        (a->attr == fs_c.xsd_double || a->attr == fs_c.xsd_float)) {
        return 1;
    }
    if (a->valid & fs_valid_bit(FS_V_IN) &&
        (a->attr == fs_c.xsd_integer || a->attr == fs_c.xsd_boolean)) {
        return 1;
    }
    if (a->valid & fs_valid_bit(FS_V_DE) &&
        (a->attr == fs_c.xsd_decimal)) {
        return 1;
    }
    if (a->attr == fs_c.xsd_pinteger ||
        a->attr == fs_c.xsd_ninteger ||
        a->attr == fs_c.xsd_npinteger ||
        a->attr == fs_c.xsd_nninteger ||
        a->attr == fs_c.xsd_long ||
        a->attr == fs_c.xsd_int ||
        a->attr == fs_c.xsd_short ||
        a->attr == fs_c.xsd_byte ||
        a->attr == fs_c.xsd_ulong ||
        a->attr == fs_c.xsd_uint ||
        a->attr == fs_c.xsd_ushort ||
        a->attr == fs_c.xsd_ubyte) {
        /* it's just a bloody integer, get over it */
        char *end = NULL;
        a->in = strtoll(a->lex, &end, 10);
        if (*end == '\0') {
            a->attr = fs_c.xsd_integer;
            a->valid |= fs_valid_bit(FS_V_IN);

            return 1;
        }
    }

    return 0;
}

int fs_is_error(fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	return 1;
    }

    return 0;
}

int fs_is_plain_or_string(fs_value v)
{
    if (fs_is_error(v)) {
        return 0;
    }
    if (FS_IS_BNODE(v.rid) || FS_IS_URI(v.rid)) {
        return 0;
    }
    if (v.attr != fs_c.empty && v.attr != fs_c.xsd_string) {
        return 0;
    }

    return 1;
}

/* return true if arg1 and arg2 are compatible, as per
 * http://www.w3.org/TR/sparql11-query/#func-arg-compatibility */
int fs_arg_compatible(fs_value arg1, fs_value arg2)
{
    /* The arguments are simple literals or literals typed as xsd:string */
    if (fs_is_plain_or_string(arg1) && fs_is_plain_or_string(arg2)) {
        return 1;
    }

    /* The arguments are plain literals with identical language tags */
    if (arg1.attr == arg2.attr && FS_IS_LITERAL(arg1.attr)) {
        return 1;
    }

    /* The first argument is a plain literal with language tag and the second
     * argument is a simple literal or literal typed as xsd:string */
    if (FS_IS_LITERAL(arg1.attr) && fs_is_plain_or_string(arg2)) {
        return 1;
    }

    return 0;
}

int fs_value_is_true(fs_value a)
{
    if (a.attr == fs_c.xsd_boolean) {
	return a.in;
    }
    if (a.attr == fs_c.xsd_integer) {
	return a.in != 0;
    }
    if (a.attr == fs_c.xsd_double || fs_c.xsd_float) {
	return fabs(a.fp) != 0.0;
    }
    if (a.attr == fs_c.xsd_decimal) {
	return !fs_decimal_equal(&a.de, fs_decimal_zero);
    }
    if (a.lex) {
	return strlen(a.lex);
    }

    return 0;
}

int fs_value_equal(fs_value a, fs_value b)
{
    if ((a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) &&
	(b.valid & fs_valid_bit(FS_V_TYPE_ERROR))) {
	return 1;
    }
    if (a.valid & fs_valid_bit(FS_V_RID) && b.valid & fs_valid_bit(FS_V_RID)) {
	return a.rid == b.rid;
    }
    if (a.attr != b.attr) {
	return 0;
    }
    if (a.attr == fs_c.xsd_double || a.attr == fs_c.xsd_float) {
	return a.fp == b.fp;
    } else if (a.attr == fs_c.xsd_decimal) {
	return fs_decimal_equal(&a.de, &b.de);
    } else if (a.attr == fs_c.xsd_integer) {
	return a.in == b.in;
    } else if (a.attr == fs_c.xsd_boolean) {
	return (a.in ? 1: 0) == (b.in ? 1: 0);
    } else if (a.attr == fs_c.xsd_datetime) {
	return a.in == b.in;
    } else if (a.attr == fs_c.xsd_string && a.lex && b.lex) {
	return !strcmp(a.lex, b.lex);
    } else if (a.attr == b.attr && a.lex && b.lex) {
	return !strcmp(a.lex, b.lex);
    }

    return 0;
}

void fs_value_print(fs_value v)
{
    if (v.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
	printf("error");
	if (v.lex) {
	    printf("(%s)", v.lex);
	}

	return;
    }

    if (v.attr == fs_c.xsd_double) {
	printf("db");
    } else if (v.attr == fs_c.xsd_float) {
	printf("fl");
    } else if (v.attr == fs_c.xsd_decimal) {
	printf("de");
    } else if (v.attr == fs_c.xsd_integer) {
	printf("in");
    } else if (v.attr == fs_c.xsd_boolean) {
	printf("bl");
    } else if (v.attr == fs_c.xsd_string) {
	printf("st");
    } else if (v.attr == fs_c.xsd_datetime) {
	printf("dt");
    } else if (v.attr == fs_c.empty || v.attr == FS_RID_NULL) {
	if (v.rid == FS_RID_NULL) {
	    printf("NULL");
	} else if (FS_IS_BNODE(v.rid)) {
	    printf("bnode");
        } else if (FS_IS_URI(v.rid)) {
	    printf("uri");
	} else {
            printf("plain");
	}
    } else {
	printf("attr:%llx", v.attr);
    }

    if (v.valid & fs_valid_bit(FS_V_RID)) {
	printf(" rid:%016llx", v.rid);
    }
    if (v.lex) {
	printf(" l:%s", v.lex);
    }
    if (v.valid & fs_valid_bit(FS_V_FP)) {
	printf(" f:%f", v.fp);
    }
    if (v.valid & fs_valid_bit(FS_V_DE)) {
        char *dlex = fs_decimal_to_lex(&v.de);
	printf(" d:%s", dlex);
        free(dlex);
    }
    if (v.valid & fs_valid_bit(FS_V_IN)) {
	printf(" i:%lld", (long long)v.in);
    }
}

fs_value fs_value_fill_lexical(fs_query *q, fs_value a)
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

    return fs_value_error(FS_ERROR_INVALID_TYPE, "¡bad lexical cast!");
}

fs_value fs_value_fill_rid(fs_query *q, fs_value a)
{
    if (a.valid & fs_valid_bit(FS_V_RID)) {
        return a;
    }

    if (a.valid & fs_valid_bit(FS_V_TYPE_ERROR)) {
        a.rid = FS_RID_NULL;
    }

    fs_value_fill_lexical(q, a);

    a.rid = fs_hash_literal(a.lex, a.attr);
    a.valid |= fs_valid_bit(FS_V_RID);

    return a;
}

/* N.B. this code is taken from glib 2.12.13 */
gboolean fs_time_from_iso8601(const char *iso_date, time_t *time_)
{
  struct tm tm = { 0 };
  long val;

  g_return_val_if_fail (iso_date != NULL, FALSE);
  g_return_val_if_fail (time_ != NULL, FALSE);

  val = strtoul(iso_date, (char **)&iso_date, 10);
  if (*iso_date == '-') {
      /* YYYY-MM-DD */
      tm.tm_year = val - 1900;
      iso_date++;
      tm.tm_mon = strtoul (iso_date, (char **)&iso_date, 10) - 1;
      
      if (*iso_date++ != '-')
       	return FALSE;
      
      tm.tm_mday = strtoul (iso_date, (char **)&iso_date, 10);
  } else {
      /* YYYYMMDD */
      tm.tm_mday = val % 100;
      tm.tm_mon = (val % 10000) / 100 - 1;
      tm.tm_year = val / 10000 - 1900;
  }

  if (*iso_date++ != 'T') return FALSE;
  
  val = strtoul(iso_date, (char **)&iso_date, 10);
  if (*iso_date == ':') {
      /* hh:mm:ss */
      tm.tm_hour = val;
      iso_date++;
      tm.tm_min = strtoul (iso_date, (char **)&iso_date, 10);
      
      if (*iso_date++ != ':')
        return FALSE;
      
      tm.tm_sec = strtoul (iso_date, (char **)&iso_date, 10);
  } else {
      /* hhmmss */
      tm.tm_sec = val % 100;
      tm.tm_min = (val % 10000) / 100;
      tm.tm_hour = val / 10000;
  }

  *time_ = timegm(&tm);
  
  if (*iso_date == '+' || *iso_date == '-') {
      gint sign = (*iso_date == '+') ? -1 : 1;
      
      val = 60 * strtoul (iso_date + 1, (char **)&iso_date, 10);
      
      if (*iso_date == ':') {
	  val = 60 * val + strtoul (iso_date + 1, NULL, 10);
      } else {
          val = 60 * (val / 100) + (val % 100);
      }

      *time_ += (time_t) (val * sign);
  }

  return TRUE;
}


/* vi:set expandtab sts=4 sw=4: */
