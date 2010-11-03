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

#include "filter.h"
#include "../common/hash.h"
#include "../common/rdf-constants.h"

int main()
{
    fs_hash_init(FS_HASH_UMAC);

    int passes = 0;
    int fails = 0;

#if 1
#define DUMP(t) printf("%lld %s\n", fs_c.xsd_##t, #t);
    DUMP(double);
    DUMP(float);
    DUMP(decimal);
    DUMP(integer);
    DUMP(boolean);
#endif

#define TEST1(f, a, x) printf("[%s] ", fs_value_equal(f(NULL, a), x) ? "PASS" : "FAIL"); printf("%s(", #f); fs_value_print(a); printf(") = "); fs_value_print(f(NULL, a)); printf("\n"); if (!fs_value_equal(f(NULL, a), x)) { fails++; printf("       should have been "); fs_value_print(x); printf("\n"); } else { passes++; }
#define TEST2(f, a, b, x) printf("[%s] ", fs_value_equal(f(NULL, a, b), x) ? "PASS" : "FAIL"); printf("%s(", #f); fs_value_print(a); printf(", "); fs_value_print(b); printf(") = "); fs_value_print(f(NULL, a, b)); printf("\n"); if (!fs_value_equal(f(NULL, a, b), x)) { fails++; printf("       should have been "); fs_value_print(x); printf("\n"); } else { passes++; }
#define TEST3(f, a, b, c, x) printf("[%s] ", fs_value_equal(f(NULL, a, b, c), x) ? "PASS" : "FAIL"); printf("%s(", #f); fs_value_print(a); printf(", "); fs_value_print(b); printf(", "); fs_value_print(c); printf(") = "); fs_value_print(f(NULL, a, b, c)); printf("\n"); if (!fs_value_equal(f(NULL, a, b, c), x)) { fails++; printf("       should have been "); fs_value_print(x); printf("\n"); } else { passes++; }
#define URI(x) fs_value_uri(x)
#define RID(x) fs_value_rid(x)
#define BND(x) fs_value_rid(0x8000000000000000LL | x)
#define STR(x) fs_value_string(x)
#define PLN(x) fs_value_plain(x)
#define PLN_L(x, l) fs_value_plain_with_lang(x, l)
#define DBL(x) fs_value_double(x)
#define FLT(x) fs_value_float(x)
#define DEC(x) fs_value_decimal(x)
#define INT(x) fs_value_integer(x)
#define BLN(x) fs_value_boolean(x)
#define DAT(x) fs_value_datetime(x)
#define DAT_S(x) fs_value_datetime_from_string(x)
#define ERR() fs_value_error(FS_ERROR_INVALID_TYPE, NULL)
#define BLK() fs_value_blank()

    TEST2(fn_numeric_add, BLK(), BLK(), ERR());
    TEST2(fn_numeric_add, INT(1), URI("test:"), ERR());
    TEST2(fn_numeric_add, STR("2"), INT(3), ERR());
    TEST2(fn_numeric_add, INT(2), INT(3), INT(5));
    TEST2(fn_numeric_add, DEC(2.5), DEC(-1), DEC(1.5));
    TEST2(fn_numeric_add, DEC(-2.5), DEC(-1.5), DEC(-4));
    TEST2(fn_numeric_subtract, INT(2), INT(3), INT(-1));
    TEST2(fn_numeric_add, DBL(1), INT(2), DBL(3));
    TEST2(fn_numeric_add, FLT(17000), INT(2), FLT(17002));
    TEST2(fn_numeric_subtract, FLT(17000), INT(2), FLT(16998));
    TEST2(fn_numeric_subtract, DEC(17000.5), INT(2), DEC(16998.5));
    TEST2(fn_numeric_multiply, URI("http://example.com/"), INT(2), ERR());
    TEST2(fn_numeric_multiply, DEC(3.5), INT(2), DEC(7));
    TEST2(fn_numeric_multiply, DEC(35), DBL(-0.1), DBL(-3.5));
    TEST2(fn_numeric_multiply, INT(1), FLT(23), FLT(23));
    TEST2(fn_numeric_multiply, INT(10), INT(23), INT(230));
    TEST2(fn_numeric_divide, INT(9), URI("http://example.org/"), ERR());
    TEST2(fn_numeric_divide, INT(9), INT(2), DEC(4.5));
    TEST2(fn_numeric_divide, INT(-9), INT(2), DEC(-4.5));
    TEST2(fn_numeric_divide, INT(-9), INT(-2), DEC(4.5));
    TEST2(fn_numeric_divide, DBL(90), FLT(10), DBL(9));
    TEST2(fn_numeric_divide, FLT(99), FLT(-10), FLT(-9.9));
    TEST2(fn_equal, URI("http://example.com/"), DEC(23), BLN(0));
    TEST2(fn_equal, URI("http://example.com/"), URI("http://example.com/"), BLN(1));
    TEST2(fn_equal, INT(23), DEC(23), BLN(1));
    TEST2(fn_equal, fs_value_decimal_from_string("-23.0"), DEC(-23), BLN(1));
    TEST2(fn_equal, STR("foo"), PLN("foo"), BLN(0));
    TEST2(fn_equal, PLN("foo"), PLN("foo"), BLN(1));
    TEST2(fn_equal, BLN(0), BLN(0), BLN(1));
    TEST2(fn_greater_than, STR("BBB"), STR("AAA"), BLN(1));
    TEST2(fn_greater_than, PLN("BBB"), PLN("AAA"), BLN(1));
    TEST2(fn_greater_than, PLN("AAA"), PLN("BBB"), BLN(0));
    TEST2(fn_less_than, PLN("BBB"), PLN("AAA"), BLN(0));
    TEST2(fn_less_than, PLN("AAA"), PLN("BBB"), BLN(1));
    TEST2(fn_less_than, INT(20), INT(15), BLN(0));
    TEST2(fn_numeric_equal, INT(23), INT(23), BLN(1));
    TEST2(fn_numeric_equal, INT(23), DEC(23), BLN(1));
    TEST2(fn_numeric_equal, INT(23), FLT(23), BLN(1));
    TEST2(fn_numeric_equal, INT(23), DBL(23), BLN(1));
    TEST2(fn_numeric_equal, fn_minus(NULL, INT(23)), DBL(-23), BLN(1));
    TEST2(fn_datetime_equal, DAT(1000), DAT(1000), BLN(1));
    TEST2(fn_datetime_equal, DAT(time(NULL)), DAT(1000), BLN(0));
    TEST2(fn_numeric_less_than, INT(0), DBL(23), BLN(1));
    TEST2(fn_numeric_less_than, INT(23), DBL(23), BLN(0));
    TEST2(fn_numeric_less_than, DBL(22.99999), INT(23), BLN(1));
    TEST2(fn_numeric_less_than, DEC(-18.51), DEC(-18.5), BLN(1));
    TEST2(fn_numeric_less_than, DBL(-18.51), DBL(-18.5), BLN(1));
    TEST2(fn_numeric_less_than, DEC(-18.5), DEC(-18.51), BLN(0));
    TEST2(fn_numeric_less_than, DBL(-18.5), DBL(-18.51), BLN(0));
    TEST2(fn_numeric_greater_than, DEC(-121.98882), DEC(-121.739856), BLN(0));
    TEST2(fn_numeric_greater_than, DEC(37.67473), DEC(37.677954), BLN(0));
    TEST2(fn_numeric_greater_than, INT(0), DBL(23), BLN(0));
    TEST2(fn_numeric_greater_than, INT(23), DBL(23), BLN(0));
    TEST2(fn_numeric_greater_than, DBL(22.99999), INT(23), BLN(0));
    TEST2(fn_numeric_greater_than, DEC(-18.51), DEC(-18.5), BLN(0));
    TEST2(fn_numeric_greater_than, DBL(-18.51), DBL(-18.5), BLN(0));
    TEST2(fn_numeric_greater_than, DEC(-18.5), DEC(-18.51), BLN(1));
    TEST2(fn_numeric_greater_than, DBL(-18.5), DBL(-18.51), BLN(1));
    TEST2(fn_logical_and, BLN(1), BLN(0), BLN(0));
    TEST2(fn_logical_and, BLN(1), BLN(1), BLN(1));
    TEST2(fn_logical_and, INT(0), INT(1), BLN(0));
    TEST2(fn_logical_and, INT(1), INT(1), BLN(1));
    TEST2(fn_logical_and, STR("true"), INT(1), BLN(1));
    TEST2(fn_logical_and, STR("false"), INT(1), BLN(1));
    TEST2(fn_logical_and, INT(1), ERR(), ERR());
    TEST2(fn_logical_and, ERR(), INT(1), ERR());
    TEST2(fn_logical_and, INT(0), ERR(), BLN(0));
    TEST2(fn_logical_and, ERR(), INT(0), BLN(0));
    TEST2(fn_logical_and, ERR(), ERR(), ERR());
    TEST2(fn_logical_or, BLN(1), BLN(0), BLN(1));
    TEST2(fn_logical_or, BLN(1), BLN(1), BLN(1));
    TEST2(fn_logical_or, INT(0), INT(1), BLN(1));
    TEST2(fn_logical_or, INT(1), INT(1), BLN(1));
    TEST2(fn_logical_or, STR("true"), INT(32), BLN(1));
    TEST2(fn_logical_or, STR("false"), INT(1), BLN(1));
    TEST2(fn_logical_or, INT(1), ERR(), BLN(1));
    TEST2(fn_logical_or, ERR(), INT(1), BLN(1));
    TEST2(fn_logical_or, INT(0), ERR(), ERR());
    TEST2(fn_logical_or, ERR(), INT(0), ERR());
    TEST2(fn_logical_or, ERR(), ERR(), ERR());
    TEST2(fn_compare, STR("AAA"), STR("BBB"), INT(-1));
    TEST2(fn_compare, STR("BBB"), STR("BBB"), INT(0));
    TEST2(fn_compare, STR("BBB"), STR("AAA"), INT(1));
    TEST2(fn_compare, PLN("BBB"), PLN("AAA"), INT(1));
    TEST2(fn_compare, STR("BBB"), PLN("BBB"), ERR());
    TEST2(fn_compare, STR("http://example.com/"), URI("http://example.com/"), ERR());
    TEST2(fn_compare, URI("http://example.com/"), URI("http://example.com/"), ERR());
    TEST2(fn_compare, INT(1), PLN("BBB"), ERR());
    TEST3(fn_matches, PLN("foobar"), PLN("foo"), BLK(), BLN(1));
    TEST3(fn_matches, PLN("foobar"), PLN("^foo"), BLK(), BLN(1));
    TEST3(fn_matches, PLN("foobar"), PLN("bar$"), BLK(), BLN(1));
    TEST3(fn_matches, PLN("foobar"), PLN("BAR"), STR("i"), BLN(1));
    TEST3(fn_matches, PLN("foobar"), PLN("^FOOB[AO]R$"), STR("i"), BLN(1));
    TEST3(fn_matches, PLN("foobar"), PLN("^bar"), BLK(), BLN(0));
    TEST3(fn_matches, PLN("foobar"), PLN("^foo$"), BLK(), BLN(0));
    TEST3(fn_matches, PLN("foobar"), PLN("foo bar"), PLN("x"), BLN(1));
    TEST3(fn_matches, INT(23), PLN("foo bar"), PLN("x"), ERR());
    TEST3(fn_matches, PLN("foobar"), DAT(1000), PLN("x"), ERR());
    TEST1(fn_bound, URI("http://example.com/"), BLN(1));
    TEST1(fn_bound, STR("http"), BLN(1));
    TEST1(fn_bound, PLN(""), BLN(1));
    TEST1(fn_bound, BND(100), BLN(1));
    TEST1(fn_bound, RID(FS_RID_NULL), BLN(0));
    TEST1(fn_is_blank, URI("http://example.com/"), BLN(0));
    TEST1(fn_is_blank, STR("http"), BLN(0));
    TEST1(fn_is_blank, PLN(""), BLN(0));
    TEST1(fn_is_blank, BND(100), BLN(1));
    TEST1(fn_is_blank, RID(FS_RID_NULL), BLN(0));
    TEST1(fn_is_iri, URI("http://example.com/"), BLN(1));
    TEST1(fn_is_iri, STR("http"), BLN(0));
    TEST1(fn_is_iri, PLN(""), BLN(0));
    TEST1(fn_is_iri, BND(100), BLN(0));
    TEST1(fn_is_iri, RID(FS_RID_NULL), BLN(0));
    TEST1(fn_is_literal, URI("http://example.com/"), BLN(0));
    TEST1(fn_is_literal, STR("http"), BLN(1));
    TEST1(fn_is_literal, PLN(""), BLN(1));
    TEST1(fn_is_literal, BND(100), BLN(0));
    TEST1(fn_is_literal, RID(FS_RID_NULL), BLN(0));
    TEST1(fn_str, URI("http://example.com/"), PLN("http://example.com/"));
    TEST1(fn_str, STR("http"), PLN("http"));
    TEST1(fn_str, PLN(""), PLN(""));
    TEST1(fn_str, BLN(1), PLN("true"));
    TEST1(fn_str, INT(1), PLN("1"));
    TEST1(fn_str, FLT(11.1), PLN("11.100000"));
    TEST1(fn_str, DBL(11.1), PLN("11.100000"));
    TEST1(fn_str, DEC(23), PLN("23.000000"));
    TEST1(fn_str, DAT(1000), PLN("1970-01-01T00:16:40"));
    TEST1(fn_str, BND(100), ERR());
    TEST1(fn_str, RID(FS_RID_NULL), ERR());
    TEST1(fn_lang, PLN("foo"), PLN(""));
    TEST1(fn_lang, STR("foo"), PLN(""));
    TEST1(fn_lang, PLN_L("foo", "en"), PLN("en"));
    TEST1(fn_lang, PLN_L("foo", ""), PLN(""));
    TEST1(fn_lang, BLN(1), PLN(""));
    TEST1(fn_lang, INT(1), PLN(""));
    TEST1(fn_lang, DEC(1.1), PLN(""));
    TEST1(fn_lang, FLT(1.1), PLN(""));
    TEST1(fn_lang, DBL(1.1), PLN(""));
    TEST1(fn_lang, URI("http://example.org/"), ERR());
    TEST1(fn_datatype, PLN("foo"), ERR());
    TEST1(fn_datatype, STR("foo"), URI(XSD_STRING));
    TEST1(fn_datatype, PLN_L("foo", "en"), ERR());
    TEST1(fn_datatype, PLN_L("foo", ""), ERR());
    TEST1(fn_datatype, BLN(1), URI(XSD_BOOLEAN));
    TEST1(fn_datatype, INT(1), URI(XSD_INTEGER));
    TEST1(fn_datatype, DEC(1.1), URI(XSD_DECIMAL));
    TEST1(fn_datatype, FLT(1.1), URI(XSD_FLOAT));
    TEST1(fn_datatype, DBL(1.1), URI(XSD_DOUBLE));
    TEST1(fn_datatype, DAT(1000), URI(XSD_DATETIME));
    TEST1(fn_datatype, URI("http://example.org/"), ERR());
    TEST2(fn_equal, DAT_S("1975-01-07"), DAT(158284800), BLN(1));
    TEST2(fn_equal, DAT_S("1975-01-07T00:00:01"), DAT(158284801), BLN(1));
    TEST2(fn_equal, DAT_S("1975-01-07T01:00:01+0000"), DAT(158288401), BLN(1));
    TEST2(fn_equal, DAT_S("1975-01-07T01:00:01-0900"), DAT(158320801), BLN(1));
    TEST2(fn_cast, DBL(2.1), URI(XSD_INTEGER), INT(2));
    TEST2(fn_cast, PLN("2.23"), URI(XSD_DOUBLE), DBL(2.23));
    TEST2(fn_cast, PLN_L("2.23", "en"), URI(XSD_DOUBLE), DBL(2.23));
    TEST2(fn_cast, STR("2.23"), URI(XSD_DOUBLE), DBL(2.23));
    TEST2(fn_cast, URI("http://example.com/"), URI(XSD_STRING), ERR());
    TEST2(fn_cast, DAT(1000), URI(XSD_STRING), STR("1970-01-01T00:16:40"));
    TEST2(fn_cast, STR("1975-01-07T01:00:01-0900"), URI(XSD_DATETIME), DAT(158320801));

    printf("\n=== pass %d, fail %d\n", passes, fails);

    if (fails) {
	return 1;
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
