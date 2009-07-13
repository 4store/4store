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
 *  Copyright (C) 2007 Steve Harris for Garlik
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "decimal.h"

#define TRUE  1
#define FALSE 0 

#define TEST_LEX_I32(i) fs_decimal_init_from_int32(&d, i); lex = fs_decimal_to_lex(&d); sprintf(tmp, "%d", i); printf("[%s] %s -> '%s'\n", strcmp(tmp, lex) ? "FAIL" : "PASS", tmp, lex); free(lex)
#define TEST_LEX_I64(i) fs_decimal_init_from_int64(&d, i); lex = fs_decimal_to_lex(&d); sprintf(tmp, "%lld", (long long int)i); printf("[%s] %s -> '%s'\n", strcmp(tmp, lex) ? "FAIL" : "PASS", tmp, lex); free(lex)
#define TEST_LEX_I64_O(i, o, l) fs_decimal_init_from_int64_offset(&d, i, o); lex = fs_decimal_to_lex(&d); printf("[%s] %s -> '%s'\n", strcmp(l, lex) ? "FAIL" : "PASS", l, lex)
#define TEST_LEX_STR(s) fs_decimal_init_from_str(&d, s); lex = fs_decimal_to_lex(&d); printf("[%s] %s -> '%s'\n", strcmp(s, lex) ? "FAIL" : "PASS", s, lex)
#define TEST_LEX_DBL(i, l) fs_decimal_init_from_double(&d, i); lex = fs_decimal_to_lex(&d); printf("[%s] %s -> '%s'\n", strncmp(lex, l, strlen(#i)) ? "FAIL" : "PASS", #i, lex); free(lex)
#define TEST_DBL_CONV(s, v) fs_decimal_init_from_str(&d, s); fs_decimal_to_double(&d, &fp); printf("[%s] %s double -> %f\n", fp == v ? "PASS" : "FAIL", s, fp)
#define TEST_INT_CONV(s, v) fs_decimal_init_from_str(&d, s); fs_decimal_to_int64(&d, &in); printf("[%s] %s int -> %lld\n", in == v ? "PASS" : "FAIL", s, (long long int)in)
#define TEST_BOOL(f, as, bs, v) fs_decimal_init_from_str(&a, as); fs_decimal_init_from_str(&b, bs); printf("[%s] %s(%s, %s) = %s\n", fs_decimal_ ## f(&a, &b) == v ? "PASS" : "FAIL", #f, as, bs, fs_decimal_ ## f(&a, &b) ? "TRUE" : "FALSE")
#define TEST_FUNC(f, as, bs, rs) fs_decimal_init_from_str(&a, as); fs_decimal_init_from_str(&b, bs); fs_decimal_init_from_str(&r, rs); fs_decimal_ ## f(&a, &b, &d); lex = fs_decimal_to_lex(&d); printf("[%s] %s(%s, %s) = %s\n", !strcmp(lex, rs) ? "PASS" : "FAIL", #f, as, bs, lex); free(lex)

int main(int argc, char *argv[])
{
    char tmp[256];
    fs_decimal d, a, b, r;
    double fp;
    int64_t in;
    char *lex;

    printf("sizeof(fs_decimal) = %zd\n\n", sizeof(fs_decimal));
    TEST_LEX_I32(0);
    TEST_LEX_I32(1);
    TEST_LEX_I32(-1);
    TEST_LEX_I32(1234);
    TEST_LEX_I32(1234567890);
    TEST_LEX_I32(-23);
    TEST_LEX_I32(-99999999);
    TEST_LEX_I32(INT32_MAX);
    TEST_LEX_I32(INT32_MIN);
    TEST_LEX_I64(2147483648LL);
    TEST_LEX_I64(-2147483648LL);
    TEST_LEX_I64((long long)INT64_MAX);
    TEST_LEX_I64((long long)INT64_MIN);
    TEST_LEX_I64_O(123456LL, 3, "123.456");
    TEST_LEX_I64_O(1000123456LL, 6, "1000.123456");
    TEST_LEX_I64_O(1LL, 18, "0.000000000000000001");
    TEST_LEX_I64_O(23LL, -10, "230000000000");
    TEST_LEX_I64_O(23LL, -100, "overflow error");
    TEST_LEX_STR("0");
    TEST_LEX_STR("1");
    TEST_LEX_STR("23");
    TEST_LEX_STR("23.9");
    TEST_LEX_STR("23.0000000000001");
    TEST_LEX_STR("-1");
    TEST_LEX_STR("-99");
    TEST_LEX_STR("0.000000023");
    TEST_LEX_STR("-99.000000023");
    TEST_LEX_STR("-12345678999.00000002300001");
    TEST_LEX_STR("99223372036854775807.1234567890123456789");
    TEST_LEX_DBL(0.0, "0");
    TEST_LEX_DBL(10.0, "10");
    TEST_LEX_DBL(25.6, "25.6");
    TEST_LEX_DBL(0.00000001, "0.00000001");
    TEST_LEX_DBL(-25.6, "-25.6");
    TEST_DBL_CONV("0", 0.0);
    TEST_DBL_CONV("-1", -1.0);
    TEST_DBL_CONV("1000", 1000.0);
    TEST_DBL_CONV("23.0001", 23.0001);
    TEST_DBL_CONV("23.000000000000000001", 23.0);
    TEST_INT_CONV("0", 0.0);
    TEST_INT_CONV("-1", -1);
    TEST_INT_CONV("1000", 1000);
    TEST_INT_CONV("23.0001", 23);
    TEST_INT_CONV("23.000000000000000001", 23.0);
    TEST_INT_CONV("6989651668307017727", 6989651668307017727LL);
    TEST_INT_CONV("-99999999", -99999999);
    TEST_BOOL(equal, "0", "0", TRUE);
    TEST_BOOL(equal, "1", "1", TRUE);
    TEST_BOOL(equal, "-1", "-1", TRUE);
    TEST_BOOL(equal, "1", "-1", FALSE);
    TEST_BOOL(equal, "10", "10", TRUE);
    TEST_BOOL(equal, "23.00000000000000001", "23.00000000000000001", TRUE);
    TEST_BOOL(equal, "23.000000000000000012", "23.000000000000000013", FALSE);
    TEST_BOOL(equal, "-0", "0", TRUE);
    TEST_BOOL(equal, "9223372036854775807", "9223372036854775807", TRUE);
    TEST_BOOL(equal, "error", "error", TRUE);
    TEST_BOOL(less_than, "0", "0", FALSE);
    TEST_BOOL(less_than, "1", "0", FALSE);
    TEST_BOOL(less_than, "0", "1", TRUE);
    TEST_BOOL(less_than, "-1", "1", TRUE);
    TEST_BOOL(less_than, "-10", "1", TRUE);
    TEST_BOOL(less_than, "13.0000000000000001", "13.0000000000000002", TRUE);
    TEST_BOOL(less_than, "99223372036854775807.0000000000000001", "99223372036854775807.0000000000000002", TRUE);
    TEST_BOOL(less_than, "99223372036854775807.0000000000000002", "99223372036854775807.0000000000000001", FALSE);
    TEST_BOOL(less_than_equal, "0", "0", TRUE);
    TEST_BOOL(less_than_equal, "1", "0", FALSE);
    TEST_BOOL(less_than_equal, "0", "1", TRUE);
    TEST_BOOL(less_than_equal, "-1", "1", TRUE);
    TEST_BOOL(less_than_equal, "-10", "1", TRUE);
    TEST_BOOL(less_than_equal, "13.0000000000000001", "13.0000000000000002", TRUE);
    TEST_BOOL(less_than_equal, "99223372036854775807.0000000000000001", "99223372036854775807.0000000000000002", TRUE);
    TEST_BOOL(less_than_equal, "99223372036854775807.0000000000000002", "99223372036854775807.0000000000000001", FALSE);
    TEST_BOOL(greater_than, "0", "0", FALSE);
    TEST_BOOL(greater_than, "1", "0", TRUE);
    TEST_BOOL(greater_than, "0", "1", FALSE);
    TEST_BOOL(greater_than, "-1", "1", FALSE);
    TEST_BOOL(greater_than, "-10", "1", FALSE);
    TEST_BOOL(greater_than, "13.0000000000000001", "13.0000000000000002", FALSE);
    TEST_BOOL(greater_than, "13.0000000000000002", "13.0000000000000001", TRUE);
    TEST_BOOL(greater_than_equal, "0", "0", TRUE);
    TEST_BOOL(greater_than_equal, "1", "0", TRUE);
    TEST_BOOL(greater_than_equal, "0", "1", FALSE);
    TEST_BOOL(greater_than_equal, "-1", "1", FALSE);
    TEST_BOOL(greater_than_equal, "-10", "1", FALSE);
    TEST_BOOL(greater_than_equal, "13.0000000000000001", "13.0000000000000002", FALSE);
    TEST_BOOL(greater_than_equal, "13.0000000000000002", "13.0000000000000001", TRUE);
    TEST_FUNC(add, "0", "0", "0");
    TEST_FUNC(add, "0", "1", "1");
    TEST_FUNC(add, "9999", "1", "10000");
    TEST_FUNC(add, "4432", "54354326342", "54354330774");
    TEST_FUNC(add, "100", "-1", "99");
    TEST_FUNC(add, "-1", "-1", "-2");
    TEST_FUNC(add, "-9999.001", "9999", "-0.001");
    TEST_FUNC(add, "9999", "-1", "9998");
    TEST_FUNC(add, "48032", "-391", "47641");
    TEST_FUNC(add, "185", "-329", "-144");
    TEST_FUNC(add, "-100", "1", "-99");
    TEST_FUNC(add, "1", "-100", "-99");
    TEST_FUNC(add, "99223372036854775807.0000000000000002", "-89223372036854775807.0000000000000001", "10000000000000000000.0000000000000001");
    TEST_FUNC(subtract, "0", "0", "0");
    TEST_FUNC(subtract, "0", "23", "-23");
    TEST_FUNC(subtract, "23", "23", "0");
    TEST_FUNC(subtract, "23", "24", "-1");
    TEST_FUNC(subtract, "1000", "-100", "1100");
    TEST_FUNC(multiply, "1", "1", "1");
    TEST_FUNC(multiply, "99", "99", "9801");
    TEST_FUNC(multiply, "23.00000023", "98.76", "2271.4800227148");
    TEST_FUNC(multiply, "9922337203685477.5807", "197.34", "1958074023775292145.775338");
    TEST_FUNC(multiply, "197.34", "9922337203685477.5807", "1958074023775292145.775338");
    TEST_FUNC(multiply, "9999999999999999.9999", "9999999999999999.9999", "overflow error");
    TEST_FUNC(multiply, "9000000000000000.9999", "0.000002", "18000000000.0000019998");
    TEST_FUNC(multiply, "-1", "-1", "1");
    TEST_FUNC(multiply, "-1", "10", "-10");
    TEST_FUNC(multiply, "-23.00000023", "99.99", "-2299.7700229977");
    TEST_FUNC(divide, "1", "1", "1");
    TEST_FUNC(divide, "1", "123", "0.0081300813008130081");
    TEST_FUNC(divide, "23", "1000", "0.023");
    /* these ones are not actually right, but damn close */
    TEST_FUNC(divide, "-23.00000023", "99.99", "-0.2300230046004600437");
    TEST_FUNC(divide, "69.001", "-690", "-0.10000144927536231334");
    TEST_FUNC(divide, "223372036854775807", "6.5", "34364928746888585.6802799672462813027");

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
