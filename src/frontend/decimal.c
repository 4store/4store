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

#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "decimal.h"

static const fs_decimal unit_val = {
    flags: 0,
    digit: { 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 }
};

static const fs_decimal zero_val = {
    flags: 0,
    digit: { 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

static const fs_decimal d1_val = {
    flags: 0,
    digit: { 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

static const fs_decimal d2_val = {
    flags: 0,
    digit: { 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

static const fs_decimal d2_914_val = {
    flags: 0,
    digit: { 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
             9, 1, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
};

static const double conv_factors[4] = { 1.0e-21, 1.0e-11, 0.1, 1.0e9 };

const fs_decimal *fs_decimal_zero = &zero_val;

int fs_decimal_init(fs_decimal *d)
{
    *d = zero_val;

    return 0;
}

static int from_uint64(fs_decimal *d, unsigned long long i, int offset)
{
    int pos = FS_D_OVER_DIGITS + FS_D_INT_DIGITS + offset - 1;
    while (i != 0) {
        if (pos < 0 || pos >= FS_D_DIGITS) {
            d->flags |= FS_D_OVERFLOW;

            return 1;
        }
        d->digit[pos] = i % FS_D_BASE;
        i /= FS_D_BASE;
        pos--;
    }

    return 0;
}

int fs_decimal_init_from_int32(fs_decimal *d, int32_t i)
{
    fs_decimal_init(d);
    unsigned long long val = llabs(i);
    if (i < 0) {
        d->flags |= FS_D_NEGATIVE;
    }

    return from_uint64(d, val, 0);
}

int fs_decimal_init_from_int64(fs_decimal *d, int64_t i)
{
    fs_decimal_init(d);
    unsigned long long val = llabs(i);
    if (i < 0) {
        d->flags |= FS_D_NEGATIVE;
    }

    return from_uint64(d, val, 0);
}

int fs_decimal_init_from_int64_offset(fs_decimal *d, int64_t i, int offset)
{
    fs_decimal_init(d);

    unsigned long long val = llabs(i);
    if (i < 0) {
        d->flags |= FS_D_NEGATIVE;
    }

    return from_uint64(d, val, offset);
}

int fs_decimal_init_from_double(fs_decimal *d, double v)
{
    char tmp[256];

    snprintf(tmp, 255, "%20.20f", v);

    return fs_decimal_init_from_str(d, tmp);
}

int fs_decimal_init_from_str(fs_decimal *d, const char *str)
{
    fs_decimal_init(d);

    int decpos = -1;
    int has_sign = 0;
    int len = strlen(str);

    for (int cpos = 0; str[cpos]; cpos++) {
        if (str[cpos] < '0' || str[cpos] > '9') {
            if (str[cpos] == '-' && cpos == 0) {
                has_sign = 1;
                d->flags = FS_D_NEGATIVE;
            } else if (str[cpos] == '+' && cpos == 0) {
                has_sign = 1;
            } else if (str[cpos] == '.') {
                if (decpos == -1) {
                    decpos = cpos;
                } else {
                    d->flags = FS_D_SYNTAX_ERROR;

                    return 1;
                }
            } else {
                d->flags = FS_D_SYNTAX_ERROR;

                return 1;
            }
        }
    }
    if (decpos == -1) decpos = len;
    int offset = has_sign ? 1 : 0;

    for (int cpos = offset; cpos < decpos; cpos++) {
        int dpos = FS_D_OVER_DIGITS + FS_D_INT_DIGITS - decpos + cpos;
        d->digit[dpos] = str[cpos] - '0';
    }
    for (int cpos = decpos + 1; cpos < len; cpos++) {
        int dpos = FS_D_OVER_DIGITS + FS_D_INT_DIGITS - decpos + cpos - 1;
        d->digit[dpos] = str[cpos] - '0';
    }

    return 0;
}

char *fs_decimal_to_lex(fs_decimal *d)
{
    char *res = calloc(FS_D_DIGITS+5, sizeof(char));
    char *outpos = res;
    int started = 0;
    int error = 0;

    if (d->flags & FS_D_OVERFLOW) {
        sprintf(res, "overflow error");

        return res;
    }
    if (d->flags & FS_D_SYNTAX_ERROR) {
        sprintf(res, "syntax error");

        return res;
    }
    if (d->flags & FS_D_NEGATIVE) {
        *outpos++ = '-';
    }
    for (int i=0; i<FS_D_DIGITS; i++) {
        if (i == FS_D_INT_DIGITS + FS_D_OVER_DIGITS) {
            if (!started) {
                *outpos++ = '0';
                started = 1;
            }
            *outpos++ = '.';
        }
        if (started || d->digit[i]) {
            started = 1;
            *outpos++ = d->digit[i] + '0';
        }
        if (d->digit[i] < 0 || d->digit[i] > 9) {
            error = 1;
        }
    }

    outpos--;
    while (*outpos == '0' || *outpos == '.') {
        if (outpos == res) break;
        if (*outpos == '.') {
            *outpos = '\0';
            break;
        }
        *outpos-- = '\0';
    }

    if (error) {
        strcat(res, " ¡ERR!");
    }

    return res;
}

int fs_decimal_to_double(const fs_decimal *d, double *fp)
{
    *fp = 0.0;
    for (int block=0; block<4; block++) {
        long long bval = 0;
        long long factor = 1;
        for (int i=0; i<10; i++) {
            bval += factor * d->digit[FS_D_DIGITS-(block*10)-i];
            factor *= 10;
        }
        *fp += bval * conv_factors[block];
    }

    if (d->flags & FS_D_NEGATIVE) *fp = -*fp;

    return 0;
}

int fs_decimal_to_int64(const fs_decimal *d, int64_t *in)
{
    *in = 0LL;
    long long factor = 1;
    for (int i=0; i<FS_D_INT_DIGITS; i++) {
        *in += factor * d->digit[FS_D_OVER_DIGITS+FS_D_INT_DIGITS-i-1];
        factor *= 10;
    }

    if (d->flags & FS_D_NEGATIVE) *in = -*in;

    return 0;
}

void fs_decimal_print(const fs_decimal *a, FILE *out)
{
    fprintf(out, "[%04x]", a->flags);
    for (int i=0; i<FS_D_DIGITS; i++) {
        if (i == FS_D_OVERFLOW + FS_D_INT_DIGITS) fprintf(out, ".");
        fprintf(out, "%d", a->digit[i]);
    }
}

void fs_decimal_copy(const fs_decimal *from, fs_decimal *to)
{
    memcpy(to, from, sizeof(fs_decimal));
}

int fs_decimal_is_zero(const fs_decimal *a)
{
    for (int i=0; i<FS_D_DIGITS; i++) {
        if (a->digit[i] != 0) return 0;
    }

    return 1;
}

int fs_decimal_equal(const fs_decimal *a, const fs_decimal *b)
{
    if ((a->flags | FS_D_NEGATIVE) != (b->flags | FS_D_NEGATIVE)) return 0;

    int is_zero = 1;
    for (int i=0; i<FS_D_DIGITS; i++) {
        if (a->digit[i] != b->digit[i]) return 0;
        if (a->digit[i] != 0) is_zero = 0;
    }

    if (is_zero) return 1;

    if ((a->flags & FS_D_NEGATIVE) != (b->flags & FS_D_NEGATIVE)) return 0;

    return 1;
}

/* return true is a < b, or def otherwise */
static int decimal_less_than(const fs_decimal *a, const fs_decimal *b, int def)
{
    if ((a->flags & FS_D_NEGATIVE) != (b->flags & FS_D_NEGATIVE)) {
        if (a->flags & FS_D_NEGATIVE) return 1;

        return 0;
    }

    if ((a->flags & FS_D_NEGATIVE) && (b->flags & FS_D_NEGATIVE)) {
        for (int i=0; i<FS_D_DIGITS; i++) {
            if (a->digit[i] > b->digit[i]) return 1;
            if (a->digit[i] < b->digit[i]) return 0;
        }
        return def;
    }

    for (int i=0; i<FS_D_DIGITS; i++) {
        if (a->digit[i] < b->digit[i]) return 1;
        if (a->digit[i] > b->digit[i]) return 0;
    }

    return def;
}

static int decimal_greater_than(const fs_decimal *a, const fs_decimal *b, int def)
{
    if ((a->flags & FS_D_NEGATIVE) != (b->flags & FS_D_NEGATIVE)) {
        if (a->flags & FS_D_NEGATIVE) return 0;

        return 1;
    }

    if ((a->flags & FS_D_NEGATIVE) && (b->flags & FS_D_NEGATIVE)) {
        for (int i=0; i<FS_D_DIGITS; i++) {
            if (a->digit[i] > b->digit[i]) return 0;
            if (a->digit[i] < b->digit[i]) return 1;
        }
        return def;
    }

    for (int i=0; i<FS_D_DIGITS; i++) {
        if (a->digit[i] < b->digit[i]) return 0;
        if (a->digit[i] > b->digit[i]) return 1;
    }

    return def;
}

int fs_decimal_less_than(const fs_decimal *a, const fs_decimal *b)
{
    return decimal_less_than(a, b, 0);
}

int fs_decimal_less_than_equal(const fs_decimal *a, const fs_decimal *b)
{
    return decimal_less_than(a, b, 1);
}

int fs_decimal_greater_than(const fs_decimal *a, const fs_decimal *b)
{
    return decimal_greater_than(a, b, 0);
}

int fs_decimal_greater_than_equal(const fs_decimal *a, const fs_decimal *b)
{
    return decimal_greater_than(a, b, 1);
}

static void add_simple(const fs_decimal *a, const fs_decimal *b, fs_decimal *r)
{
    fs_decimal_init(r);

    for (int i = FS_D_DIGITS-1; i > 0; i--) {
        r->digit[i] += a->digit[i] + b->digit[i];
        if (r->digit[i] > 9) {
            r->digit[i-1] += r->digit[i] / 10;
            r->digit[i] = r->digit[i] % 10;
        }
    }
}

static int fs_decimal_radix_complement(const fs_decimal *a, fs_decimal *r)
{
    fs_decimal intl = zero_val;

    for (int i=0; i<FS_D_DIGITS; i++) {
        intl.digit[i] = 9 - a->digit[i];
    }
    add_simple(&intl, &unit_val, r);

    return 0;
}

int fs_decimal_negate(const fs_decimal *a, fs_decimal *r)
{
    fs_decimal_copy(a, r);
    r->flags ^= FS_D_NEGATIVE;

    return 0;
}

int fs_decimal_add(const fs_decimal *ain, const fs_decimal *bin, fs_decimal *r)
{
    fs_decimal av, bv, *a, *b;
    fs_decimal tmp = zero_val;

    if (ain->flags & FS_D_OVERFLOW || bin->flags & FS_D_OVERFLOW) {
        fs_decimal_init(r);
        r->flags = FS_D_OVERFLOW;

        return 1;
    }

    if (ain->flags & FS_D_NEGATIVE) {
        a = &av;
        fs_decimal_radix_complement(ain, &av);
    } else {
        a = (fs_decimal *)ain;
    }
    if (bin->flags & FS_D_NEGATIVE) {
        b = &bv;
        fs_decimal_radix_complement(bin, &bv);
    } else {
        b = (fs_decimal *)bin;
    }

    for (int i = FS_D_DIGITS-1; i > 0; i--) {
        tmp.digit[i] += a->digit[i] + b->digit[i];
        if (tmp.digit[i] > 9) {
            tmp.digit[i-1] += tmp.digit[i] / 10;
            tmp.digit[i] = tmp.digit[i] % 10;
        }
    }

    if (tmp.digit[0] == 1) {
        tmp.digit[0] = 0;
    }
    if (tmp.digit[1] == 9) {
        fs_decimal_radix_complement(&tmp, &tmp);
        tmp.flags |= FS_D_NEGATIVE;
    }
    *r = tmp;

    return 0;
}

int fs_decimal_subtract(const fs_decimal *a, const fs_decimal *b, fs_decimal *r)
{
    fs_decimal intl;

    fs_decimal_negate(b, &intl);

    return fs_decimal_add(a, &intl, r);
}

static void mul_internal(const fs_decimal *a, int mul, int offset, fs_decimal *r)
{
    for (int i=FS_D_DIGITS-1; i >= 0; i--) {
        const int digit = i-offset+FS_D_FRAC_DIGITS-1;
        if (digit >= 0 && digit < FS_D_DIGITS) {
            r->digit[digit] = a->digit[i] * mul;
        }
    }
}

int fs_decimal_multiply(const fs_decimal *a, const fs_decimal *b, fs_decimal *r)
{
    fs_decimal sum = zero_val;

    if (a->flags & FS_D_OVERFLOW || b->flags & FS_D_OVERFLOW) {
        fs_decimal_init(r);
        r->flags = FS_D_OVERFLOW;

        return 1;
    }

    for (int i=0; i < FS_D_DIGITS; i++) {
        if (b->digit[FS_D_DIGITS-i-FS_D_OVER_DIGITS]) {
            fs_decimal tmp = zero_val;
            mul_internal(a, b->digit[FS_D_DIGITS-i-FS_D_OVER_DIGITS], i, &tmp);
            if (tmp.digit[0] || tmp.digit[1]) {
                sum.flags |= FS_D_OVERFLOW;
            }
            fs_decimal_add(&sum, &tmp, &sum);
        }
    }

    sum.flags |= (a->flags & FS_D_NEGATIVE) ^ (b->flags & FS_D_NEGATIVE);
    *r = sum;

    return 0;
}

/* shift the decimal a by "places" decimal places, +ve direction is reduces
 * power, -ve direction increases */

static int decimal_shift(const fs_decimal *a, fs_decimal *r, int places)
{
    fs_decimal intl = zero_val;

    for (int i=places; i < FS_D_DIGITS; i++) {
        if (i >= 0 && i - places < FS_D_DIGITS) {
            intl.digit[i] = a->digit[i - places];
        }
    }

    *r = intl;

    return 0;
}

/* normalise the decimal into the range (0,1] */

static int fs_decimal_normalise(const fs_decimal *a, fs_decimal *r, int *shift)
{
    for (int i=0; i<FS_D_DIGITS; i++) {
        if (a->digit[i]) {
            *shift = FS_D_OVER_DIGITS + FS_D_INT_DIGITS - i;
            break;
        }
    }

    decimal_shift(a, r, *shift);

    return 0;
}

int fs_decimal_divide(const fs_decimal *n, const fs_decimal *d, fs_decimal *q)
{
    fs_decimal norm;
    int shift = 0;

    /* catch divide by zero error */
    if (fs_decimal_is_zero(d)) {
        return 1;
    }

    /* use Newton-Raphson series approximation to calculate 1/d */
    fs_decimal_normalise(d, &norm, &shift);

    fs_decimal x;
    if (norm.digit[FS_D_OVER_DIGITS + FS_D_INT_DIGITS] >= 5) {
        /* for 0.5 < norm < 1.0 we can use x = 2.914 - 2d as starting pt */
        fs_decimal twod;
        fs_decimal_multiply(&d2_val, &norm, &twod);
        fs_decimal_subtract(&d2_914_val, &twod, &x);
    } else {
        /* otherwise, don't know where to start, use 1.0 */
        x = d1_val;
    }

    fs_decimal last = zero_val;

    /* if it hasn't converged after 30 iterations it usually doesn't */
    for (int i=0; i<30; i++) {
#if 0
        printf("step %2d = ", i);
        fs_decimal_print(&x, stdout);
        printf("\n");
#endif
        /* calculate x = x(2-dx) */
        fs_decimal dx, tmp;
        fs_decimal_multiply(&norm, &x, &dx);
        fs_decimal_subtract(&d2_val, &dx, &tmp);
        fs_decimal_multiply(&tmp, &x, &x);
        if (fs_decimal_equal(&x, &last)) break;
        last = x;
    }
    /* round up to nearest representable number */
    fs_decimal_add(&x, &unit_val, &x);

#if 0
    printf("step  N = ");
    fs_decimal_print(&x, stdout);
    printf("\n");
#endif

    /* shift the aproximate reciprocal back to correct power */
    decimal_shift(&x, &x, shift);

    /* q = n * 1/d */
    fs_decimal_multiply(n, &x, q);
    q->flags ^= (d->flags & FS_D_NEGATIVE);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
