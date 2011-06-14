#ifndef DECIMAL_H
#define DECIMAL_H

#include "inttypes.h"

#define FS_D_BASE 10
#define FS_D_OVER_DIGITS 2
#define FS_D_INT_DIGITS 20
#define FS_D_FRAC_DIGITS 20
#define FS_D_DIGITS (FS_D_INT_DIGITS+FS_D_FRAC_DIGITS+FS_D_OVER_DIGITS)

#define FS_D_NEGATIVE      0x1
#define FS_D_OVERFLOW      0x2
#define FS_D_SYNTAX_ERROR  0x4

typedef struct _fs_decimal {
    int flags;
    signed char digit[FS_D_DIGITS];
} fs_decimal;

extern const fs_decimal *fs_decimal_zero;

/* write the 32 bit integer value into the decimal */
int fs_decimal_init_from_int32(fs_decimal *d, int32_t i);

/* write the 64 bit integer value into the decimal */
int fs_decimal_init_from_int64(fs_decimal *d, int64_t i);

/* write the 64 bit integer value into the decimal, offset by some decimal
 * places, eg 1234, 2 will write the value 12.34 into the decimal */
int fs_decimal_init_from_int64_offset(fs_decimal *d, int64_t i, int offset);

/* write the nearst representable value of the double v into d */
int fs_decimal_init_from_double(fs_decimal *d, double v);

/* convert base 10 lexical form into decimal */
int fs_decimal_init_from_str(fs_decimal *d, const char *str);

/* produce a normalised lexical represenation of the decimal */
char *fs_decimal_to_lex(fs_decimal *d);

/* convert decimal value into nearest double represenation, non 0 on failure */
int fs_decimal_to_double(const fs_decimal *d, double *fp);

/* convert decimal value into nearest int represenation, non 0 on failure */
int fs_decimal_to_int64(const fs_decimal *d, int64_t *in);

/* copy one decimal to another */
void fs_decimal_copy(const fs_decimal *from, fs_decimal *to);


/* comparison functions */

/* return true if the two decimals compare equal */
int fs_decimal_equal(const fs_decimal *a, const fs_decimal *b);

int fs_decimal_less_than(const fs_decimal *a, const fs_decimal *b);
int fs_decimal_less_than_equal(const fs_decimal *a, const fs_decimal *b);

int fs_decimal_greater_than(const fs_decimal *a, const fs_decimal *b);
int fs_decimal_greater_than_equal(const fs_decimal *a, const fs_decimal *b);

/* maths functions */

/* r = -a */
int fs_decimal_negate(const fs_decimal *a, fs_decimal *r);

/* r = a + b */
int fs_decimal_add(const fs_decimal *a, const fs_decimal *b, fs_decimal *r);

/* r = a - b */
int fs_decimal_subtract(const fs_decimal *a, const fs_decimal *b, fs_decimal *r);

/* r = a * b */
int fs_decimal_multiply(const fs_decimal *a, const fs_decimal *b, fs_decimal *r);

/* q = n / d */
int fs_decimal_divide(const fs_decimal *n, const fs_decimal *d, fs_decimal *q);

#endif
