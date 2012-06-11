#ifndef BITARR_H
#define BITARR_H

/* utility functions to handle bit array */
unsigned char *fs_new_bit_array(long n);
void fs_bit_array_set(unsigned char *p,long i,int value);
short fs_bit_array_get(unsigned char *p,long i);
void fs_bit_array_destroy(unsigned char *p);

#endif
