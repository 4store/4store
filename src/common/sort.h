#ifndef SORT_H
#define SORT_H

typedef int (*fs_compar_d_fn_t) (__const void *, __const void *, void *);

/* quicksort function, code is from glibc's qsort_r, as not all versions of
 * glibc make this available to user code */
void fs_qsort_r(void *b, size_t n, size_t s, fs_compar_d_fn_t cmp, void *arg);

#endif
