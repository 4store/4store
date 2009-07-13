#ifndef RHASH_H
#define RHASH_H

#include "backend.h"

typedef struct _fs_rhash fs_rhash;

fs_rhash *fs_rhash_open(fs_backend *be, const char *label, int flags);
fs_rhash *fs_rhash_open_filename(const char *filename, int flags);
int fs_rhash_flush(fs_rhash *rh);
int fs_rhash_close(fs_rhash *rh);

int fs_rhash_get(fs_rhash *rh, fs_resource *res);
int fs_rhash_put(fs_rhash *rh, fs_resource *res);

int fs_rhash_get_multi(fs_rhash *rh, fs_resource *res, int count);
int fs_rhash_put_multi(fs_rhash *rh, fs_resource *res, int count);

void fs_rhash_print(fs_rhash *rh, FILE *out, int verbosity);

/* return number of unique resources stored */
int fs_rhash_count(fs_rhash *rh);

#endif
