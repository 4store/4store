#ifndef TLIST_H
#define TLIST_H

#include "backend.h"

struct _fs_tlist;
typedef struct _fs_tlist fs_tlist;

fs_tlist *fs_tlist_open(fs_backend *be, fs_rid model, int flags);

fs_tlist *fs_tlist_open_filename(const char *filename, int flags);

int fs_tlist_flush(fs_tlist *l);

int fs_tlist_truncate(fs_tlist *l);

int fs_tlist_unlink(fs_tlist *l);

int fs_tlist_close(fs_tlist *l);

int fs_tlist_lock(fs_tlist *l, int action);

int64_t fs_tlist_add(fs_tlist *l, fs_rid data[4]);

void fs_tlist_rewind(fs_tlist *l);
int fs_tlist_next_value(fs_tlist *l, void *out);

int fs_tlist_length(fs_tlist *l);

void fs_tlist_print(fs_tlist *l, FILE *out, int verbosity);

/* vi:set expandtab sts=4 sw=4: */

#endif
