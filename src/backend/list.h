#ifndef LIST_H
#define LIST_H

#include "backend.h"

struct _fs_list;
typedef struct _fs_list fs_list;

fs_list *fs_list_open(fs_backend *be, const char *label, size_t width, int flags);

fs_list *fs_list_open_filename(const char *filename, size_t width, int flags);

int fs_list_flush(fs_list *l);

int fs_list_truncate(fs_list *l);

int fs_list_unlink(fs_list *l);

int fs_list_close(fs_list *l);

int fs_list_lock(fs_list *l, int action);

int32_t fs_list_add(fs_list *l, const void *data);

void fs_list_rewind(fs_list *l);
int fs_list_next_value(fs_list *l, void *out);

int fs_list_get(fs_list *l, int32_t pos, void *data);

int fs_list_length(fs_list *l);

void fs_list_print(fs_list *l, FILE *out, int verbosity);

/* vi:set expandtab sts=4 sw=4: */

#endif
