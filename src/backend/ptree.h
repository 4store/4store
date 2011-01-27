#ifndef PTREE_H
#define PTREE_H

#include "backend.h"
#include "ptable.h"

typedef struct _fs_ptree fs_ptree;
typedef struct _fs_ptree_it fs_ptree_it;
typedef uint32_t fs_ptree_leafid;

fs_ptree *fs_ptree_open(fs_backend *be, fs_rid pred, char pk, int flags, fs_ptable *chain);

fs_ptree *fs_ptree_open_filename(const char *filename, int flags, fs_ptable *chain);

int fs_ptree_write_header(fs_ptree *pt);

int fs_ptree_add(fs_ptree *pt, fs_rid pk, fs_rid pair[2], int force);
int fs_ptree_remove(fs_ptree *pt, fs_rid pk, fs_rid pair[2], fs_rid_set *models);
int fs_ptree_remove_all(fs_ptree *pt, fs_rid pair[2]);

fs_ptree_it *fs_ptree_search(fs_ptree *pt, fs_rid pk, fs_rid pair[2]);
int fs_ptree_it_get_length(fs_ptree_it *it);
int fs_ptree_it_next(fs_ptree_it *it, fs_rid pair[2]);
int fs_ptree_it_next_quad(fs_ptree_it *it, fs_rid quad[4]);
fs_ptree_it *fs_ptree_traverse(fs_ptree *pt, fs_rid mrid);
int fs_ptree_traverse_next(fs_ptree_it *it, fs_rid quad[4]);
void fs_ptree_it_free(fs_ptree_it *it);

void fs_ptree_print(fs_ptree *pt, FILE *out, int verbosity);

/* unlink backend storage file */
int fs_ptree_unlink(fs_ptree *pt);

/* close filehandle and free memory used */
int fs_ptree_close(fs_ptree *pt);

int fs_ptree_count(fs_ptree *pt);

/* vi:set expandtab sts=4 sw=4: */

#endif
