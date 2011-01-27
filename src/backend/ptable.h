#ifndef PTABLE_H
#define PTABLE_H

#include "backend.h"

typedef struct _fs_ptable fs_ptable;
typedef uint32_t fs_row_id;

/* basic file operations */
fs_ptable *fs_ptable_open(fs_backend *be, const char *label, int flags);
fs_ptable *fs_ptable_open_filename(const char *fname, int flags);
int fs_ptable_close(fs_ptable *pt);
int fs_ptable_unlink(fs_ptable *pt);

/* force data mapped by ptable out to disk, returns non 0 on failure */
int fs_ptable_sync(fs_ptable *pt);

/* dump a representation of the ptable to out */
void fs_ptable_print(fs_ptable *pt, FILE *out, int verbosity);
int fs_ptable_check_consistency(fs_ptable *pt, FILE *out, fs_row_id src, fs_row_id start, int *length);
int fs_ptable_check_leaks(fs_ptable *pt, FILE *out);

/* allocate a new row and return its ID, returns 0 on failure */
fs_row_id fs_ptable_new_row(fs_ptable *pt);

/* remove an entire chain from the table */
int fs_ptable_remove_chain(fs_ptable *pt, fs_row_id b);

/* add a pair of fs_rid data items to an existing chain ID, return the new
 * chain ID. If b is 0 then a new chain will be created */
fs_row_id fs_ptable_add_pair(fs_ptable *pt, fs_row_id b, fs_rid pair[2]);

/* fetch the contents of row b from the table */
int fs_ptable_get_row(fs_ptable *pt, fs_row_id b, fs_rid pair[2]);

/* return true if the pair exists in the chain */
int fs_ptable_pair_exists(fs_ptable *pt, fs_row_id b, fs_rid pair[2]);

/* remove all pairs matching pair[], fill models out with any models modified */
fs_row_id fs_ptable_remove_pair(fs_ptable *pt, fs_row_id b, fs_rid pair[2], int *removed, fs_rid_set *models);

/* move a row onto the free list - caller is responsible for cleaning up the
 * links */
int fs_ptable_free_row(fs_ptable *pt, fs_row_id b);

/* return the length of a chain in rows, stop counting at max, unless max is 0 */
unsigned int fs_ptable_chain_length(fs_ptable *pt, fs_row_id b, unsigned int max);

/* return the length of table in rows */
fs_row_id fs_ptable_length(fs_ptable *pt);
/* return the length of free list in rows */
uint32_t fs_ptable_free_length(fs_ptable *pt);

/* return the next row in the chain, or 0 is there is none */
fs_row_id fs_ptable_get_next(fs_ptable *pt, fs_row_id r);

#endif
