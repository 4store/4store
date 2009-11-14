#ifndef TBCHAIN_H
#define TBCHAIN_H

#include "tbchain.h"
#include "backend.h"
#include "common/datatypes.h"

typedef struct _fs_tbchain fs_tbchain; 
typedef struct _fs_tbchain_it fs_tbchain_it; 

fs_tbchain *fs_tbchain_open(fs_backend *be, const char *label, int flags);
fs_tbchain *fs_tbchain_open_filename(const char *fname, int flags);
int fs_tbchain_sync(fs_tbchain *bc);
int fs_tbchain_close(fs_tbchain *bc);
int fs_tbchain_unlink(fs_tbchain *bc);

/* create a new triple block chain */
fs_index_node fs_tbchain_new_chain(fs_tbchain *bc);
/* remove an existing triple block chain, and free the blocks */
int fs_tbchain_remove_chain(fs_tbchain *bc, fs_index_node b);

/* return the length of the chain in triples */
fs_index_node fs_tbchain_length(fs_tbchain *bc, fs_index_node b);
/* addpend a triple to the chain, creating blocks is neccesary */
fs_index_node fs_tbchain_add_triple(fs_tbchain *bc, fs_index_node b, fs_rid triple[3]) __attribute__ ((warn_unused_result));

/* functions to set/clear the "sparse" flag on a chain, indicating that not all
 * the triples included still exist in the graph */
int fs_tbchain_set_sparse(fs_tbchain *bc, fs_index_node b);
int fs_tbchain_clear_sparse(fs_tbchain *bc, fs_index_node b);
int fs_tbchain_get_sparse(fs_tbchain *bc, fs_index_node b);

/* number of blocks allocated internally */
unsigned int fs_tbchain_allocated_blocks(fs_tbchain *bc);
/* show structure of chain file */
void fs_tbchain_print(fs_tbchain *bc, FILE *out, int verbosity);
/* get some statistics from chain, printing results to out */
int fs_tbchain_get_stats(fs_tbchain *bc, fs_index_node chain, FILE *out);
/* run a constency check against chain, printing results to out */
int fs_tbchain_check_consistency(fs_tbchain *bc, fs_rid model, fs_index_node chain, FILE *out);
/* check for leaks, must be run after every chain has been checked for
 * consistency */
int fs_tbchain_check_leaks(fs_tbchain *bc, FILE *out);

/* iterator functions */

/* create an interator, return NULL on fail */
fs_tbchain_it *fs_tbchain_new_iterator(fs_tbchain *bc, fs_index_node chain);
/* get the first/next triple from the chain, returns TRUE on OK */
int fs_tbchain_it_next(fs_tbchain_it *it, fs_rid triple[3]);
/* free the iterator, returns 0 on success */
int fs_tbchain_it_free(fs_tbchain_it *it);

#endif
