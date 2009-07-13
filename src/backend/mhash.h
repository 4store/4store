#ifndef MHASH_H
#define MHASH_H

#include "backend.h"
#include "tbchain.h"

typedef struct _fs_mhash fs_mhash;

fs_mhash *fs_mhash_open(fs_backend *be, const char *label, int flags);
fs_mhash *fs_mhash_open_filename(const char *filename, int flags);
int fs_mhash_close(fs_mhash *rh);

int fs_mhash_get(fs_mhash *mh, const fs_rid rid, fs_index_node *val);
int fs_mhash_put(fs_mhash *mh, const fs_rid rid, fs_index_node val);

/* return number of unique models stored */
int fs_mhash_count(fs_mhash *rh);

/* return a vector of all they keys where the value is non 0 */
fs_rid_vector *fs_mhash_get_keys(fs_mhash *mh);

/* write any outstanding data (only headers at present) out to disk buffers */
int fs_mhash_flush(fs_mhash *mh);

void fs_mhash_print(fs_mhash *mh, FILE *out, int verbosity);
void fs_mhash_check_chain(fs_mhash *mh, fs_tbchain *tbc, FILE *out, int verbosity);

#endif
