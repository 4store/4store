#ifndef IMPORT_H
#define IMPORT_H

#include "common/4store.h"

#define FS_DRYRUN_DELETE    0x01
#define FS_DRYRUN_RESOURCES 0x02
#define FS_DRYRUN_QUADS     0x04

int fs_import(fsp_link *link, const char *model_uri, char *resource_uri,
	      const char *format, int verbosity, int dryrun, int has_o_index,
	      FILE *msg, int *count);

int fs_import_commit(fsp_link *link, int verbosity, int dryrun, int has_o_index, FILE *msg, int *count);

int fs_import_stream_start(fsp_link *link, const char *model_uri, const char *mimetype, int has_o_index, int *count);
int fs_import_stream_data(fsp_link *link, unsigned char *data, size_t count);
int fs_import_stream_finish(fsp_link *link, int *count, int *errors);
void fs_import_reread_config();

#endif
