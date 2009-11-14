#ifndef IMPORT_BACKEND_H
#define IMPORT_BACKEND_H

#include "common/datatypes.h"

int fs_res_import(fs_backend *be, int seg, long count, fs_resource buffer[]);

int fs_res_import_commit(fs_backend *be, int seg, int account);

int fs_quad_import(fs_backend *be, int seg, int flags, int count, fs_rid buffer[][4]);

int fs_quad_import_commit(fs_backend *be, int seg, int flags, int account);

int fs_delete_models(fs_backend *be, int seg, fs_rid_vector *mvec);

int fs_delete_quads(fs_backend *be, fs_rid_vector *quads[4]);

int fs_start_import(fs_backend *be, int seg);

int fs_stop_import(fs_backend *be, int seg);

/* vi:set ts=8 sts=4 sw=4: */

#endif
