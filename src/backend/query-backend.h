/*
 *  Copyright (C) 2006 Steve Harris
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  $Id: query-backend.h $
 */

#include "backend.h"
#include "../common/datatypes.h"

fs_rid_vector **fs_bind(fs_backend *be, fs_segment segment, unsigned int tobind,
			     fs_rid_vector *mv, fs_rid_vector *sv,
			     fs_rid_vector *pv, fs_rid_vector *ov,
                             int offset, int limit);

/* WARNING: check code, this function does not behave like fs_bind() even
 * though it has the same signature */
fs_rid_vector **fs_reverse_bind(fs_backend *be, fs_segment segment,
			       unsigned int tobind,
			       fs_rid_vector *mv, fs_rid_vector *sv,
			       fs_rid_vector *pv, fs_rid_vector *ov,
                               int offset, int limit);

fs_rid_vector **fs_bind_first(fs_backend *be, fs_segment segment, unsigned int tobind,
                             fs_rid_vector *mv, fs_rid_vector *sv,
                             fs_rid_vector *pv, fs_rid_vector *ov,
                             int count);

fs_rid_vector **fs_bind_next(fs_backend *be, fs_segment segment, unsigned int tobind,
                             int count);

int fs_bind_done(fs_backend *be, fs_segment segment);

unsigned long long int fs_bind_price(fs_backend *be, fs_segment segment, unsigned int tobind,
			     fs_rid_vector *mv, fs_rid_vector *sv,
			     fs_rid_vector *pv, fs_rid_vector *ov);

int fs_resolve(fs_backend *be, fs_segment segment, fs_rid_vector *v,
	fs_resource *out);

int fs_resolve_rid(fs_backend *be, fs_segment segment, fs_rid rid,
	fs_resource *out);

void fs_rid_vector_print_resolved(fs_backend *be, fs_rid_vector *v, int flags, FILE *out);

fs_data_size fs_get_data_size(fs_backend *be, int seg);

char *fs_lexstore_fetch(fs_backend *be, fs_segment segment, char type, fs_rid ptr, char *outp, int length);

/* vi:set ts=8 sts=4 sw=4: */
