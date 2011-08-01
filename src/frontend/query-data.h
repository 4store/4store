#ifndef QUERY_DATA_H
#define QUERY_DATA_H

#include "query-intl.h"
#include "../common/4store.h"

void fs_query_add_freeable(fs_query *q, void *ptr);
void fs_query_add_row_freeable(fs_query *q, void *ptr);
void fs_query_free_row_freeable(fs_query *q);
fsp_link *fs_query_link(fs_query *q);

#endif
