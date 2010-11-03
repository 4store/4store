#ifndef QUERY_DATA_H
#define QUERY_DATA_H

#include "query-intl.h"
#include "../common/4store.h"

void fs_query_add_freeable(fs_query *q, void *ptr);
fsp_link *fs_query_link(fs_query *q);

#endif
