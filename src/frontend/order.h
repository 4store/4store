#ifndef ORDER_H
#define ORDER_H

#include "query-datatypes.h"
#include "filter.h"

void fs_query_order(fs_query *q);

int fs_order_by_cmp(fs_value va, fs_value vb);

#endif
