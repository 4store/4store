#ifndef ORDER_H
#define ORDER_H

#include "query-datatypes.h"
#include "filter.h"

void fs_query_order(fs_query *q);

/* AGG + ORDER BY/HAVING
   to order matrixes of fs_value stored 
   in fs_query->agg_values */
void fs_values_order(fs_query *q); 

int fs_order_by_cmp(fs_value va, fs_value vb);

#endif
