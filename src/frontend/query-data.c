/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*
 *  Copyright (C) 2007 Steve Harris for Garlik
 */

#include "query-datatypes.h"
#include "query-intl.h"

void fs_query_add_freeable(fs_query *q, void *ptr)
{
    if (!q) return;

    q->free_list = g_slist_prepend(q->free_list, ptr);
}

fsp_link *fs_query_link(fs_query *q)
{
    if (q) {
        return q->link;
    }

    return NULL;
}

/* vi:set expandtab sts=4 sw=4: */
