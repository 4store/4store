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
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include "sort.h"
#include "../common/4s-datatypes.h"

int quad_sort_by_subject(const void *va, const void *vb)
{
    const fs_rid *a = va;
    const fs_rid *b = vb;

    if (a[1] != FS_RID_NULL && b[1] != FS_RID_NULL) {
        if (a[1] < b[1]) return -1;
        if (a[1] > b[1]) return 1;
    } else {
        return 0;
    }
    if (a[2] != FS_RID_NULL && b[2] != FS_RID_NULL) {
        if (a[2] < b[2]) return -1;
        if (a[2] > b[2]) return 1;
    } else {
        return 0;
    }
    if (a[0] != FS_RID_NULL && b[0] != FS_RID_NULL) {
        if (a[0] < b[0]) return -1;
        if (a[0] > b[0]) return 1;
    } else {
        return 0;
    }
    if (a[3] != FS_RID_NULL && b[3] != FS_RID_NULL) {
        if (a[3] < b[3]) return -1;
        if (a[3] > b[3]) return 1;
    }

    return 0;
}

int quad_sort_by_object(const void *va, const void *vb)
{
    const fs_rid *a = va;
    const fs_rid *b = vb;

    if (a[3] != FS_RID_NULL && b[3] != FS_RID_NULL) {
        if (a[3] < b[3]) return -1;
        if (a[3] > b[3]) return 1;
    } else {
        return 0;
    }
    if (a[2] != FS_RID_NULL && b[2] != FS_RID_NULL) {
        if (a[2] < b[2]) return -1;
        if (a[2] > b[2]) return 1;
    } else {
        return 0;
    }
    if (a[0] != FS_RID_NULL && b[0] != FS_RID_NULL) {
        if (a[0] < b[0]) return -1;
        if (a[0] > b[0]) return 1;
    } else {
        return 0;
    }
    if (a[1] != FS_RID_NULL && b[1] != FS_RID_NULL) {
        if (a[1] < b[1]) return -1;
        if (a[1] > b[1]) return 1;
    }

    return 0;
}

int quad_sort_by_mspo(const void *va, const void *vb)
{
    const fs_rid *a = va;
    const fs_rid *b = vb;

    if (a[0] != b[0]) {
        if (a[0] < b[0]) return -1;
        if (a[0] > b[0]) return 1;
    }
    if (a[1] != b[1]) {
        if (a[1] < b[1]) return -1;
        if (a[1] > b[1]) return 1;
    }
    if (a[2] != b[2]) {
        if (a[2] < b[2]) return -1;
        if (a[2] > b[2]) return 1;
    }
    if (a[3] != b[3]) {
        if (a[3] < b[3]) return -1;
        if (a[3] > b[3]) return 1;
    }

    return 0;
}

#define SORT(n) if (a[n] != b[n]) { if (a[n] < b[n]) return -1; if (a[n] > b[n]) return 1; }

int quad_sort_by_psmo(const void *va, const void *vb)
{
    const fs_rid *a = va;
    const fs_rid *b = vb;

    SORT(2);
    SORT(1);
    SORT(0);
    SORT(3);

    return 0;
}

int quad_sort_by_poms(const void *va, const void *vb)
{
    const fs_rid *a = va;
    const fs_rid *b = vb;

    SORT(2);
    SORT(3);
    SORT(0);
    SORT(1);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
