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

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "4s-datatypes.h"
#include "4s-hash.h"

#define FS_RID_SET_ENTRIES 1024
#define FS_RID_ENTRY_HASH(r) ((r >> 12) & (FS_RID_SET_ENTRIES-1))

struct rid_entry {
    fs_rid            val;
    struct rid_entry *next;
};

struct _fs_rid_set {
    struct rid_entry entry[FS_RID_SET_ENTRIES];
    int scan_hash;
    struct rid_entry *scan_entry;
};

#ifdef DEBUG_RV_ALLOC
fs_rid_vector *fs_rid_vector_new_logged(int length, char *file, int line)
{
    fs_rid_vector *t = calloc(1, sizeof(fs_rid_vector));
    printf("@@ RV %s:%d %p 0NEW\n", file, line, t);
    t->file = file;
    t->line = line;
    t->length = length;
    const int size = length < 32 ? 32 : length;
    t->size = size;
    t->data = calloc(size, sizeof(fs_rid));

    return t;
}
#else
fs_rid_vector *fs_rid_vector_new(int length)
{
    fs_rid_vector *t = calloc(1, sizeof(fs_rid_vector));
    t->length = length;
    const int size = length < 32 ? 32 : length;
    t->size = size;
    t->data = calloc(size, sizeof(fs_rid));

    return t;
}
#endif

fs_rid_vector *fs_rid_vector_new_from_args(int length, ...)
{
    va_list argp;
    fs_rid_vector *t = fs_rid_vector_new(length);

    va_start(argp, length);
    for (int i=0; i<length; i++) {
	t->data[i] = va_arg(argp, fs_rid);
    }

    return t;
}

void fs_rid_vector_append(fs_rid_vector *v, fs_rid r)
{
    if (v->length >= v->size) {
	if (v->size) {
	    v->size *= 2;
	} else {
	    v->size = 32;
	}
	v->data = realloc(v->data, sizeof(fs_rid) * v->size);
    }
    v->data[v->length] = r;
    v->length++;
}

void fs_rid_vector_append_vector(fs_rid_vector *v, fs_rid_vector *v2)
{
    if (!v) return;
    if (!v2) return;

    if (v2->length > 4 && v->size - v->length < v2->length) {
	v->size += v2->length > 32 ? v2->length : 32;
	v->data = realloc(v->data, sizeof(fs_rid) * v->size);
    }

    if (v->size - v->length >= v2->length) {
	memcpy(&v->data[v->length], v2->data, sizeof(fs_rid) * v2->length);
	v->length += v2->length;

	return;
    }

    for (int j=0; j<v2->length; j++) {
	fs_rid_vector_append(v, v2->data[j]);
    }
}

void fs_rid_vector_append_vector_no_nulls(fs_rid_vector *v, fs_rid_vector *v2)
{
    if (!v2) return;

    for (int j=0; j<v2->length; j++) {
	if (v2->data[j] != FS_RID_NULL) {
	    fs_rid_vector_append(v, v2->data[j]);
	}
    }
}

void fs_rid_vector_append_vector_no_nulls_lit(fs_rid_vector *v, fs_rid_vector *v2)
{
    if (!v2) return;

    for (int j=0; j<v2->length; j++) {
	if (v2->data[j] != FS_RID_NULL && !FS_IS_LITERAL(v2->data[j])) {
	    fs_rid_vector_append(v, v2->data[j]);
	}
    }
}

void fs_rid_vector_append_set(fs_rid_vector *v, fs_rid_set *s)
{
    if (!s) return;

    for (int hash=0; hash < FS_RID_SET_ENTRIES; hash++) {
	for (struct rid_entry *e=&(s->entry[hash]); e; e=e->next) {
	    if (e->val != FS_RID_NULL) {
		fs_rid_vector_append(v, e->val);
	    }
	}
    }
}

fs_rid_vector *fs_rid_vector_copy(fs_rid_vector *v)
{
    if (!v) return NULL;

    fs_rid_vector *v2 = fs_rid_vector_new(v->size);
    v2->length = v->length;
    memcpy(v2->data, v->data, v->size * sizeof(fs_rid));

    return v2;
}

void fs_rid_vector_clear(fs_rid_vector *v)
{
    v->length = 0;
}

void fs_rid_vector_print(fs_rid_vector *v, int flags, FILE *out)
{
    if (!v) {
	fprintf(out, "RID vector: (null)\n");

	return;
    }
    fprintf(out, "RID vector (%d items)\n", v->length);
    for (int i=0; i<v->length; i++) {
	fprintf(out, "%4d %016llx\n", i, v->data[i]);
    }
}

static int rva_data_compare(fs_rid_vector **v, int count, int p1, fs_rid w[])
{
    for (int i=0; i<count; i++) {
	if (v[i]->data[p1] > w[i]) {
	    return 1;
	}
	if (v[i]->data[p1] < w[i]) {
	    return -1;
	}
    }

    return 0;
}

static void rva_swap(fs_rid_vector **v, int count, int a, int b)
{
    fs_rid tmp;

    for (int i=0; i<count; i++) {
	tmp = v[i]->data[a];
	v[i]->data[a] = v[i]->data[b];
	v[i]->data[b] = tmp;
    }
}

static int rid_partition(fs_rid_vector **v, int count, int left, int right)
{
    int pivot = (left + right) / 2;
    fs_rid pvals[count];
    for (int i=0; i<count; i++) {
	pvals[i] = v[i]->data[pivot];
    }

    rva_swap(v, count, pivot, right);

    int store = left;

    for (int p = left; p < right; p++) {
	if (rva_data_compare(v, count, p, pvals) <= 0) {
	    rva_swap(v, count, store, p);
	    store++;
	}
    }
    rva_swap(v, count, right, store);

    return store;
}

/* inplace quicksort on an array of rid_vectors */
void fs_rid_vector_array_sort(fs_rid_vector **v, int count, int left, int right)
{
    if (right > left) {
	int pivot = rid_partition(v, count, left, right);
	fs_rid_vector_array_sort(v, count, left, pivot-1);
	fs_rid_vector_array_sort(v, count, pivot+1, right);
    }
}

static int rid_compare(const void *va, const void *vb)
{
    /* these have to be signed to avoid a conflict with the way mysql aggregate
     * functions work */
    int64_t a = *((fs_rid *)va);
    int64_t b = *((fs_rid *)vb);

    if (a > b) return 1;
    if (a < b) return -1;

    return 0;
}

void fs_rid_vector_sort(fs_rid_vector *v)
{
    qsort(v->data, v->length, sizeof(fs_rid), rid_compare);
}

void fs_rid_vector_uniq(fs_rid_vector *v, int remove_null)
{
    if (!v) return;

    int length = v->length;

    if (!v->data) return;

    int outrow = 0;
    for (int row = 0; row < length; row++) {
	if (remove_null && v->data[row] == FS_RID_NULL) continue;
	if (row < length - 1 && v->data[row] == v->data[row+1]) {
	    continue;
	}
	v->data[outrow] = v->data[row];
	outrow++;
    }
    v->length = outrow;
}

static int inter_sub(int count, int pos, const fs_rid_vector *rv[], fs_rid val)
{
    int found = 0;

    if (pos >= count) return 1;

    for (int i=0; i<rv[pos]->length; i++) {
	if (rv[pos]->data[i] == val) {
	    found = 1;
	    break;
	}
    }

    if (!found) return 0;

    return inter_sub(count, pos+1, rv, val);
}

fs_rid_vector *fs_rid_vector_intersect(int count, const fs_rid_vector *rv[])
{
    fs_rid_vector *ret = fs_rid_vector_new(0);

    for (int i=0; i<rv[0]->length; i++) {
	if (inter_sub(count, 1, rv, rv[0]->data[i])) {
	    fs_rid_vector_append(ret, rv[0]->data[i]);
	}
    }

    return ret;
}

void fs_rid_vector_truncate(fs_rid_vector *rv, int32_t length)
{
    if (!rv) return;

    if (rv->length < length) return;

    rv->length = length;
}

void fs_rid_vector_grow(fs_rid_vector *rv, int32_t length)
{
    if (!rv) return;

    if (length > rv->size) {
	rv->data = realloc(rv->data, sizeof(fs_rid *) * length);
	rv->size = length;
    }

    rv->length = length;
}

int fs_rid_vector_contains(fs_rid_vector *v, fs_rid r)
{
    if (!v) return 0;
    if (!v->data) return 0;

    for (int row = 0; row < v->length; row++) {
	if (v->data[row] == r) return 1;
    }

    return 0;
}

char *fs_rid_vector_to_string(fs_rid_vector *v)
{
    char *ret = calloc(24, v->length);
    char *pos = ret;

    for (int i=0; i<v->length; i++) {
	if (i>0) *pos++ = ',';
	pos += sprintf(pos, "%lld", v->data[i]);
    }

    return ret;
}

void fs_rid_vector_free(fs_rid_vector *v)
{
    if (v) {
#ifdef DEBUG_RV_ALLOC
	printf("@@ RV %s:%d %p 1FREE\n", v->file, v->line, v);
#endif
	free(v->data);
	free(v);
    }
}

fs_p_vector *fs_p_vector_new(int length)
{
    fs_p_vector *t = calloc(1, sizeof(fs_p_vector));
    t->length = length;
    int size = length < 32 ? 32 : length;
    t->size = size;
    t->data = calloc(size, sizeof(void *));

    return t;
}

void fs_p_vector_append(fs_p_vector *v, void *p)
{
    if (v->length >= v->size) {
	if (v->size == 0) v->size = 32;
	while (v->length >= v->size) {
	    v->size *= 2;
	}
	v->data = realloc(v->data, sizeof(void *) * v->size);
    }
    v->data[v->length] = p;
    v->length++;
}

void fs_p_vector_append_vector(fs_p_vector *v, fs_p_vector *v2)
{
    if (!v) return;
    if (!v2) return;

    if (v2->length > 4 && v->size - v->length < v2->length) {
	v->size += v2->length > 32 ? v2->length : 32;
	v->data = realloc(v->data, sizeof(void *) * v->size);
    }

    if (v->size - v->length >= v2->length) {
	memcpy(&v->data[v->length], v2->data, sizeof(void *) * v2->length);
	v->length += v2->length;

	return;
    }

    for (int j=0; j<v2->length; j++) {
	fs_p_vector_append(v, v2->data[j]);
    }
}

void fs_p_vector_clear(fs_p_vector *v)
{
    v->length = 0;
}

void fs_p_vector_free(fs_p_vector *v)
{
    if (v) {
	free(v->data);
	free(v);
    }
}

fs_rid_str_vector *fs_rid_str_vector_new(int length)
{
    fs_rid_str_vector *t = calloc(1, sizeof(fs_rid_str_vector));
    t->length = length;
    t->size = length;
    t->rdata = calloc(length, sizeof(fs_rid));
    t->sdata = calloc(length, sizeof(char *));

    return t;
}

void fs_rid_str_vector_free(fs_rid_str_vector *t)
{
    if (t) {
	for (int i=0; i<t->length; i++) {
	    free(t->sdata[i]);
	}
	free(t->rdata);
	free(t->sdata);
	free(t);
    }
}

fs_rid_set *fs_rid_set_new()
{
    fs_rid_set *s = calloc(1, sizeof(fs_rid_set));
    for (int i=0; i<FS_RID_SET_ENTRIES; i++) {
	s->entry[i].val = FS_RID_NULL;
    }

    return s;
}

void fs_rid_set_add(fs_rid_set *s, fs_rid val)
{
    if (val == FS_RID_NULL) return;

    struct rid_entry *e = &s->entry[FS_RID_ENTRY_HASH(val)];
    if (e->val == val) return;
    if (e->val == FS_RID_NULL) {
	e->val = val;

	return;
    }
    while (e->next) {
	if (e->val == val) return;
	e = e->next;
    }

    struct rid_entry *ne = calloc(1, sizeof(struct rid_entry));
    ne->val = val;
    e->next = ne;
}

int fs_rid_set_contains(fs_rid_set *s, fs_rid val)
{
    if (val == FS_RID_NULL) return 0;

    struct rid_entry *e = &s->entry[FS_RID_ENTRY_HASH(val)];

    if (e->val == FS_RID_NULL) return 0;
    if (e->val == val) return 1;

    while (e->val != FS_RID_NULL && e->next) {
	if (e->val == val) return 1;
	e = e->next;
    }
    return (e->val == val) && (e->val != FS_RID_NULL);
}

int fs_rid_set_rewind(fs_rid_set *s)
{
    if (!s) return 1;

    for (int i=0; i<FS_RID_SET_ENTRIES; i++) {
        if (s->entry[i].val != FS_RID_NULL) {
            s->scan_hash = i;
            s->scan_entry = &s->entry[i];

            return 0;
        }
    }

    s->scan_hash = FS_RID_SET_ENTRIES;
    s->scan_entry = NULL;

    return 0;
}

fs_rid fs_rid_set_next(fs_rid_set *s)
{
    while (s->scan_hash < FS_RID_SET_ENTRIES) {
	if (s->scan_entry) {
	    if (s->scan_entry->val != FS_RID_NULL) {
		fs_rid rid = s->scan_entry->val;
		s->scan_entry = s->scan_entry->next;

		return rid;
	    }
	}
	(s->scan_hash)++;
	s->scan_entry = &s->entry[s->scan_hash];
    }

    return FS_RID_NULL;
}

void fs_rid_set_print(fs_rid_set *s)
{
    printf("rid_set at %p\n", s);
    for (int i=0; i<FS_RID_SET_ENTRIES; i++) {
	if (s->entry[i].val != FS_RID_NULL) {
	    printf("  %llx\n", s->entry[i].val);
	}
	if (s->entry[i].next) {
	    struct rid_entry *e = s->entry[i].next;
	    while (e) {
		printf("  %llx\n", e->val);
		e = e->next;
	    }
	}
    }
}

void fs_rid_set_free(fs_rid_set *s)
{
    for (int i=0; i<FS_RID_SET_ENTRIES; i++) {
	if (s->entry[i].next) {
	    struct rid_entry *e = s->entry[i].next;
	    while (e) {
		struct rid_entry *next = e->next;
		free(e);
		e = next;
	    }
	}
    }

    free(s);
}

double fs_time()
{
    struct timeval now;

    gettimeofday(&now, 0);

    return (double)now.tv_sec + (now.tv_usec * 0.000001);
}

int fs_resource_cmp(const void *va, const void *vb)
{
    const fs_resource *a = va;
    const fs_resource *b = vb;

    if (a->rid < b->rid) return -1;
    if (a->rid > b->rid) return 1;

    return 0;
}

/* vi:set ts=8 sts=4 sw=4: */
