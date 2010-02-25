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

    Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <glib.h>
#include <errno.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "common/timing.h"

#include "tree.h"
#include "tlist.h"
#include "backend.h"
#include "backend-intl.h"
#include "common/hash.h"
#include "common/params.h"
#include "common/datatypes.h"
#include "common/error.h"
#include "import-backend.h"
#include "query-backend.h"
#include "lock.h"

#define RES_BUF_SIZE  10240
#define QUAD_BUF_SIZE 10240

#define CACHE_SIZE 32768
#define CACHE_MASK (CACHE_SIZE - 1)
#define CACHE_ENTRY(r) (rid_cache[((r)>>10) & CACHE_MASK])

static long res_pos;
static long quad_pos;

static fs_rid rid_cache[CACHE_SIZE];

static fs_resource res_buffer[RES_BUF_SIZE];

struct q_buf {
    int skip;
    fs_rid quad[4];
};

static struct q_buf quad_buffer[QUAD_BUF_SIZE];

int quick_res_check(fs_backend *be, int seg, fs_rid rid, char *lex)
{
    if (CACHE_ENTRY(rid) == rid) {
	return 1;
    }
    CACHE_ENTRY(rid) = rid;

    return 0;
}

int fs_res_import(fs_backend *be, int seg, long count, fs_resource buffer[])
{
    double then = fs_time();
    int i = 0;
    while (i < count) {
	for (; i < count && res_pos < RES_BUF_SIZE; i++) {
	    /* don't remember why this is commented out anymore. swh 2009-07-06 */
	    //XXX if (!quick_res_check(be, seg, buffer[i].rid, buffer[i].lex)) {
		res_buffer[res_pos].rid = buffer[i].rid;
		res_buffer[res_pos].attr = buffer[i].attr;
		res_buffer[res_pos].lex = g_strdup(buffer[i].lex);
		res_pos++;
	    //}
	}
	if (res_pos == RES_BUF_SIZE) {
	    fs_res_import_commit(be, seg, 0);
	}
    }
    double now = fs_time();
    be->in_time[seg].add_r += now - then;

    return 0;
}

int fs_res_import_commit(fs_backend *be, int seg, int account)
{
    if (seg < 0 || seg >= be->segments) {
	fs_error(LOG_ERR, "segment number %d out of range", seg);
    }
    double then = fs_time();

    fs_rhash_put_multi(be->res, res_buffer, res_pos);
    for (int i=0; i<res_pos; i++) {
	g_free(res_buffer[i].lex);
    }

    res_pos = 0;

    if (account) {
	double now = fs_time();
	be->in_time[seg].commit_r += now - then;
    }

    return 0;
}

int fs_quad_import(fs_backend *be, int seg, int flags, int count, fs_rid buffer[][4])
{
    if ((flags & (FS_BIND_BY_SUBJECT | FS_BIND_BY_OBJECT)) == 0) {
	fs_error(LOG_ERR, "neither FS_BIND_BY_SUBJECT or FS_BIND_BY_OBJECT set");

	return 1;
    }
    if (flags & FS_BIND_BY_OBJECT) {
	fs_error(LOG_WARNING, "this backend doesn't use FS_BIND_BY_OBJECT");

	return 2;
    }

    if (seg < 0 || seg >= be->segments) {
	fs_error(LOG_ERR, "segment number %d out of range", seg);

	return 3;
    }

    double then = fs_time();
    int i = 0;
    while (i < count) {
	for (; i < count && quad_pos < QUAD_BUF_SIZE; i++, quad_pos++) {
	    quad_buffer[quad_pos].skip = 0;
	    quad_buffer[quad_pos].quad[0] = buffer[i][0];
	    quad_buffer[quad_pos].quad[1] = buffer[i][1];
	    quad_buffer[quad_pos].quad[2] = buffer[i][2];
	    quad_buffer[quad_pos].quad[3] = buffer[i][3];
	}
	if (quad_pos == QUAD_BUF_SIZE) {
	    if (!be->pended_import) {
		be->pended_import = 1;
		for (int pend=0; pend < FS_PENDED_LISTS; pend++) {
		    char label[256];
		    snprintf(label, 255, "pl-%1x", pend);
		    be->pended[pend] = fs_list_open(be, label,
			sizeof(fs_rid) * 4, O_CREAT | O_TRUNC | O_RDWR);
		}
	    }
	    int ret = fs_quad_import_commit(be, seg, flags, 0);
	    if (ret) {
		fs_error(LOG_CRIT, "quad commit failed");

		return ret;
	    }
	}
    }

    double now = fs_time();
    be->in_time[seg].add_s += now - then;

    return 0;
}

static int qbuf_sort_ps(const void *va, const void *vb)
{
    const struct q_buf *a = va;
    const struct q_buf *b = vb;
    const int keys[4] = { 2, 1, 3, 0 };

    for (int i=0; i<4; i++) {
	if (a->quad[keys[i]] < b->quad[keys[i]]) {
	    return -1;
	}
	if (a->quad[keys[i]] > b->quad[keys[i]]) {
	    return 1;
	}
    }

    return 0;
}

static int qbuf_sort_po(const void *va, const void *vb)
{
    const struct q_buf *a = va;
    const struct q_buf *b = vb;

    if (a->quad[2] < b->quad[2]) {
	return -1;
    }
    if (a->quad[2] > b->quad[2]) {
	return 1;
    }
    if (a->quad[3] < b->quad[3]) {
	return -1;
    }
    if (a->quad[3] > b->quad[3]) {
	return 1;
    }

    return 0;
}

static int qbuf_sort_m(const void *va, const void *vb)
{
    const struct q_buf *a = va;
    const struct q_buf *b = vb;

    if (a->quad[0] < b->quad[0]) {
	return -1;
    }
    if (a->quad[0] > b->quad[0]) {
	return 1;
    }

    return 0;
}

int fs_quad_import_commit(fs_backend *be, int seg, int flags, int account)
{
    if ((flags & (FS_BIND_BY_SUBJECT | FS_BIND_BY_OBJECT)) == 0) {
	fs_error(LOG_ERR, "neither FS_BIND_BY_SUBJECT or FS_BIND_BY_OBJECT set");

	return 1;
    }
    if (flags & FS_BIND_BY_OBJECT) {
	fs_error(LOG_WARNING, "this backend doesn't use FS_BIND_BY_OBJECT");

	return 2;
    }

    if (seg < 0 || seg >= be->segments) {
	fs_error(LOG_ERR, "segment number %d out of range", seg);

	return 3;
    }

    double then = fs_time();

    TIME(NULL);

    if (be->pended_import) {
	for (int i=0; i<quad_pos; i++) {
	    if (quad_buffer[i].skip) continue;

	    const fs_rid pred = quad_buffer[i].quad[2];
	    const int pend_list = (pred >> 40) % FS_PENDED_LISTS;
	    fs_list_add(be->pended[pend_list], quad_buffer[i].quad);
	}
    } else {
	for (int pass=0; pass<2; pass++) {
	    if (pass == 0) {
		qsort(quad_buffer, quad_pos, sizeof(struct q_buf),
		      qbuf_sort_ps);
		for (int i=1; i<quad_pos; i++) {
		    if (quad_buffer[i].quad[0] == quad_buffer[i-1].quad[0] &&
			quad_buffer[i].quad[1] == quad_buffer[i-1].quad[1] &&
			quad_buffer[i].quad[2] == quad_buffer[i-1].quad[2] &&
			quad_buffer[i].quad[3] == quad_buffer[i-1].quad[3]) {
			quad_buffer[i].skip = 1;
		    }
		}
	    } else {
		qsort(quad_buffer, quad_pos, sizeof(struct q_buf),
		      qbuf_sort_po);
	    }
	    const int force = pass == 1 ? 1 : 0;
	    fs_rid last_pred = FS_RID_NULL;
	    fs_ptree *pt = NULL;

	    for (int i=0; i<quad_pos; i++) {
		if (quad_buffer[i].skip) continue;

		const fs_rid pred = quad_buffer[i].quad[2];
		if (last_pred != pred) {
		    pt = NULL;
		    pt = fs_backend_get_ptree(be, pred, pass);
		    if (!pt) {
			fs_backend_open_ptree(be, pred);
			int id = fs_list_add(be->predicates, &pred);
			if (pass == 0) pt = be->ptrees_priv[id].ptree_s;
			else pt = be->ptrees_priv[id].ptree_o;
		    }
		    last_pred = pred;
		}
		int ds0, ds1, pk;
		if (pass == 0) {
		    ds0 = 0; ds1 = 3; pk = 1;
		} else {
		    ds0 = 0; ds1 = 1; pk = 3;
		}
		fs_rid pair[2] = { quad_buffer[i].quad[ds0], quad_buffer[i].quad[ds1] };
		int ret = fs_ptree_add(pt, quad_buffer[i].quad[pk], pair, force);
		if (pass == 0 && ret) {
		    quad_buffer[i].skip = 1;
		}
		if (pass == 0 && !ret) {
		    (be->approx_size)++;
		}
	    }
	}
    }
    fs_list_flush(be->predicates);

    /* append to model indexes */
    qsort(quad_buffer, quad_pos, sizeof(struct q_buf), qbuf_sort_m);
    fs_tlist *tl = NULL;
    fs_rid last_model = FS_RID_NULL;
    fs_index_node model_node = 0;
    for (int i=0; i<quad_pos; i++) {
	if (quad_buffer[i].skip) continue;

	const fs_rid model = quad_buffer[i].quad[0];
	if (model != last_model) {
	    if (tl) {
		fs_tlist_close(tl);
	    }
	    tl = NULL;
	    model_node = 0;
	    fs_index_node node = 0;
	    if (fs_mhash_get(be->models, model, &node)) {
		fs_error(LOG_ERR, "failed to get node for model %016llx",
			 model);
	    }
	    if (node == 1) {
		tl = fs_tlist_open(be, model, O_RDWR);
		if (!tl) {
		    fs_error(LOG_CRIT, "failed to open tlist");
		}
	    } else if (node == 0) {
		if (fs_backend_model_files(be)) {
		    tl = fs_tlist_open(be, model, O_CREAT | O_RDWR);
		    fs_mhash_put(be->models, model, 1);
		} else {
		    model_node = node;
		}
	    } else {
		model_node = node;
	    }
	    last_model = model;
	}

	if (tl) {
	    fs_tlist_add(tl, &quad_buffer[i].quad[1]);
	} else {
	    if (!model_node) {
		model_node = fs_tbchain_new_chain(be->model_list);
		fs_mhash_put(be->models, model, model_node);
	    }
	    fs_index_node new_node = fs_tbchain_add_triple(be->model_list,
		model_node, &quad_buffer[i].quad[1]);
	    if (new_node != model_node) {
		model_node = new_node;
		fs_mhash_put(be->models, model, model_node);
	    }
	}
    }
    if (tl) {
	fs_tlist_close(tl);
    }
    
    TIME("list append");

    quad_pos = 0;

    if (account) {
	double now = fs_time();
	be->in_time[seg].commit_q += now - then;
    }

    return 0;
}

int fs_delete_quads(fs_backend *be, fs_rid_vector *quads[4])
{
    int errors = 0;
    fs_rid_set *preds = fs_rid_set_new();
    for (int i=0; i<quads[2]->length; i++) {
	fs_rid_set_add(preds, quads[2]->data[i]);
    }
    fs_rid_set *models = fs_rid_set_new();
    for (int i=0; i<quads[0]->length; i++) {
	fs_rid_set_add(models, quads[0]->data[i]);
    }
    fs_rid pred;
    fs_rid_set_rewind(preds);
    while ((pred = fs_rid_set_next(preds)) != FS_RID_NULL) {
	fs_ptree *sfp = fs_backend_get_ptree(be, pred, 0);
	fs_ptree *ofp = fs_backend_get_ptree(be, pred, 1);
	if (!sfp && !ofp) {
	    /* this predicate doesn't exist in this segment */
	    continue;
	}
	if (!sfp) {
	    fs_error(LOG_CRIT, "failed to get s ptree for pred %016llx", pred);
	    continue;
	}
	if (!ofp) {
	    fs_error(LOG_CRIT, "failed to get o ptree for pred %016llx", pred);
	    continue;
	}

	for (int i=0; i<quads[2]->length; i++) {
	    if (quads[2]->data[i] == pred) {
		fs_rid spair[2] = { quads[0]->data[i], quads[3]->data[i] };
		fs_ptree_remove(sfp, quads[1]->data[i], spair);
		fs_rid opair[2] = { quads[0]->data[i], quads[1]->data[i] };
		fs_ptree_remove(ofp, quads[3]->data[i], opair);
	    }
	}
    }
    fs_rid_set_free(preds);
    fs_rid model;
    fs_rid_set_rewind(preds);
    while ((model = fs_rid_set_next(models)) != FS_RID_NULL) {
        fs_index_node val = 0;
        fs_mhash_get(be->models, model, &val);
        if (val > 1) {
	    fs_tbchain_set_bit(be->model_list, val, FS_TBCHAIN_SUPERSET);
	}
    }

    return errors;
}

static int remove_by_search(fs_backend *be, fs_rid model, fs_index_node model_id)
{
    int errors = 0;
    fs_tbchain_it *it = fs_tbchain_new_iterator(be->model_list, model, model_id);
    fs_rid triple[3];
    fs_rid_set *preds = fs_rid_set_new();
    while (fs_tbchain_it_next(it, triple)) {
	fs_rid_set_add(preds, triple[1]);
    }
    fs_tbchain_it_free(it);
    fs_rid pred;
    fs_rid_set_rewind(preds);
    while ((pred = fs_rid_set_next(preds)) != FS_RID_NULL) {
	fs_ptree *sfp = fs_backend_get_ptree(be, pred, 0);
	fs_ptree *ofp = fs_backend_get_ptree(be, pred, 1);
	if (!sfp && !ofp) {
	    fs_error(LOG_CRIT, "failed to get ptrees for pred %016llx", pred);
	    continue;
	}
	if (!sfp) {
	    fs_error(LOG_CRIT, "failed to get s ptree for pred %016llx", pred);
	    continue;
	}
	if (!ofp) {
	    fs_error(LOG_CRIT, "failed to get o ptree for pred %016llx", pred);
	    continue;
	}

	fs_tbchain_it *it =
	    fs_tbchain_new_iterator(be->model_list, model, model_id);
	fs_rid triple[3];
	while (fs_tbchain_it_next(it, triple)) {
	    if (triple[1] == pred) {
		fs_rid spair[2] = { model, triple[2] };
		if (fs_ptree_remove(sfp, triple[0], spair)) {
		    fs_error(LOG_ERR, "failed to remove known quad %016llx %016llx %016llx %016llx from s index", model, triple[0], triple[1], triple[2]);
		    errors++;
		}
		fs_rid opair[2] = { model, triple[0] };
		if (fs_ptree_remove(ofp, triple[2], opair)) {
		    fs_error(LOG_ERR, "failed to remove known quad %016llx %016llx %016llx %016llx from o index", model, triple[0], triple[1], triple[2]);
		    errors++;
		}
	    }
	}
    }
    fs_rid_set_free(preds);
    errors += fs_tbchain_remove_chain(be->model_list, model_id);

    return errors;
}

int fs_delete_models(fs_backend *be, int seg, fs_rid_vector *mvec)
{
    double then = fs_time();
    int errs = 0;

    fs_rid_vector *todo = fs_rid_vector_new(0);
    for (int i=0; i<mvec->length; i++) {
        fs_rid model = mvec->data[i];

        if (model == FS_RID_NULL) {
            fs_rid_vector_free(todo);
            todo = fs_rid_vector_new_from_args(1, FS_RID_NULL);
        } else if (!FS_IS_URI(model)) {
            fs_error(LOG_WARNING, "given non-URI model %lld to delete, ignoring" , model);
        }

        fs_index_node exists = 1; /* in case no model list is available */
        if (be->model_data) {
            fs_backend_model_get_usage(be, seg, model, &exists);
        }
        if (exists) {
            fs_rid_vector_append(todo, model);
        }
    }

    if (todo->data[0] == FS_RID_NULL) {
        fs_backend_unlink_indexes(be, seg);
        fs_backend_close_files(be, seg);
        errs += fs_backend_open_files(be, seg, O_RDWR | O_CREAT | O_TRUNC, FS_OPEN_ALL);
        fs_rid_vector_free(todo);

        return errs;
    }

    /* special case for the common case where we're going to replace a
     * small graph in a large KB */
    if (todo->length == 1) {
        fs_index_node val = 0;
        fs_mhash_get(be->models, todo->data[0], &val);
        /* we don't handle the tlist case, and we only want to handle
         * graphs that are less than 1% of the total size, or really small
         * this way */
        if (val > 1) {
	    long long chain_length = fs_tbchain_length(be->model_list, val);
	    if (chain_length < be->approx_size / 100 ||
		chain_length < 100) {
		remove_by_search(be, todo->data[0], val);
		fs_backend_model_set_usage(be, seg, todo->data[0], 0);
		fs_rid_vector_free(todo);

		return errs;
	    }
        }
    }

    for (int i=0; i<todo->length; i++) {
        fs_index_node val = 0;
        fs_mhash_get(be->models, todo->data[i], &val);
        if (val == 1) {
	    fs_tlist *tl = fs_tlist_open(be, todo->data[i], O_RDWR);
	    if (tl) {
		fs_tlist_truncate(tl);
		fs_tlist_close(tl);
	    }
	} else if (val > 1) {
	    fs_tbchain_remove_chain(be->model_list, val);
        }
        fs_backend_model_set_usage(be, seg, todo->data[i], 0);
    }

    if (todo->length) {
        for (int i=0; i<be->ptree_length; i++) {
	    fs_backend_ptree_limited_open(be, i);
	    for (int j=0; j<todo->length; j++) {
		fs_rid pair[2] = { todo->data[j], FS_RID_NULL };
		/* if nothing is removed from the s index, we can skip the
		       o index */
		/* if we had a remove function that took a list of model
		   rids we could be more efficient here, but really we'd
		   like to use a model list where possible  */

		if (!fs_ptree_remove_all(be->ptrees_priv[i].ptree_s, pair)) {
		    fs_ptree_remove_all(be->ptrees_priv[i].ptree_o, pair);
		}
	    }
        }
    }

    fs_rid_vector_free(todo);
    fs_mhash_flush(be->models);

    double now = fs_time();
    be->in_time[seg].remove += now - then;

    return errs;
}

/* vi:set ts=8 sts=4 sw=4: */
