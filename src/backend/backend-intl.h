#ifndef BACKEND_INTL_H
#define BACKEND_INTL_H

#include "list.h"
#include "rhash.h"
#include "mhash.h"
#include "ptree.h"
#include "ptable.h"
#include "tbchain.h"
#include "metadata.h"

struct ptree_ref {
    fs_rid pred;
    fs_ptree *ptree_s;
    fs_ptree *ptree_o;
    fs_list *pend;
};

struct _fs_backend {
    const char *db_name;
    fs_metadata *md;
    fs_segment segments;
    fs_segment segment;
    int salt;
    int stream;
    const char *hash;
    FILE *lex_f;
    fs_list *pending_delete;
    fs_list *pending_insert;
    fs_rhash *res;
    fs_mhash *models;
    fs_tbchain *model_list;
    fs_list *predicates;
    fs_ptable *pairs;
    int pended_import;
    int ptree_size;
    int ptree_length;
    struct ptree_ref *ptrees;
    fs_import_timing in_time[FS_MAX_SEGMENTS];
    fs_query_timing out_time[FS_MAX_SEGMENTS];
    int checked_transaction;
    int transaction;
    int mid_commit;
    int model_data;
    int model_dirs;
    int model_files;
    long long approx_size; /* a value read from ptrees at startup, and updated
			    * not guaranteed to be accurate */
    float min_free;
};

#endif

/* vi:set ts=8 sts=4 sw=4: */
