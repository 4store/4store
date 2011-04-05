#ifndef DATATYPES_H
#define DATATYPES_H

#include <stdint.h>
#include <stdio.h>

/* uncomment to get a trace of allocated red_vectors */
//#define DEBUG_RV_ALLOC

#define FS_RID_NULL 0x8000000000000000LL
#define FS_RID_GONE 0x0000000000000000LL

#define FS_BIND_MODEL              0x01
#define FS_BIND_SUBJECT            0x02
#define FS_BIND_PREDICATE          0x04
#define FS_BIND_OBJECT             0x08
#define FS_BIND_DISTINCT          0x100
#define FS_BIND_OPTIONAL          0x200
#define FS_BIND_UNION             0x400
#define FS_BIND_REVERSE           #error feature removed
#define FS_QUERY_CONSOLE_OUTPUT   0x800

#define FS_BIND_SAME_MASK        0xf000
#define FS_BIND_SAME_XXXX        0x0000
#define FS_BIND_SAME_XXAA        0x1000
#define FS_BIND_SAME_XAXA        0x2000
#define FS_BIND_SAME_XAAX        0x3000
#define FS_BIND_SAME_XAAA        0x4000
#define FS_BIND_SAME_AXXA        0x5000
#define FS_BIND_SAME_AXAX        0x6000
#define FS_BIND_SAME_AXAA        0x7000
#define FS_BIND_SAME_AAXX        0x8000
#define FS_BIND_SAME_AAXA        0x9000
#define FS_BIND_SAME_AAAX        0xa000
#define FS_BIND_SAME_AAAA        0xb000
#define FS_BIND_SAME_AABB        0xc000
#define FS_BIND_SAME_ABAB        0xd000
#define FS_BIND_SAME_ABBA        0xe000

#define FS_QUERY_RESTRICTED        0x800000

#define FS_BIND_BY_SUBJECT        0x1000000
#define FS_BIND_BY_OBJECT         0x2000000
/* FS_BIND_START is backend-only, never sent over the wire */
#define FS_BIND_END               0x8000000
/* FS_BIND_PRICE is backend-only, never sent over the wire */
#define FS_BIND_PRICE            0x10000000
#define FS_QUERY_EXPLAIN         0x20000000
#define FS_QUERY_COUNT           0x40000000
#define FS_QUERY_DEFAULT_GRAPH   0x80000000

typedef unsigned long long int fs_rid;
typedef uint32_t fs_segment;
typedef uint32_t fs_index_node;

typedef struct {
    uint32_t length;
    uint32_t size;
    fs_rid *data;
#ifdef DEBUG_RV_ALLOC
    char *file;
    int line;
#endif
} fs_rid_vector;

typedef struct {
    uint32_t length;
    uint32_t size;
    void **data;
} fs_p_vector;

typedef struct {
    uint32_t length;
    uint32_t size;
    fs_rid *rdata;
    char **sdata;
} fs_rid_str_vector;

typedef struct _fs_resource {
    fs_rid  rid;
    char   *lex;
    fs_rid  attr; /* DT/lang etc */
} fs_resource;

typedef struct _fs_rid_set fs_rid_set;

typedef struct _fs_import_timing {
    double add_s;
    double add_o;
    double add_r;
    double commit_q;
    double commit_r;
    double remove;
    double rebuild;
} fs_import_timing;

typedef struct _fs_query_timing {
    int bind_count;
    double bind;
    int price_count;
    double price;
    int resolve_count;
    double resolve;
} fs_query_timing;

typedef struct _fs_old_data_size {
    unsigned long long int quads_s;
    unsigned long long int quads_o;
    unsigned long long int resources;
    unsigned long long int models_s;
    unsigned long long int models_o;
} fs_old_data_size;

typedef struct _fs_data_size {
    unsigned long long int quads_s;
    unsigned long long int quads_o;
    unsigned long long int quads_sr;
    unsigned long long int resources;
    unsigned long long int models_s;
    unsigned long long int models_o;
} fs_data_size;

typedef struct _fs_quad_freq {
    fs_rid pri;		/* primary key value */
    fs_rid sec;		/* secondary key value */
    long long freq;	/* approximate quantity of entries */
} fs_quad_freq;

typedef enum {
  FS_HASH_UNKNOWN,
  FS_HASH_MD5,
  FS_HASH_UMAC,
  FS_HASH_CRC64
} fsp_hash_enum;

#ifdef DEBUG_RV_ALLOC
#define fs_rid_vector_new(len) fs_rid_vector_new_logged(len, __FILE__, __LINE__)
fs_rid_vector *fs_rid_vector_new_logged(int length, char *file, int line);
#else
fs_rid_vector *fs_rid_vector_new(int length);
#endif

fs_rid_vector *fs_rid_vector_new_from_args(int length, ...);
void fs_rid_vector_append(fs_rid_vector *v, fs_rid r);
void fs_rid_vector_append_vector(fs_rid_vector *v, fs_rid_vector *v2);
void fs_rid_vector_append_vector_no_nulls(fs_rid_vector *v, fs_rid_vector *v2);
void fs_rid_vector_append_vector_no_nulls_lit(fs_rid_vector *v, fs_rid_vector *v2);
fs_rid_vector *fs_rid_vector_copy(fs_rid_vector *v);
void fs_rid_vector_clear(fs_rid_vector *v);
void fs_rid_vector_print(fs_rid_vector *v, int flags, FILE *out);
void fs_rid_vector_array_sort(fs_rid_vector **v, int count, int left, int right);
void fs_rid_vector_sort(fs_rid_vector *v);
void fs_rid_vector_uniq(fs_rid_vector *v, int remove_null);
int fs_rid_vector_contains(fs_rid_vector *v, fs_rid r);
char *fs_rid_vector_to_string(fs_rid_vector *v);
fs_rid_vector *fs_rid_vector_intersect(int count, const fs_rid_vector *rv[]);
void fs_rid_vector_truncate(fs_rid_vector *rv, int32_t length);
void fs_rid_vector_free(fs_rid_vector *t);

fs_p_vector *fs_p_vector_new(int length);
void fs_p_vector_append(fs_p_vector *v, void *p);
void fs_p_vector_append_vector(fs_p_vector *v, fs_p_vector *p);
void fs_rid_vector_append_set(fs_rid_vector *v, fs_rid_set *s);
void fs_p_vector_clear(fs_p_vector *v);
void fs_p_vector_free(fs_p_vector *v);
#define fs_rid_vector_length(rv) ((rv) ? (rv)->length : 0)

fs_rid_str_vector *fs_rid_str_vector_new(int length);
void fs_rid_str_vector_free(fs_rid_str_vector *t);

fs_rid_set *fs_rid_set_new(void);
void fs_rid_set_add(fs_rid_set *s, fs_rid val);
int fs_rid_set_contains(fs_rid_set *s, fs_rid vsl);
int fs_rid_set_rewind(fs_rid_set *s);
fs_rid fs_rid_set_next(fs_rid_set *s);
void fs_rid_set_print(fs_rid_set *s);
void fs_rid_set_free(fs_rid_set *s);
 
double fs_time(void); 

int fs_resource_cmp(const void *va, const void *vb);

/* vi:set ts=8 sts=4 sw=4: */

#endif
