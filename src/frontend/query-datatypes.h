#ifndef QUERY_DATATYPES_H
#define QUERY_DATATYPES_H

#include <rasqal.h>

#include "../common/params.h"
#include "../common/datatypes.h"

#define FS_BINDING_MAX_VARS 128

typedef struct _fs_binding {
	char *name;
	fs_rid_vector *vals;
	rasqal_expression *expression;
			/* if the var is projected as an expression, this
			   points to the expression */
	int proj;	/* is var projected? */
	int selected;	/* is the variable used in final stage selections */
	int used;	/* used in current expression */
	int bound_in_block[FS_MAX_BLOCKS];
			/* number of times var bound in block */
	int need_val;	/* do we need the RID values (for joins or FILTERs) */
	int bound;	/* is var bound yet? */
	int appears;	/* block var first appears in */
	int depends;	/* column that this variable depends on */
	int sort;       /* whether to sort on that column */
} fs_binding;

typedef struct _fs_query fs_query;
typedef struct _fs_query_state fs_query_state;

typedef enum { FS_NONE, FS_INNER, FS_LEFT, FS_UNION } fs_join_type;

fs_binding *fs_binding_new(void);
int fs_binding_set_expression(fs_binding *b, rasqal_variable *var, rasqal_expression *ex);
void fs_binding_free(fs_binding *b);
int fs_binding_any_bound(fs_binding *b);
int fs_binding_width(fs_binding *b);
int fs_binding_length(fs_binding *b);
fs_binding *fs_binding_add(fs_binding *b, rasqal_variable *var, fs_rid val, int projected);
fs_binding *fs_binding_create(fs_binding *b, const char *name, fs_rid val, int projected);
fs_binding *fs_binding_copy(fs_binding *b);
fs_binding *fs_binding_copy_and_clear(fs_binding *b);
void fs_binding_add_vector(fs_binding *b, rasqal_variable *var, fs_rid_vector *vals);
void fs_binding_intersect_vector(fs_binding *b, const char *name,
		        fs_rid_vector *vals);
fs_binding *fs_binding_get(fs_binding *b, rasqal_variable *var);
fs_rid fs_binding_get_val(fs_binding *b, rasqal_variable *var, int idx, int *bound);
fs_rid_vector *fs_binding_get_vals(fs_binding *b, const char *name, int *bound);
void fs_binding_clear_used_all(fs_binding *b);
void fs_binding_set_used(fs_binding *b, rasqal_variable *var);
void fs_binding_copy_row_unused(fs_binding *b1, int row, int count, fs_binding *b2);
void fs_binding_union(fs_query *q, fs_binding *a, fs_binding *b);
void fs_binding_merge(fs_query *q, int block, fs_binding *from, fs_binding *to);
fs_binding *fs_binding_join(fs_query *q, fs_binding *a, fs_binding *b, fs_join_type);
const char *fs_join_type_as_string(fs_join_type t);
void fs_binding_print(fs_binding *b, FILE *out);
void fs_binding_sort(fs_binding *b);
void fs_binding_uniq(fs_binding *b);
void fs_binding_truncate(fs_binding *b, int length);

fs_binding *fs_binding_apply_filters(fs_query *q, int block, fs_binding *b, raptor_sequence *c);

#endif
