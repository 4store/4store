#ifndef RESULFS_H
#define RESULFS_H

#include "query-datatypes.h"
#include "filter-datatypes.h"
#include "../common/datatypes.h"

#define FS_RESULT_FLAG_HEADERS             16

/* highly experimental and undocumented INSERT implementation */
#define FS_RESULT_FLAG_CONSTRUCT_AS_INSERT 32

typedef enum {
	FS_TYPE_NONE,
	FS_TYPE_URI,
	FS_TYPE_LITERAL,
	FS_TYPE_BNODE
} fs_result_type;

typedef struct _fs_row {
	const char     *name;
	fs_rid          rid;
	fs_result_type  type;
	const char     *lex;
	const char     *dt;
	const char     *lang;
	int            stop;
} fs_row;

/* evaluate an expression that may include variables, returning the evaluated
 * value of the expression or an error.
 *
 * row is the row in the binding table where the variable bindings may be
 * found, block is the query expression block in which context the expression
 * is being evaluated, or -1 if it has query scope, e is the expression to
 * evaluate */
fs_value fs_expression_eval(fs_query *q, int row, int block, rasqal_expression *e);

void fs_value_to_row(fs_query *q, fs_value v, fs_row *r);

int fs_query_get_columns(fs_query *q);
fs_row *fs_query_fetch_header_row(fs_query *q);
fs_row *fs_query_fetch_row(fs_query *q);
void fs_query_results_output(fs_query *q, const char *fmt, int flags, FILE *out);

/* escape URI, return result must be g_free'd */

char *fs_uri_escape(const char *str);

/* apply ORDER BY to a single column in a binding table, results in *sorted */
int fs_sort_column(fs_query *q, fs_binding *b, int col, int **sorted);

#endif
