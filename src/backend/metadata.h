#ifndef METADATA_H
#define METADATA_H

#include "../common/datatypes.h"

/* metadata properties */
#define FS_MD_PREFIX			"http://4store.org/metadata#"
#define FS_MD_NAME			FS_MD_PREFIX "kb_name"
#define FS_MD_SEGMENTS			FS_MD_PREFIX "num_segments"
#define FS_MD_VERSION			FS_MD_PREFIX "version"
#define FS_MD_SALT			FS_MD_PREFIX "salt"
#define FS_MD_HASH			FS_MD_PREFIX "hash"
#define FS_MD_SEGMENT_P			FS_MD_PREFIX "segment_p"
#define FS_MD_SEGMENT_M			FS_MD_PREFIX "segment_m"
#define FS_MD_BNODE			FS_MD_PREFIX "bnode"
#define FS_MD_HASHFUNC			FS_MD_PREFIX "hash_function"
#define FS_MD_STORE			FS_MD_PREFIX "store_type"
#define FS_MD_MODEL_DATA		FS_MD_PREFIX "model_data"
#define FS_MD_MODEL_DIRS		FS_MD_PREFIX "model_dirs"
#define FS_MD_MODEL_FILES		FS_MD_PREFIX "model_files"
#define FS_MD_CODE_VERSION		FS_MD_PREFIX "code_version"

#define FS_MD_PKSALT			FS_MD_PREFIX "pksalt"
#define FS_MD_PWSALT			FS_MD_PREFIX "pwsalt"
#define FS_MD_PKHASH			FS_MD_PREFIX "pkhash"
#define FS_MD_PWHASH			FS_MD_PREFIX "pwhash"

typedef struct _fs_metadata fs_metadata;

fs_metadata *fs_metadata_open(const char *kb);
int fs_metadata_clear(fs_metadata *m);
int fs_metadata_flush(fs_metadata *m);
void fs_metadata_close(fs_metadata *m);

/* return pointer to string entry, do not free */
const char *fs_metadata_get_string(fs_metadata *m, const char *prop, const char *def);

/* return value converted to an integer */
long long int fs_metadata_get_int(fs_metadata *m, const char *prop, long long int def);

/* return RID vector of int values, for multiple predicate values, return value
 * should be free'd */
fs_rid_vector *fs_metadata_get_int_vector(fs_metadata *m, const char *prop);

/* return value converted to a boolean, "true" is 1, everything else is 0 */
int fs_metadata_get_bool(fs_metadata *m, const char *prop, int def);

/* replace exisitng entry if exists, add if not */
int fs_metadata_set(fs_metadata *m, const char *key, const char *val);
int fs_metadata_set_int(fs_metadata *m, const char *key, long long int val);

/* add new entry */
int fs_metadata_add(fs_metadata *m, const char *key, const char *val);
int fs_metadata_add_int(fs_metadata *m, const char *key, long long int val);

#endif
