#include <stdlib.h>
#include <string.h>
#include "common/4s-store-root.h"
#include "common/params.h"

const gchar * fs_get_store_root(void)
{
    static gchar * _fs_store_root = NULL;
    const char *env_setting;
    if(NULL == _fs_store_root) {
	env_setting = getenv(FS_STORE_ROOT_ENV_VAR);
	if(env_setting) {
	    _fs_store_root = g_strdup((const gchar *)env_setting);
	} else {
	    _fs_store_root = strdup((const gchar *)FS_BUILD_TIME_STORE_ROOT);
	}
    }
    return _fs_store_root;
}

#define SINGLETON_STRING_GET_FUNCTION(lower_stem,UPPER_STEM)		\
    const gchar *fs_get_ ## lower_stem (void)				\
    {									\
	static gchar *_fs_ ## lower_stem = NULL;			\
	    if(NULL == _fs_ ## lower_stem) {				\
		_fs_ ## lower_stem = g_strconcat(fs_get_store_root(),	\
						 _FS_ ## UPPER_STEM,	\
						 NULL);			\
	    }								\
	    return _fs_ ## lower_stem;					\
    }

SINGLETON_STRING_GET_FUNCTION(kb_dir_format,     KB_DIR_FORMAT)
SINGLETON_STRING_GET_FUNCTION(chain_format,      CHAIN_FORMAT)
SINGLETON_STRING_GET_FUNCTION(file_lock_format,  FILE_LOCK_FORMAT)
SINGLETON_STRING_GET_FUNCTION(lex_format,        LEX_FORMAT)
SINGLETON_STRING_GET_FUNCTION(list_format,       LIST_FORMAT)
SINGLETON_STRING_GET_FUNCTION(md_file_format,    MD_FILE_FORMAT)
SINGLETON_STRING_GET_FUNCTION(mhash_format,      MHASH_FORMAT)
SINGLETON_STRING_GET_FUNCTION(ptable_format,     PTABLE_FORMAT)
SINGLETON_STRING_GET_FUNCTION(ptree_format,      PTREE_FORMAT)
SINGLETON_STRING_GET_FUNCTION(qlist_format,      QLIST_FORMAT)
SINGLETON_STRING_GET_FUNCTION(rhash_format,      RHASH_FORMAT)
SINGLETON_STRING_GET_FUNCTION(ri_file_format,    RI_FILE_FORMAT)
SINGLETON_STRING_GET_FUNCTION(seg_dir_format,    SEG_DIR_FORMAT)
SINGLETON_STRING_GET_FUNCTION(slist_format,      SLIST_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tbchain_format,    TBCHAIN_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tlist_all_format,  TLIST_ALL_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tlist_dir_format,  TLIST_DIR_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tlist_dird_format, TLIST_DIRD_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tlist_format,      TLIST_FORMAT)
SINGLETON_STRING_GET_FUNCTION(tree_format,       TREE_FORMAT)
