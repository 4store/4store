#ifndef _4S_STORE_ROOT_H
#define _4S_STORE_ROOT_H

#include <glib.h>

const gchar * fs_get_store_root(void);

#define SINGLETON_STRING_PROTOTYPE(lower_stem)	\
  const gchar *fs_get_ ## lower_stem (void);

SINGLETON_STRING_PROTOTYPE(kb_dir_format)
SINGLETON_STRING_PROTOTYPE(chain_format)
SINGLETON_STRING_PROTOTYPE(file_lock_format)
SINGLETON_STRING_PROTOTYPE(lex_format)
SINGLETON_STRING_PROTOTYPE(list_format)
SINGLETON_STRING_PROTOTYPE(md_file_format)
SINGLETON_STRING_PROTOTYPE(mhash_format)
SINGLETON_STRING_PROTOTYPE(ptable_format)
SINGLETON_STRING_PROTOTYPE(ptree_format)
SINGLETON_STRING_PROTOTYPE(qlist_format)
SINGLETON_STRING_PROTOTYPE(rhash_format)
SINGLETON_STRING_PROTOTYPE(ri_file_format)
SINGLETON_STRING_PROTOTYPE(seg_dir_format)
SINGLETON_STRING_PROTOTYPE(slist_format)
SINGLETON_STRING_PROTOTYPE(tbchain_format)
SINGLETON_STRING_PROTOTYPE(tlist_all_format)
SINGLETON_STRING_PROTOTYPE(tlist_dir_format)
SINGLETON_STRING_PROTOTYPE(tlist_dird_format)
SINGLETON_STRING_PROTOTYPE(tlist_format)
SINGLETON_STRING_PROTOTYPE(tree_format)

#define _FS_KB_DIR_FORMAT       "/%s/"
#define _FS_CHAIN_FORMAT        "/%s/%04x/%s.chain"
#define _FS_FILE_LOCK_FORMAT    "/%s/%04x/%s.lock"
#define _FS_LEX_FORMAT          "/%s/%04x/lex.dat"
#define _FS_LIST_FORMAT         "/%s/%04x/%s.list"
#define _FS_MD_FILE_FORMAT      "/%s/metadata.nt"
#define _FS_MHASH_FORMAT        "/%s/%04x/%s.mhash"
#define _FS_PTABLE_FORMAT       "/%s/%04x/%s.ptable"
#define _FS_PTREE_FORMAT        "/%s/%04x/p%c-%016llx.ptree"
#define _FS_QLIST_FORMAT        "/%s/%04x/%s.qlist"
#define _FS_RHASH_FORMAT        "/%s/%04x/%s.rhash"
#define _FS_RI_FILE_FORMAT      "/%s/runtime.info"
#define _FS_SEG_DIR_FORMAT      "/%s/%04x/"
#define _FS_SLIST_FORMAT        "/%s/%04x/%s.slist"
#define _FS_TBCHAIN_FORMAT      "/%s/%04x/%s.tbchain"
#define _FS_TLIST_ALL_FORMAT    "/%s/%04x/m/*.tlist"
#define _FS_TLIST_DIRD_FORMAT   "/%s/%04x/m/%c%c/%c%c"
#define _FS_TLIST_DIR_FORMAT    "/%s/%04x/m/%c%c/%c%c/%s.tlist"
#define _FS_TLIST_FORMAT        "/%s/%04x/m/%016llx.tlist"
#define _FS_TREE_FORMAT         "/%s/%04x/%s.tree"

#endif
