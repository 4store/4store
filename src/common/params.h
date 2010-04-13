#ifndef PARAMS_H
#define PARAMS_H

#include <fcntl.h>

//#define FS_MD5   1
//#define FS_CRC64 1
#define FS_UMAC  1

#if defined(FS_CRC64)
#define FS_HASH "CRC64"
#elif defined(FS_MD5)
#define FS_HASH "MD5"
#elif defined(FS_UMAC)
#define FS_HASH "UMAC"
#else
#error "no hash function is defined"
#endif

#define FS_BACKEND_VER GIT_REV FS_HASH
#define FS_FRONTEND_VER GIT_REV

#define FS_MAX_SEGMENTS 256

#define FS_TMP_PATH "/tmp"

#define FS_FILE_MODE 0600

#define FS_EARLIEST_TABLE_VERSION 10
#define FS_CURRENT_TABLE_VERSION 11

#define FS_MAX_BLOCKS 256

#define FS_FANOUT_LIMIT 998

#ifndef O_NOATIME
#define FS_O_NOATIME 0
#else
#define FS_O_NOATIME O_NOATIME
#endif

/* enables profiling of write() times in import clients */
#define FS_PROFILE_WRITE

#define FS_STORE_ROOT "/var/lib/4store"
#define FS_KB_DIR     FS_STORE_ROOT "/%s/"
#define FS_MD_FILE    FS_STORE_ROOT "/%s/metadata.nt"
#define FS_SEG_DIR    FS_STORE_ROOT "/%s/%04x/"
#define FS_FILE_LOCK  FS_STORE_ROOT "/%s/%04x/%s.lock"
#define FS_LEX        FS_STORE_ROOT "/%s/%04x/lex.dat"
#define FS_CHAIN      FS_STORE_ROOT "/%s/%04x/%s.chain"
#define FS_TREE       FS_STORE_ROOT "/%s/%04x/%s.tree"
#define FS_LIST       FS_STORE_ROOT "/%s/%04x/%s.list"
#define FS_QLIST      FS_STORE_ROOT "/%s/%04x/%s.qlist"
#define FS_TLIST      FS_STORE_ROOT "/%s/%04x/m/%016llx.tlist"
#define FS_TLIST_ALL  FS_STORE_ROOT "/%s/%04x/m/*.tlist"
#define FS_TLIST_DIR  FS_STORE_ROOT "/%s/%04x/m/%c%c/%c%c/%s.tlist"
#define FS_TLIST_DIRD FS_STORE_ROOT "/%s/%04x/m/%c%c/%c%c"
#define FS_SLIST      FS_STORE_ROOT "/%s/%04x/%s.slist"
#define FS_RHASH      FS_STORE_ROOT "/%s/%04x/%s.rhash"
#define FS_MHASH      FS_STORE_ROOT "/%s/%04x/%s.mhash"
#define FS_PTREE      FS_STORE_ROOT "/%s/%04x/p%c-%016llx.ptree"
#define FS_PTABLE     FS_STORE_ROOT "/%s/%04x/%s.ptable"
#define FS_TBCHAIN    FS_STORE_ROOT "/%s/%04x/%s.tbchain"

#define FS_LEGAL_KB_CHARS "abcdefghijklmnopqrstuvwxyz" \
                           "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
                           "0123456789_"

#endif
