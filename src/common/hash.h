#ifndef HASH_H
#define HASH_H

#include <glib.h>
#include <stdint.h>

#include "common/datatypes.h"
#include "common/4store.h"

#define FS_IS_BNODE(x)   (((x) & 0xC000000000000000LL) == 0x8000000000000000LL)
#define FS_IS_URI(x)     (((x) & 0xC000000000000000LL) == 0xC000000000000000LL)
#define FS_IS_URI_BN(x)  (((x) & 0x8000000000000000LL) == 0x8000000000000000LL)
#define FS_IS_LITERAL(x) (((x) & 0x8000000000000000LL) == 0x0000000000000000LL)
#define FS_BNODE_NUM(x)   ((x) - 0x8000000000000000LL)
#define FS_NUM_BNODE(x)   ((x) | 0x8000000000000000LL)

#define FS_RID_SEGMENT(x, size) ((x) & ((size) - 1))

struct fs_globals {
	fs_rid empty;
	fs_rid xsd_string;
	fs_rid xsd_integer;
	fs_rid xsd_float;
	fs_rid xsd_double;
	fs_rid xsd_decimal;
	fs_rid xsd_boolean;
	fs_rid xsd_datetime;
	fs_rid xsd_pinteger;
	fs_rid xsd_ninteger;
	fs_rid xsd_npinteger;
	fs_rid xsd_nninteger;
	fs_rid xsd_long;
	fs_rid xsd_int;
	fs_rid xsd_short;
	fs_rid xsd_byte;
	fs_rid xsd_ulong;
	fs_rid xsd_uint;
	fs_rid xsd_ushort;
	fs_rid xsd_ubyte;
	fs_rid lang_en;
	fs_rid lang_fr;
	fs_rid lang_de;
	fs_rid lang_es;
	fs_rid rdf_type;
};

extern struct fs_globals fs_c;

void fs_hash_init(fsp_hash_enum type);
void fs_hash_freshen();
void fs_hash_fini();

extern fs_rid (*fs_hash_uri)(const char *str);
extern fs_rid (*fs_hash_literal)(const char *str, fs_rid attr);
GHashTable * fs_hash_bnids();

void umac_crypto_hash(const char *str, char *result);

#endif
