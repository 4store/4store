#ifndef HASH_H
#define HASH_H

#include <stdint.h>

#include "4s-datatypes.h"

#define FS_IS_BNODE(x)   (((x) & 0xC000000000000000LL) == 0x8000000000000000LL)
#define FS_IS_URI(x)     (((x) & 0xC000000000000000LL) == 0xC000000000000000LL)
#define FS_IS_URI_BN(x)  (((x) & 0x8000000000000000LL) == 0x8000000000000000LL)
#define FS_IS_LITERAL(x) (((x) & 0x8000000000000000LL) == 0x0000000000000000LL)
#define FS_BNODE_NUM(x)   ((x) - 0x8000000000000000LL)
#define FS_NUM_BNODE(x)   ((x) | 0x8000000000000000LL)

#define FS_RID_SEGMENT(x, size) ((x) & ((size) - 1))

/* for all sorts of complex reasons it's hard for the backend to figure this
 * RID out, it's value depends on the hash function, so it needs to be changed
 * if the hash function changes  */
#define FS_DEFAULT_GRAPH_RID (0xDB4D687EBF8EED87LL)

struct fs_globals {
	fs_rid default_graph;
	fs_rid system_config;
	fs_rid empty;
	fs_rid lang_de;
	fs_rid lang_en;
	fs_rid lang_en_gb;
	fs_rid lang_es;
	fs_rid lang_fr;
	fs_rid rdf_type;
	fs_rid xsd_boolean;
	fs_rid xsd_byte;
	fs_rid xsd_datetime;
	fs_rid xsd_date;
	fs_rid xsd_decimal;
	fs_rid xsd_double;
	fs_rid xsd_float;
	fs_rid xsd_int;
	fs_rid xsd_integer;
	fs_rid xsd_long;
	fs_rid xsd_ninteger;
	fs_rid xsd_nninteger;
	fs_rid xsd_npinteger;
	fs_rid xsd_pinteger;
	fs_rid xsd_short;
	fs_rid xsd_string;
	fs_rid xsd_ubyte;
	fs_rid xsd_uint;
	fs_rid xsd_ulong;
	fs_rid xsd_ushort;
	fs_rid rdfs_label;
	fs_rid fs_text_index;
	fs_rid fs_token;
	fs_rid fs_dmetaphone;
	fs_rid fs_stem;
	fs_rid fs_acl_admin;
	fs_rid fs_acl_access_by;
    fs_rid fs_acl_default_admin;
};

extern struct fs_globals fs_c;

void fs_hash_init(fsp_hash_enum type);
void fs_hash_freshen(void);
void fs_hash_fini(void);

fs_rid fs_hash_uri(const char *str);
fs_rid fs_hash_uri_ignore_bnode(const char *str);
fs_rid fs_hash_literal(const char *str, fs_rid attr);
struct fs_globals fs_global_constants(void);

void umac_crypto_hash(const char *str, char *result);
const char * fs_hash_predefined_uri(fs_rid rid);
const char * fs_hash_predefined_literal(fs_rid rid);

#endif
