#ifndef PREFIX_TRIE_H
#define PREFIX_TRIE_H

#define FS_MAX_PREFIX_LENGTH 255

typedef struct _fs_prefix_trie fs_prefix_trie;

typedef struct _fs_prefix {
    int score;
    char prefix[FS_MAX_PREFIX_LENGTH+1];
} fs_prefix;

fs_prefix_trie *fs_prefix_trie_new();

int fs_prefix_trie_add_string(fs_prefix_trie *t, const char *str);

void fs_prefix_trie_print(fs_prefix_trie *t);

fs_prefix *fs_prefix_trie_get_prefixes(fs_prefix_trie *t, int number);

int fs_prefix_trie_add_code(fs_prefix_trie *t, const char *str, int code);

int fs_prefix_trie_get_code(fs_prefix_trie *t, const char *str, int *prefix_len);

void fs_prefix_trie_reset(fs_prefix_trie *t);

void fs_prefix_trie_free(fs_prefix_trie *t);

/* vi:set expandtab sts=4 sw=4: */

#endif
