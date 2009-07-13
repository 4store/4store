#ifndef TREE_INTL_H
#define TREE_INTL_H

struct tree_blacklist {
    struct tree_blacklist *next;
    fs_rid a;
    fs_rid b;
};

struct tree_header {
    uint32_t id;
    char chainfile[500]; /* this makes it exactly 512 bytes */
    int32_t length;
    int32_t size;
};

struct _fs_tree {
    fs_backend *be;
    char *filename;
    struct tree_header *header;
    fs_chain *bc;
    int fd;
    size_t len;
    char *ptr;
    fs_tree_index *data;
    struct tree_blacklist *blacklist;
};

#endif
