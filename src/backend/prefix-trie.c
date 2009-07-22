/*
 *  Copyright (C) 2009 Steve Harris
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  $Id: prefix-trie.c $
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "prefix-trie.h"
#include "common/error.h"

#define PREFIX_EDGES 16

#define TRIE_SIZE 1024

typedef struct _fs_prefix_trie_node fs_prefix_trie_node;

typedef int32_t nodeid;

struct _fs_prefix_trie_node {
    int count;
    int code;
    char label[PREFIX_EDGES];
    nodeid edge[PREFIX_EDGES];
};

struct _fs_prefix_trie {
    int strings;
    fs_prefix_trie_node *nodes;
    nodeid root;
    nodeid free;
};

static nodeid fs_prefix_trie_get_node(fs_prefix_trie *t)
{
    if (t->free == 0) {
//printf("@@ out of nodes\n");
        /* no more free nodes */
        return 0;
    }

    nodeid lastfree = t->free;
    t->free = t->nodes[t->free].edge[0];
//printf("@@ next=%d, free=%d\n", lastfree, t->free);
    memset(&(t->nodes[lastfree]), 0, sizeof(fs_prefix_trie_node));

    return lastfree;
}

fs_prefix_trie *fs_prefix_trie_new()
{
    fs_prefix_trie *t = calloc(1, sizeof(fs_prefix_trie));
    t->nodes = calloc(TRIE_SIZE, sizeof(fs_prefix_trie_node));

    for (int i=0; i<TRIE_SIZE-1; i++) {
        t->nodes[i].edge[0] = i+1;
    }
    /* end of the free list */
    t->nodes[TRIE_SIZE-1].edge[0] = 0;

    t->free = 1;
    t->root = fs_prefix_trie_get_node(t);

    return t;
}

int fs_prefix_trie_add_string(fs_prefix_trie *t, const char *str)
{
    (t->strings)++;
    nodeid n = t->root;

    for (char *pos = (char *)str; *pos; pos++) {
        int edge = -1;
        for (int e=0; e<PREFIX_EDGES; e++) {
            if (t->nodes[n].label[e] == *pos) {
                t->nodes[n].count++;
                edge = e;
                break;
            } else if (t->nodes[n].label[e] == '\0') {
                t->nodes[n].count++;
                t->nodes[n].label[e] = *pos;
                t->nodes[n].edge[e] = fs_prefix_trie_get_node(t);
                if (t->nodes[n].edge[e] == 0) {
                    /* no more nodes left */
                    return 1;
                }
                edge = e;
                break;
            }
        }
        if (edge == -1) {
printf("@@ no room for '%s' at depth %d\n", str, pos-str);
            return 1;
        }
        n = t->nodes[n].edge[edge];
    }

    return 0;
}

int fs_prefix_trie_get_code(fs_prefix_trie *t, const char *str)
{
    nodeid n = t->root;
    int deepest_code = 0;

    for (char *pos = (char *)str; *pos; pos++) {
        int edge = -1;
        for (int e=0; e<PREFIX_EDGES; e++) {
            if (t->nodes[n].label[e] == *pos) {
                edge = e;
                break;
            } else if (t->nodes[n].label[e] == '\0') {
                return deepest_code;
            }
        }
        if (edge == -1) {
            fs_error(LOG_ERR, "should never get here");
        }
        n = t->nodes[n].edge[edge];
        if (t->nodes[n].code) {
            deepest_code = t->nodes[n].code;
        }
    }

    return deepest_code;
}

int fs_prefix_trie_add_code(fs_prefix_trie *t, const char *str, int code)
{
    nodeid n = t->root;

    for (char *pos = (char *)str; *pos; pos++) {
        int edge = -1;
        for (int e=0; e<PREFIX_EDGES; e++) {
            if (t->nodes[n].label[e] == *pos) {
                edge = e;
                break;
            } else if (t->nodes[n].label[e] == '\0') {
                t->nodes[n].label[e] = *pos;
                t->nodes[n].edge[e] = fs_prefix_trie_get_node(t);
                if (t->nodes[n].edge[e] == 0) {
                    /* no more nodes left */
                    return 1;
                }
                edge = e;
                break;
            }
        }
        if (edge == -1) {
printf("@@ no room for '%s' at depth %d\n", str, pos-str);
            return 1;
        }
        if (*(pos+1) == '\0') t->nodes[n].code = code;
        n = t->nodes[n].edge[edge];
    }

    return 0;
}

static void fs_prefix_trie_get_prefixes_intl(fs_prefix_trie *t, char *current,
    fs_prefix *pr, int last_score, nodeid node, int depth)
{
    for (int e=0; e<PREFIX_EDGES; e++) {
        if (t->nodes[node].edge[e] == 0) {
            return;
        }
        int score = (t->nodes[node].count - 1) * depth;
        if (score < last_score) {
            /* last one was a candidate */
           
            char last_char = current[depth-1]; 
            current[depth-1] = '\0';
            /* add to pr table */
            int lowest_row = 0;
            int lowest_score = FS_MAX_PREFIX_LENGTH * t->strings;
            for (int row=0; row<FS_PREFIXES; row++) {
                /* check for duplicates */
                if (pr[row].score == last_score) {
                    if (strcmp(pr[row].prefix, current) == 0) {
                        lowest_row = -1;
                        break;
                    }
                } else if (pr[row].score == 0) {
                    pr[row].score = last_score;
                    strncpy(pr[row].prefix, current, FS_MAX_PREFIX_LENGTH);
                    lowest_row = -1;
                    break;
                }
                if (pr[row].score < lowest_score) {
                    lowest_score = pr[row].score;
                    lowest_row = row;
                }
            }
            if (!lowest_row != -1 && lowest_score < last_score) {
                pr[lowest_row].score = last_score;
                strncpy(pr[lowest_row].prefix, current, FS_MAX_PREFIX_LENGTH);
            }
            current[depth-1] = last_char;
        }
        current[depth] = t->nodes[node].label[e];
        current[depth+1] = '\0';
        //printf("%d (%d)\t%s\n", score, t->nodes[node].count, current);
        fs_prefix_trie_get_prefixes_intl(t, current, pr, score, t->nodes[node].edge[e], depth+1);
    }
}

fs_prefix *fs_prefix_trie_get_prefixes(fs_prefix_trie *t)
{
    fs_prefix *pr = calloc(FS_PREFIXES, sizeof(fs_prefix));
    char current[256];

    fs_prefix_trie_get_prefixes_intl(t, current, pr, 0, t->root, 0);

    return pr;
}

static void fs_prefix_trie_print_intl(fs_prefix_trie *t, nodeid node, int depth)
{
    for (int e=0; e<PREFIX_EDGES; e++) {
        if (t->nodes[node].edge[e] == 0) {
            break;
        }
        for (int i=0; i<depth; i++) {
            printf(" ");
        }
        printf("%c (%d)\n", t->nodes[node].label[e], t->nodes[node].count);
        //printf("%c(%d)", t->nodes[node].label[e], t->nodes[node].count);
        fs_prefix_trie_print_intl(t, t->nodes[node].edge[e], depth+1);
    }
}

void fs_prefix_trie_print(fs_prefix_trie *t)
{
    fs_prefix_trie_print_intl(t, t->root, 0);
}

/* vi:set expandtab sts=4 sw=4: */
