/*
 *  Copyright (C) 2009 Steve Harris
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
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
#include "../common/error.h"

#define PREFIX_EDGES 16

#define TRIE_SIZE 4096

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
        /* no more free nodes */
        return 0;
    }

    nodeid lastfree = t->free;
    t->free = t->nodes[t->free].edge[0];
    memset(&(t->nodes[lastfree]), 0, sizeof(fs_prefix_trie_node));

    return lastfree;
}

static void fs_prefix_trie_free_node(fs_prefix_trie *t, nodeid n)
{
    memset(&(t->nodes[n]), 0, sizeof(fs_prefix_trie_node));
    t->nodes[n].edge[0] = t->free;
    t->free = n;
}

static void fs_prefix_trie_free_subtree(fs_prefix_trie *t, nodeid n)
{
    for (int e=0; e<PREFIX_EDGES; e++) {
        if (t->nodes[n].edge[e]) {
            fs_prefix_trie_free_subtree(t, t->nodes[n].edge[e]);
        }
    }
    fs_prefix_trie_free_node(t, n);
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

    for (char *pos = (char *)str; *pos && pos-str < FS_MAX_PREFIX_LENGTH; pos++) {
        int edge = -1;
        int lowest_count = t->strings + 1;
        int lowest_edge = -1;
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
            } else {
                if (t->nodes[t->nodes[n].edge[e]].count < lowest_count) {
                    lowest_count = t->nodes[t->nodes[n].edge[e]].count;
                    lowest_edge = e;
                }
            }
        }
        /* if we couldn't fit it in anywhere, make space */
        if (edge == -1) {
            if (lowest_edge == -1) {
                fs_error(LOG_ERR, "unable to fit string at depth %ld", (long int)(pos-str));

                return 1;
            }
            fs_prefix_trie_free_subtree(t, t->nodes[n].edge[lowest_edge]);
            t->nodes[n].label[lowest_edge] = *pos;
            t->nodes[n].edge[lowest_edge] = fs_prefix_trie_get_node(t);
            if (t->nodes[n].edge[lowest_edge] == 0) {
                /* no more nodes left */
                return 1;
            }
            edge = lowest_edge;
        }
        n = t->nodes[n].edge[edge];
        t->nodes[n].count++;
    }

    return 0;
}

int fs_prefix_trie_get_code(fs_prefix_trie *t, const char *str, int *prefix_len)
{
    nodeid n = t->root;
    int deepest_code = 0;
    int plen = 0;
    int depth = 0;

    for (char *pos = (char *)str; *pos; pos++) {
        int edge = -1;
        for (int e=0; e<PREFIX_EDGES; e++) {
            if (t->nodes[n].label[e] == *pos) {
                edge = e;
                break;
            } else if (t->nodes[n].label[e] == '\0') {
                if (prefix_len) *prefix_len = plen;

                return deepest_code;
            }
        }
        if (edge == -1) {
            fs_error(LOG_ERR, "should never get here");

            return 0;
        }
        if (t->nodes[n].code) {
            deepest_code = t->nodes[n].code;
            plen = pos - str + 1;
        }
        n = t->nodes[n].edge[edge];
        depth++;
    }

    if (prefix_len) *prefix_len = plen;

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
            return 1;
        }
        if (*(pos+1) == '\0') t->nodes[n].code = code;
        n = t->nodes[n].edge[edge];
    }

    return 0;
}

static void fs_prefix_trie_get_prefixes_intl(fs_prefix_trie *t, int max,
    char *current, fs_prefix *pr, int last_score, nodeid node, int depth)
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
            for (int row=0; row < max; row++) {
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
            /* if there's a less effective one in the list, replace */
            if (lowest_row != -1 && lowest_score < last_score) {
                pr[lowest_row].score = last_score;
                strncpy(pr[lowest_row].prefix, current, FS_MAX_PREFIX_LENGTH);
            }
            current[depth-1] = last_char;
        }
        current[depth] = t->nodes[node].label[e];
        current[depth+1] = '\0';
        if (depth >= FS_MAX_PREFIX_LENGTH - 1) {
            return;
        }
        fs_prefix_trie_get_prefixes_intl(t, max, current, pr, score, t->nodes[node].edge[e], depth+1);
    }
}

static int sort_prefixes(const void *va, const void *vb)
{
    const fs_prefix *a = (fs_prefix *)va;
    const fs_prefix *b = (fs_prefix *)vb;

    return (b->score - a->score);
}

fs_prefix *fs_prefix_trie_get_prefixes(fs_prefix_trie *t, int max)
{
    fs_prefix *pr = calloc(max, sizeof(fs_prefix));
    char current[FS_MAX_PREFIX_LENGTH+1];

    fs_prefix_trie_get_prefixes_intl(t, max, current, pr, 0, t->root, 0);

    /* sort in descending score order */
    qsort(pr, max, sizeof(fs_prefix), sort_prefixes);

    /* remove prefixes that are slight superstrings of higher scoring ones */
    for (int i=0; i<max; i++) {
        if (pr[i].score == 0) {
            break;
        }
        for (int j=0; j<i; j++) {
            const int li = strlen(pr[i].prefix);
            const int lj = strlen(pr[j].prefix);
            /* if lower scoring prefix is shorter, we still want it */
            if (li < lj) {
                continue;
            }
            /* if lower scoring prefix is much longer, we still want it */
            if (li > lj + 8) {
                continue;
            }
            /* if lower scoring prefix is a superstring, ditch it */
            if (strncmp(pr[i].prefix, pr[j].prefix, lj) == 0) {
                pr[i].score = 0;
            }
        }
    }

    /* resort in descending score order, so removed prefixes go to the end */
    qsort(pr, max, sizeof(fs_prefix), sort_prefixes);

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
        printf("%c (%d)\n", t->nodes[node].label[e], t->nodes[t->nodes[node].edge[e]].count);
        fs_prefix_trie_print_intl(t, t->nodes[node].edge[e], depth+1);
    }
}

void fs_prefix_trie_print(fs_prefix_trie *t)
{
    printf("trie: %p\n", t);
    int free = 0;
    printf("  root node: %d\n", t->root);
    for (nodeid pos = t->free; pos; pos = t->nodes[pos].edge[0]) free++;
    printf("  free nodes: %d\n", free);
    fs_prefix_trie_print_intl(t, t->root, 0);
}

void fs_prefix_trie_reset(fs_prefix_trie *t)
{
    fs_prefix_trie_free_subtree(t, t->root);

    t->root = fs_prefix_trie_get_node(t);
}

void fs_prefix_trie_free(fs_prefix_trie *t)
{
    free(t->nodes);
    free(t);
}

/* vi:set expandtab sts=4 sw=4: */
