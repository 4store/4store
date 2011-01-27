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
 *  $Id: prefix-trie-test.c $
 */

#include <stdio.h>
#include <stdlib.h>

#include "prefix-trie.h"

#define CODE(x) code = fs_prefix_trie_get_code(t, x, &plen); printf("code("x") = %d [%s]%s\n", code, code ? pr[code-1].prefix : "", x+plen)

int main()
{
    fs_prefix_trie *t = fs_prefix_trie_new();

    fs_prefix_trie_add_string(t, "http://www.foo.com/AAA");
    fs_prefix_trie_add_string(t, "http://www.foo.com/");
    fs_prefix_trie_add_string(t, "http://www.bar.com/");
    fs_prefix_trie_add_string(t, "http://www.baz.com/");
    fs_prefix_trie_add_string(t, "http://www.foo.com/ABC");
    fs_prefix_trie_add_string(t, "http://www.foo.com/AAB");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xp");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xq");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xr");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://xxx.com/yyyy/xs");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/xxxx");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/xyxx");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/xxxy");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/yyyy");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/zzzz");

    //fs_prefix_trie_print(t);

    printf("best prefixes:\n");
    fs_prefix *pr = fs_prefix_trie_get_prefixes(t, 16);
    for (int row=0; row<16; row++) {
        if (pr[row].score == 0) break;
        printf("%d\t%s\n", pr[row].score, pr[row].prefix);
        fs_prefix_trie_add_code(t, pr[row].prefix, row + 1);
    }
    printf("retreived prefixes:\n");
    for (int row=0; row<16; row++) {
        if (pr[row].score == 0) break;
        int code = fs_prefix_trie_get_code(t, pr[row].prefix, NULL);
        printf("%d\t%s\n", code, pr[row].prefix);
    }
    printf("tests:\n");
    int plen, code;
    code = fs_prefix_trie_get_code(t, "http://www.baaaaaa.com/", &plen);
    printf("code(http://www.baaaaaa.com/) = %d [%s]%s\n", code, code ? pr[code-1].prefix : "", "http://www.baaaaaa.com/"+plen);
    CODE("http://www.baaaaaa.com/");
    CODE("http://www.foo.com/");
    CODE("http://www.xyz.com/");
    CODE("http://xxx.com/yyyy/xyxx/a");
    CODE("http://xxx.com/yyyy/xa");
    CODE("file://");
    free(pr);

    char junk[128];
    junk[127] = '\0';
    for (int i=0; i<200; i++) {
        for (int c=0; c<127; c++) {
            junk[c] = (rand() % 64) + 64;
        }
        if (fs_prefix_trie_add_string(t, junk)) {
            printf("add failed\n");
            break;
        }
    }

    printf("next two blocks of text should match:\n");
    fs_prefix_trie_reset(t);
    fs_prefix_trie_print(t);

    for (int i=0; i<200; i++) {
        for (int c=0; c<127; c++) {
            junk[c] = (rand() % 64) + 64;
        }
        if (fs_prefix_trie_add_string(t, junk)) {
            printf("add failed\n");
            break;
        }
    }

    fs_prefix_trie_reset(t);
    fs_prefix_trie_print(t);

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
