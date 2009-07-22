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
 *  $Id: prefix-trie-test.c $
 */

#include <stdio.h>
#include <stdlib.h>

#include "prefix-trie.h"

#define CODE(x) printf("code("x") = %d\n", fs_prefix_trie_get_code(t, x))

int main()
{
    fs_prefix_trie *t = fs_prefix_trie_new();

    fs_prefix_trie_add_string(t, "http://www.foo.com/AAA");
    fs_prefix_trie_add_string(t, "http://www.foo.com/");
    fs_prefix_trie_add_string(t, "http://www.bar.com/");
    fs_prefix_trie_add_string(t, "http://www.baz.com/");
    fs_prefix_trie_add_string(t, "http://www.foo.com/ABC");
    fs_prefix_trie_add_string(t, "http://www.foo.com/AAB");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/xxxx");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/yyyy");
    fs_prefix_trie_add_string(t, "http://www.xxx.com/xxxx/zzzz");

    fs_prefix_trie_print(t);

    printf("best prefixes:\n");
    fs_prefix *pr = fs_prefix_trie_get_prefixes(t);
    for (int row=0; row<FS_PREFIXES; row++) {
        if (pr[row].score == 0) break;
        printf("%d\t%s\n", pr[row].score, pr[row].prefix);
        fs_prefix_trie_add_code(t, pr[row].prefix, row + 1);
    }
    printf("retreived prefixes:\n");
    for (int row=0; row<FS_PREFIXES; row++) {
        if (pr[row].score == 0) break;
        int code = fs_prefix_trie_get_code(t, pr[row].prefix);
        printf("%d\t%s\n", code, pr[row].prefix);
    }
    free(pr);
    printf("tests:\n");
    CODE("http://baaaaaa.com/");
    CODE("http://foo.com/");

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
