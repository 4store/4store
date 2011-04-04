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
 *  $Id: 4s-update.c $
 */

#include <stdio.h>
#include <raptor.h>
#include <rasqal.h>

#include "query.h"
#include "update.h"
#include "../common/error.h"
#include "../common/gnu-options.h"

int main(int argc, char *argv[])
{
    fs_gnu_options(argc, argv, "<kb-name> <sparql-update-request>\n");

    if (argc != 3) {
        printf("Usage: %s <kb-name> <sparql-update-request>\n", argv[0]);

        return 1;
    }

    char *kb_name = argv[1];
    char *password = "";

    fsp_link *link = fsp_open_link(kb_name, password, FS_OPEN_HINT_RW);
    if (!link) {
      fs_error(LOG_ERR, "couldn't connect to “%s”", kb_name);
      return 2;
    }

    fs_hash_init(fsp_hash_type(link));

    fs_query_state *qs = fs_query_init(link, NULL, NULL);
    char *message = NULL;
    int ret = fs_update(qs, argv[2], &message, TRUE);
    if (message) {
        printf("%s\n", message);
        g_free(message);
    }
    fsp_close_link(link);
    fs_query_fini(qs);

    return ret;
}

/* vi:set expandtab sts=4 sw=4: */
