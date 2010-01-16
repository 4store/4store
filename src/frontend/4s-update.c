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

#include "update.h"
#include "common/hash.h"
#include "common/error.h"

int main(int argc, char *argv[])
{
    if (argc != 3) {
        printf("Usage: %s <kb-name> 'LOAD <uri>'\n", argv[0]);

        return 1;
    }

    char *kb_name = argv[1];
    char *password = "";

    fsp_link *link = fsp_open_link(kb_name, password, FS_OPEN_HINT_RW);
    if (!link) {
      fs_error(LOG_ERR, "couldn't connect to “%s”", kb_name);
      return 2;
    }

    raptor_init();
    fs_hash_init(fsp_hash_type(link));

    char *message = NULL;
    int ret = fs_update(link, argv[2], &message, TRUE);
    if (message) printf("%s\n", message);

    return ret;
}

/* vi:set expandtab sts=4 sw=4: */
