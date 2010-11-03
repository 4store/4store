/*
 *  Copyright (C) 2010 Steve Harris
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
 *  $Id: gnu-options.c $
 */

/* handle standard GNU options, --help, --version is a sruffy, but compliant
 * way. Returns 1 if the app should print help to stdout and exit. */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libgen.h>

int fs_gnu_options(int argc, char *argv[], char *help)
{
    if (argc != 2) {
        return 0;
    }

    if (!strcmp(argv[1], "--help")) {
        printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);
        if (help) {
            printf("Usage: %s %s", basename(argv[0]), help);

            exit(0);
        }

        return 1;
    } else if (!strcmp(argv[1], "--version")) {
        printf("%s, built for 4store %s\n", basename(argv[0]), GIT_REV);

        exit(0);
    }

    return 0;
}

/* vi:set expandtab sts=4 sw=4: */
