/*
    4store - a clustered RDF storage and query engine

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
/*
 *  Copyright (C) 2006 Steve Harris for Garlik
 */

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <sys/types.h>
#include <unistd.h>
#include <glib.h>
#include <string.h>
#ifdef __GLIBC__
#include <execinfo.h>
#endif
#ifdef __APPLE__
#include <dlfcn.h>
#endif

#include "error.h"

void fs_error_intl(int severity, char *file, int line, const char *kb, const char *fmt, ...)
{
    va_list argp;

    va_start(argp, fmt);
    char *msg = g_strdup_vprintf(fmt, argp);
    if (kb)
      fsp_log(severity, "%s:%d kb=%s %s", file, line, kb, msg);
    else 
      fsp_log(severity, "%s:%d %s", file, line, msg);
    g_free(msg);

#ifdef __GLIBC__
    if (severity == LOG_CRIT) {
	void *stack[256];
        int size = backtrace(stack, 256);
        char **symbols = backtrace_symbols (stack, size);
        for (int k = 1; k < size; ++k) {
	    fsp_log(severity, " %d: %s", k, symbols[k]);
        }
        free(symbols);
    }
#endif
#ifdef __APPLE__
    if (severity == LOG_CRIT) {
        Dl_info info;
        void **frame = __builtin_frame_address(0);
        void **bp = *frame;
        void *ip = frame[1];
        int f = 1;

        while (bp && ip && dladdr(ip, &info)) {
            fsp_log(severity, "% 2d: %p <%s+%u> %s", f++, ip, info.dli_sname, (unsigned int)(ip - info.dli_saddr), basename((char *)info.dli_fname));
            if (info.dli_sname && !strcmp(info.dli_sname, "main")) break;
            ip = bp[1];
            bp = bp[0];
        }
    }
#endif
}

/* vi:set ts=8 sts=4 sw=4: */
