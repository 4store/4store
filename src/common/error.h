#ifndef ERROR_H
#define ERROR_H

#include "4store.h"

#define fs_error(s, f...) (fs_error_intl(s, __FILE__, __LINE__, NULL, f))
#define link_error(s, f...) (fs_error_intl(s, __FILE__, __LINE__, link->kb_name, f))

void fs_error_intl(int severity, char *file, int line, const char *kb, const char *fmt, ...)
                                   __attribute__ ((format(printf, 5, 6)));

#endif
