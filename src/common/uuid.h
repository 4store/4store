#ifndef FS_UUID_H
#define FS_UUID_H

#if !defined(IN_CONFIGURE)
# include <4store-config.h>
#endif

#if !defined(HAVE_UUID_STRING_T)
#define HAVE_UUID_STRING_T 1
typedef char uuid_string_t[128];
#endif

#if defined(HAVE_UUID_UUID_H)
# include <uuid/uuid.h>
#elif defined(HAVE_OSSP_UUID_H)
# include <ossp/uuid.h>
#else
# include <uuid.h>
#endif

#endif
