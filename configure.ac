#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.50])
AC_INIT([4store], m4_esyscmd([./version.sh]), [http://4store.org/support/], [4store])
AC_CONFIG_SRCDIR([src/backend/backend-intl.h])
AM_INIT_AUTOMAKE([1.7 std-options -Wall])
AM_CONFIG_HEADER(config.h)

# Checks for programs.
AC_PROG_AWK
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB

# Checks for libraries.
AC_SEARCH_LIBS([el_init], [edit], [have_libedit=yes], [have_libedit=no])
AC_CHECK_LIB([m], [sin])
AC_CHECK_LIB([ncurses], [initscr])
AC_CHECK_LIB([readline], [readline])
AC_CHECK_LIB([z], [deflateInit])
PKG_CHECK_MODULES([GLIB], glib-2.0 >= 2.2.0)
PKG_CHECK_MODULES([GTHREAD], gthread-2.0 >= 2.2.0)
PKG_CHECK_MODULES([LIBXML], libxml-2.0)
PKG_CHECK_MODULES([RAPTOR], raptor)
PKG_CHECK_MODULES([RASQAL], rasqal >= 0.9.14)
PKG_CHECK_MODULES([RASQAL], rasqal >= 0.9.16, AC_DEFINE(HAVE_RASQAL_WORLD, [], [Defined if we have rasqal world object]), [])
AC_CHECK_LIB([pcre], [pcre_compile])
AC_CHECK_PROG(WITH_PCRE_CONFIG, pcre-config, yes, no)
if test $WITH_PCRE_CONFIG = no; then
	AC_MSG_ERROR([Cannot find pcre-config])
fi
MDNS_CFLAGS=""
MDNS_LIBS=""
PKG_CHECK_MODULES([AVAHI], avahi, have_avahi=yes, have_avahi=no)
if test $have_avahi = no; then
	AC_CHECK_FUNC([DNSServiceRegister], [have_dns_sd=yes], [have_dns_sd=no])
	if test $have_dns_sd = no; then
		AC_MSG_WARN([No mDNS library found (avahi or dns-sd), cluster support disabled])
	else
		AC_DEFINE([USE_DNS_SD], [], [Defined if the dns-sd library is available])
	fi
else
	AC_DEFINE([USE_AVAHI], [], [Defined if the avahi library is available])
	MDNS_CFLAGS=AVAHI_CFLAGS
	MDNS_LIBS=AVAHI_LIBS
fi
AC_SUBST(MDNS_CFLAGS)
AC_SUBST(MDNS_LIBS)

GIT_REV='"\"$(shell git describe --tags --always)\""'
AC_SUBST(GIT_REV)

# Checks for header files.
AC_FUNC_ALLOCA
AC_CHECK_HEADERS([fcntl.h limits.h locale.h netdb.h stddef.h stdint.h stdlib.h string.h sys/file.h sys/mount.h sys/param.h sys/socket.h sys/time.h sys/vfs.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_STRTOD
AC_CHECK_FUNCS([atexit ftime ftruncate gethostname gettimeofday memmove mempcpy memset mkdir munmap select setlocale socket strcasecmp strchr strdup strerror strncasecmp strrchr strstr strtol strtoull])

AC_CONFIG_FILES([Makefile
                 man/Makefile
                 src/Makefile
                 src/backend/Makefile
                 src/common/Makefile
                 src/frontend/Makefile
                 src/http/Makefile
                 src/libs/Makefile
                 src/libs/double-metaphone/Makefile
                 src/libs/stemmer/Makefile
                 src/utilities/Makefile
                 tests/Makefile])
AC_OUTPUT
