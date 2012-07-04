#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.50])
AC_INIT([4store], m4_esyscmd([./version.sh .version]), [http://4store.org/support/], [4store])
AC_CONFIG_SRCDIR([src/backend/backend-intl.h])
AM_INIT_AUTOMAKE([1.7 std-options -Wall])
AM_CONFIG_HEADER(4store-config.h)

# Checks for programs.
AC_PROG_LIBTOOL
AC_PROG_AWK
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_CHECK_PROGS(PKG_CONFIG, pkg-config)
if test "X$PKG_CONFIG" = X; then
	AC_MSG_ERROR([Cannot find pkg-config, you need this to build])
fi

AC_ARG_WITH([storage_path],
	[AS_HELP_STRING([--with-storage-path=PATH],
	                [set directory used to store indexes default=/var/lib/4store])],
	[],
	[with_storage_path=/var/lib/4store])
FS_STORE_ROOT="$with_storage_path"
AC_SUBST(FS_STORE_ROOT)

AC_ARG_WITH([config_file],
	[AS_HELP_STRING([--with-config-file=PATH],
	                [set path and filename of runtime configuration file default=/etc/4store.conf])],
	[],
	[with_config_file=/etc/4store.conf])
FS_CONFIG_FILE="$with_config_file"
AC_SUBST(FS_CONFIG_FILE)

AC_ARG_WITH([skolem_prefix],
	[AS_HELP_STRING([--with-skolem-prefix=URI],
	                [set skolemisation prefix, default=http://4store.org/.well-known/genid])],
	[],
	[with_skolem_prefix=http://4store.org/.well-known/genid])
FS_SKOLEM_PREFIX="$with_skolem_prefix"
AC_SUBST(FS_SKOLEM_PREFIX)

FS_SKOLEMIZE=0
AC_ARG_ENABLE([skolemization],
	[AS_HELP_STRING([--enable-skolemization],
                        [skolemise bNodes when emitted from store])],
	[FS_SKOLEMIZE=1])
AC_SUBST(FS_SKOLEMIZE)

FS_INSERT_DEDUP=0
AC_ARG_ENABLE([dedup-insert],
	[AS_HELP_STRING([--enable-dedup-insert],
                        [don't allow duplicate triples when inserting data])],
	[FS_INSERT_DEDUP=1])
AC_SUBST(FS_INSERT_DEDUP)

FS_DISABLE_PREFIXES=0
AC_ARG_ENABLE([no-prefixes],
	[AS_HELP_STRING([--enable-no-prefixes],
                        [don't compress common URI prefixes])],
	[FS_DISABLE_PREFIXES=1])
AC_SUBST(FS_DISABLE_PREFIXES)

# Checks for libraries.
AC_CHECK_LIB([m], [sin])
AC_CHECK_LIB([ncurses], [initscr])
AC_CHECK_LIB([readline], [readline])
AC_CHECK_LIB([z], [compress])
AC_SEARCH_LIBS([uuid_generate], [uuid])
PKG_CHECK_MODULES([GLIB], glib-2.0 >= 2.2.0)
PKG_CHECK_MODULES([GTHREAD], gthread-2.0 >= 2.2.0)
PKG_CHECK_MODULES([LIBXML], libxml-2.0)
PKG_CHECK_MODULES([RAPTOR], raptor2)
PKG_CHECK_MODULES([RASQAL], rasqal >= 0.9.22)
AC_CHECK_LIB([pcre], [pcre_compile])
AC_CHECK_PROG(WITH_PCRE_CONFIG, pcre-config, yes, no)
if test $WITH_PCRE_CONFIG = no; then
	AC_MSG_ERROR([Cannot find pcre-config])
fi
MDNS_CFLAGS=""
MDNS_LIBS=""
PKG_CHECK_MODULES([AVAHI], avahi-glib, have_avahi=yes, have_avahi=no)
if test $have_avahi = no; then
	AC_CHECK_FUNC([DNSServiceRegister], [have_dns_sd=yes], [have_dns_sd=no])
	if test $have_dns_sd = no; then
		AC_MSG_WARN([No mDNS library found (avahi or dns-sd), cluster support available through 4s-boss])
	else
		AC_DEFINE([USE_DNS_SD], [], [Defined if the dns-sd library is available])
	fi
else
	AC_DEFINE([USE_AVAHI], [], [Defined if the avahi library is available])
	PKG_CHECK_MODULES([AVAHIC], avahi-client)
	MDNS_CFLAGS="$AVAHI_CFLAGS $AVAHIC_CFLAGS"
	MDNS_LIBS="$AVAHI_LIBS $AVAHIC_LIBS"
fi
AC_SUBST(MDNS_CFLAGS)
AC_SUBST(MDNS_LIBS)

GIT_REV='"\"$(shell sh $(top_srcdir)/version.sh $(top_srcdir)/.version)\""'
AC_SUBST(GIT_REV)

# Checks for header files.
AC_FUNC_ALLOCA
AC_CHECK_HEADER(uuid/uuid.h,,AC_MSG_ERROR([cannot find UUID header]))
AC_CHECK_HEADERS([fcntl.h limits.h locale.h netdb.h stddef.h stdint.h stdlib.h string.h sys/file.h sys/mount.h sys/param.h sys/socket.h sys/time.h sys/vfs.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_STRTOD
AC_CHECK_FUNCS([atexit ftime ftruncate gethostname gettimeofday memmove mempcpy memset mkdir munmap select setlocale socket strcasecmp strchr strdup strerror strncasecmp strrchr strstr strtol strtoull])

oLIBS=$LIBS
LIBS="$GLIB_LIBS"
AC_CHECK_FUNCS([g_uri_escape_string])
LIBS=$oLIBS

AC_TRY_COMPILE(
    [#include <uuid/uuid.h>],
    [uuid_t uu; uuid_string_t uus; uuid_generate(uu); uuid_unparse(uu, uus);],
    have_uuid_string_t=yes,
    have_uuid_string_t=no
)

UUID_LIBS=""
if test "$have_uuid_string_t" = yes; then
    AC_DEFINE(HAVE_UUID_STRING_T, 1, [define to 1 if uuid_string_t used])
else
    UUID_LIBS="-luuid"
fi
AC_SUBST(UUID_LIBS)

AC_CONFIG_FILES([Makefile
                 man/Makefile
                 tests/Makefile
                 src/Makefile
                 src/backend/Makefile
                 src/common/Makefile
                 src/admin/Makefile
                 src/frontend/Makefile
                 src/http/Makefile
                 src/libs/Makefile
                 src/libs/double-metaphone/Makefile
                 src/libs/stemmer/Makefile
                 src/libs/mt19937-64/Makefile
                 src/utilities/Makefile
                 src/common/params.h
                 4store-0.pc])
AC_OUTPUT

raptor_version=`$PKG_CONFIG raptor2 --modversion 2>/dev/null`
rasqal_version=`$PKG_CONFIG rasqal --modversion 2>/dev/null`

echo
echo "Configuration status:"

if test "X$have_avahi" = Xno -a "X$have_dns_sd" = Xno; then
	echo "   Cluster support available through 4s-boss"
	echo "   Install Avahi (-glib and -client) to get mDNS support on Linux"
else
	echo "   Cluster support available through mDNS or 4s-boss"
fi
echo "   Using Raptor version $raptor_version"
echo "   Using Rasqal version $rasqal_version"
