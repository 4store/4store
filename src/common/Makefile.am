noinst_LIBRARIES = lib4sintl.a libsort.a

noinst_PROGRAMS = hashtest

nodist_pkginclude_HEADERS = 4store.h
#nodist_pkginclude_HEADERS = 4s-internals.h 4store.h datatypes.h error.h hash.h md5.h params.h rdf-constants.h rijndael-alg-fst.h sort.h timing.h umac.h gnu-options.h

noinst_HEADERS = 4s-internals.h 4store.h datatypes.h error.h hash.h md5.h params.h rdf-constants.h rijndael-alg-fst.h sort.h timing.h umac.h gnu-options.h 

AM_CFLAGS = -std=gnu99 -fno-strict-aliasing -Wall -g -O2 -I.. -DGIT_REV=@GIT_REV@ @GLIB_CFLAGS@ @MDNS_CFLAGS@
AM_LDFLAGS = @MDNS_LIBS@

hashtest_SOURCES = hashtest.c
hashtest_LDADD = lib4sintl.a @GLIB_LIBS@

lib4sintl_a_SOURCES = 4s-common.c 4s-client.c 4s-server.c 4s-mdns.c datatypes.c error.c umac.c rijndael-alg-fst.c md5.c hash.c

libsort_a_SOURCES = msort.c qsort.c

lib_LTLIBRARIES = lib4store.la 
lib4store_la_SOURCES = 4s-common.c 4s-client.c 4s-mdns.c datatypes.c error.c umac.c rijndael-alg-fst.c md5.c hash.c
lib4store_la_CFLAGS = $(AM_CFLAGS)
lib4store_la_LIBADD = @GLIB_LIBS@
