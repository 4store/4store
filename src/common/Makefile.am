noinst_LIBRARIES = lib4store.a libsort.a

noinst_PROGRAMS = hashtest

AM_CFLAGS = -std=gnu99 -fno-strict-aliasing -Wall -g -O2 -I../ -DGIT_REV=@GIT_REV@ @GLIB_CFLAGS@

hashtest_SOURCES = hashtest.c
hashtest_LDADD = lib4store.a @GLIB_LIBS@

lib4store_a_SOURCES = 4s-common.c 4s-client.c 4s-server.c 4s-mdns.c datatypes.c error.c umac.c rijndael-alg-fst.c md5.c hash.c

libsort_a_SOURCES = msort.c qsort.c
