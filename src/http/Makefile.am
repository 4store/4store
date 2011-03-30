bin_PROGRAMS = 4s-httpd

noinst_HEADERS = httpd.h

FRONTEND = ../frontend/query-cache.o ../frontend/query-datatypes.o ../frontend/query-data.o ../frontend/query.o ../frontend/optimiser.o ../frontend/order.o ../frontend/filter.o ../frontend/filter-datatypes.o ../frontend/decimal.o ../frontend/results.o ../frontend/import.o ../frontend/update.o ../frontend/group.o

# PROFILE = -pg
AM_CFLAGS = -std=gnu99 -Wall $(PROFILE) -g -O2 -I./ -I../ -DGIT_REV=@GIT_REV@ @RASQAL_CFLAGS@ @RAPTOR_CFLAGS@ @GLIB_CFLAGS@ @LIBXML_CFLAGS@ @GTHREAD_CFLAGS@ @MDNS_CFLAGS@ `pcre-config --cflags`
AM_LDFLAGS = $(PROFILE) @RASQAL_LIBS@ @RAPTOR_LIBS@ @GLIB_LIBS@ @LIBXML_LIBS@ @GTHREAD_LIBS@ @MDNS_LIBS@ `pcre-config --libs`

4s_httpd_SOURCES = httpd.c ../common/gnu-options.c
4s_httpd_LDADD = ../common/lib4sintl.a $(FRONTEND) ../common/libsort.a ../libs/stemmer/libstemmer.a ../libs/double-metaphone/libdouble_metaphone.a
