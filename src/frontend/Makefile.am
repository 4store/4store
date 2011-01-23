bin_PROGRAMS = 4s-query 4s-import 4s-delete-model 4s-size 4s-info 4s-update

noinst_PROGRAMS = filter-test decimal-test 4s-bind 4s-reverse-bind 4s-resolve 4s-dump 4s-restore

noinst_HEADERS = debug.h decimal.h filter-datatypes.h filter.h import.h optimiser.h order.h query-cache.h query-data.h query-datatypes.h query-intl.h query.h results.h update.h group.h

# PROFILE = -pg
AM_CFLAGS = -std=gnu99 -fno-strict-aliasing -Wall $(PROFILE) -g -O2 -I./ -I../ -DGIT_REV=@GIT_REV@ @GLIB_CFLAGS@ @RAPTOR_CFLAGS@ @RASQAL_CFLAGS@ @LIBXML_CFLAGS@ `pcre-config --cflags`
AM_LDFLAGS = $(PROFILE) @GLIB_LIBS@ `pcre-config --libs`

test: all filter-test
	@echo 'FILTER tests'
	@./filter-test | grep ===
	@echo 'Query tests'
	@./tests/run.pl

4s_query_SOURCES = 4s-query.c query.c results.c query-data.c query-datatypes.c query-cache.c filter.c filter-datatypes.c order.c group.c optimiser.c decimal.c
4s_query_LDADD = ../common/lib4store.a ../common/libsort.a @RAPTOR_LIBS@ @RASQAL_LIBS@ @MDNS_LIBS@

4s_update_SOURCES = 4s-update.c update.c import.c ../common/gnu-options.c query.c results.c query-data.c query-datatypes.c query-cache.c filter.c filter-datatypes.c order.c group.c optimiser.c decimal.c
4s_update_LDADD = ../common/lib4store.a ../common/libsort.a ../libs/stemmer/libstemmer.a ../libs/double-metaphone/libdouble_metaphone.a @RAPTOR_LIBS@ @RASQAL_LIBS@ @MDNS_LIBS@

4s_import_SOURCES = 4s-import.c import.c
4s_import_LDADD = ../common/lib4store.a ../libs/stemmer/libstemmer.a ../libs/double-metaphone/libdouble_metaphone.a ../common/libsort.a @RAPTOR_LIBS@ @MDNS_LIBS@

4s_delete_model_SOURCES = delete-model.c ../common/gnu-options.c
4s_delete_model_LDADD = ../common/lib4store.a @MDNS_LIBS@

4s_size_SOURCES = size.c ../common/gnu-options.c
4s_size_LDADD = ../common/lib4store.a -lm @MDNS_LIBS@

4s_info_SOURCES = 4s-info.c query.c query-datatypes.c query-data.c query-cache.c order.c group.c optimiser.c filter.c filter-datatypes.c results.c decimal.c ../common/gnu-options.c
4s_info_LDADD = ../common/lib4store.a ../common/libsort.a @RASQAL_LIBS@ @MDNS_LIBS@

4s_restore_SOURCES = restore.c restore-trix.c
4s_restore_LDADD = ../common/lib4store.a ../common/libsort.a @LIBXML_LIBS@ @MDNS_LIBS@

4s_resolve_SOURCES = 4s-resolve.c
4s_resolve_LDADD = ../common/lib4store.a ../common/libsort.a @MDNS_LIBS@

4s_bind_SOURCES = 4s-bind.c
4s_bind_LDADD = ../common/lib4store.a ../common/libsort.a @MDNS_LIBS@

4s_reverse_bind_SOURCES = 4s-reverse-bind.c
4s_reverse_bind_LDADD = ../common/lib4store.a ../common/libsort.a @MDNS_LIBS@

4s_dump_SOURCES = dump.c
4s_dump_LDADD = ../common/lib4store.a ../common/libsort.a @LIBXML_LIBS@ @MDNS_LIBS@

filter_test_SOURCES = filter-test.c filter.c filter-datatypes.c query-data.c decimal.c results.c query.c query-datatypes.c query-cache.c order.c group.c optimiser.c
filter_test_LDADD = ../common/lib4store.a ../common/libsort.a @MDNS_LIBS@ @RASQAL_LIBS@

decimal_test_SOURCES = decimal-test.c decimal.c
