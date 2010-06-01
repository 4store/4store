include ../discovery.mk
include ../rev.mk
include ../darwin.mk
include ../readedit.mk

DEFINES := $(shell pkg-config rasqal --atleast-version=0.9.14 && echo "-DHAVE_LAQRS") $(shell pkg-config rasqal --atleast-version=0.9.16 && echo "-DHAVE_RASQAL_WORLD")
BINS = 4s-query 4s-import 4s-delete-model 4s-size 4s-info 4s-update
test_BINS = filter-test decimal-test 4s-bind 4s-reverse-bind 4s-resolve

ifdef readedit
  readedit_cflags = -DUSE_LIBEDIT
  readedit_ldflags = -ledit -lncurses
else
  readedit_ldflags = -lreadline -lncurses
endif

# PROFILE = -pg
CFLAGS = -std=gnu99 -fno-strict-aliasing -Wall $(PROFILE) -g -O2 -I./ -I../ $(DEFINES) $(readedit_cflags) -DGIT_REV=\"$(gitrev)\" `pkg-config --cflags rasqal raptor glib-2.0 libxml-2.0` `pcre-config --cflags`
LDFLAGS = $(ldfdarwin) $(ldflinux) $(PROFILE) `pkg-config --libs rasqal raptor glib-2.0 libxml-2.0 $(avahi)` `pcre-config --libs`

all: $(BINS) $(test_BINS)

install:
	mkdir -p $(DESTDIR)/usr/local/bin/
	install $(BINS) $(DESTDIR)/usr/local/bin/

clean:
	rm -f *.o $(BINS) $(test_BINS)
	rm -rf *.dSYM

test: all filter-test
	@echo 'FILTER tests'
	@./filter-test | grep ===
	@echo 'Query tests'
	@./tests/run.pl

4s-query: 4s-query.o query.o results.o query-data.o query-datatypes.o query-cache.o filter.o filter-datatypes.o order.o optimiser.o decimal.o ../common/lib4store.a ../common/libsort.a
	$(CC) $(LDFLAGS) $(readedit_ldflags) -o 4s-query $^

4s-update: 4s-update.o update.o import.o ../common/lib4store.a ../libs/stemmer/libstemmer.o ../libs/double-metaphone/double_metaphone.o
	$(CC) $(LDFLAGS) $(readedit_ldflags) -o 4s-update $^

decimal.o: decimal.h

query.o: debug.h

results.o: debug.h

order.o: debug.h

query-datatypes.o: debug.h

optimiser.o: debug.h

4s-import: 4s-import.o import.o ../common/lib4store.a ../libs/stemmer/libstemmer.o ../libs/double-metaphone/double_metaphone.o ../common/libsort.a
	$(CC) $(LDFLAGS) -o 4s-import $^

4s-delete-model: delete-model.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-delete-model $^

4s-size: size.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-size $^

4s-info: 4s-info.o query.o query-datatypes.o query-data.o query-cache.o order.o optimiser.o filter.o filter-datatypes.o results.o decimal.o ../common/lib4store.a ../common/libsort.a
	$(CC) $(LDFLAGS) -o 4s-info $^

4s-restore: restore.o restore-trix.o ../common/lib4store.a ../common/libsort.a
	$(CC) $(LDFLAGS) -o 4s-restore $^

4s-resolve: 4s-resolve.o ../common/lib4store.a ../common/libsort.a

4s-bind: 4s-bind.o ../common/lib4store.a ../common/libsort.a

4s-reverse-bind: 4s-reverse-bind.o ../common/lib4store.a ../common/libsort.a

4s-dump: dump.o ../common/lib4store.a ../common/libsort.a
	$(CC) $(LDFLAGS) -o 4s-dump $^

4s-indexes: 4s-indexes.o ../common/lib4store.a ../common/libsort.a
	$(CC) $(LDFLAGS) -o $@ $^

filter-test.o: filter-test.c
	cc -c -std=gnu99 -Wall $(PROFILE) -g -I../ `pkg-config --cflags rasqal glib-2.0` `pcre-config --cflags` $^

filter-test: filter-test.o filter.o filter-datatypes.o query-data.o decimal.o ../common/lib4store.a ../common/libsort.a

decimal-test: decimal-test.o decimal.o
