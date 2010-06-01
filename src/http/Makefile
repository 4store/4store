include ../discovery.mk
include ../rev.mk
include ../darwin.mk

DEFINES := $(shell pkg-config rasqal --atleast-version=0.9.14 && echo "-DHAVE_LAQRS") $(shell pkg-config rasqal --atleast-version=0.9.16 && echo "-DHAVE_RASQAL_WORLD")
BINS = 4s-httpd

FRONTEND = ../frontend/query-cache.o ../frontend/query-datatypes.o ../frontend/query-data.o ../frontend/query.o ../frontend/optimiser.o ../frontend/order.o ../frontend/filter.o ../frontend/filter-datatypes.o ../frontend/decimal.o ../frontend/results.o ../frontend/import.o ../frontend/update.o

# PROFILE = -pg
CFLAGS = -std=gnu99 -Wall $(PROFILE) -g -O2 -I./ -I../ $(DEFINES) -DGIT_REV=\"$(gitrev)\" `pkg-config --cflags rasqal raptor glib-2.0 libxml-2.0 gthread-2.0`
LDFLAGS = $(ldfdarwin) $(ldflinux) $(PROFILE) `pkg-config --libs rasqal raptor glib-2.0 libxml-2.0 gthread-2.0 $(avahi)`

all: $(BINS) $(test_BINS)

install:
	mkdir -p $(DESTDIR)/usr/local/bin/
	install $(BINS) $(DESTDIR)/usr/local/bin/

clean:
	rm -f *.o $(BINS)
	rm -rf *.dSYM

4s-httpd: httpd.o ../common/lib4store.a $(FRONTEND) ../common/libsort.a ../libs/stemmer/libstemmer.o ../libs/double-metaphone/double_metaphone.o
	$(CC) $(LDFLAGS) -o $@ $^ `pcre-config --libs` ../libs/stemmer/libstemmer.o
