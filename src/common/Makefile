include ../discovery.mk
include ../rev.mk
include ../darwin.mk

LIB_OBJS = 4s-common.o 4s-client.o 4s-server.o 4s-mdns.o datatypes.o error.o umac.o rijndael-alg-fst.o md5.o hash.o

SORT_OBJS = msort.o qsort.o

TEST_OBJS = 4s-test.o

BINS = 4s-test

HEADERS = 4store.h 4s-internals.h

pkgs = glib-2.0 $(avahi)

ifdef avahi
  mdns_flags = -DUSE_AVAHI
endif
ifdef dnssd
  mdns_flags = -DUSE_DNS_SD
endif

CFLAGS = -std=gnu99 -fno-strict-aliasing -Wall -g -O2 -I../ -DGIT_REV=\"$(gitrev)\" $(mdns_flags) `pkg-config --cflags $(pkgs)`
LDFLAGS = $(ldfdarwin) $(lfdlinux)
LIBS = `pkg-config --libs $(pkgs)`

all: lib4store.a libsort.a

install:

4s-test: lib4store.a $(TEST_OBJS)
	$(CC) -o 4s-test $(TEST_OBJS) $(LIBS) lib4store.a

hashtest: lib4store.a hashtest.o
	$(CC) -o hashtest hashtest.o $(LIBS) lib4store.a

lib4store.a: $(LIB_OBJS) $(HEADERS)
	ar rvu lib4store.a $(LIB_OBJS)
	ranlib lib4store.a

libsort.a: $(SORT_OBJS) sort.h
	ar rvu libsort.a $(SORT_OBJS)
	ranlib libsort.a

clean:
	rm -f *.o lib4store.a libsort.a $(BINS) hashtest
	rm -rf *.dSYM
