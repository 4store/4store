include ../discovery.mk
include ../rev.mk
include ../darwin.mk

CFLAGS = -Wall -g -std=gnu99 -O2 -I.. -DGIT_REV=\"$(gitrev)\" `pkg-config --cflags raptor glib-2.0`
LDFLAGS = $(ldfdarwin) $(ldflinux) -lz `pkg-config --libs raptor glib-2.0 $(avahi)`

LIB_OBJS = chain.o bucket.o list.o tlist.o rhash.o mhash.o sort.o \
	   lock.o metadata.o disk-space.o ptree.o ptable.o tbchain.o prefix-trie.o
HEADERS = tree.h chain.h bucket.h list.h sort.h lock.h backend-intl.h \
	  metadata.h tlist.h tbchain.h prefix-trie.h
BINS = 4s-backend
TESTS = bctest bcdump listdump rhashtest rhashdump mhashtest mhashdump ptreetest ptreedump ptreebind ptabletest tbchaintest tbchaindump listtest prefix-trie-test

all: $(BINS) $(TESTS)

clean: 
	rm -f $(BINS) $(TESTS) lib4storage.a *.o
	rm -rf *.dSYM

install:
	mkdir -p $(DESTDIR)/usr/local/bin/
	install $(BINS) $(DESTDIR)/usr/local/bin/

test: all
	@mkdir -p /tmp/tstest/
	@./qbtest > /tmp/tstest/qbtest.txt
	@diff /tmp/tstest/qbtest.txt exemplar/qbtest.txt && echo "PASS" || echo "FAIL"

ibtest: ibtest.o backend.o import-backend.o query-backend.o ../common/lib4store.a

4s-backend: server.o backend.o import-backend.o query-backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-backend $^

bctest: bctest.c ../common/timing.o lib4storage.a ../common/lib4store.a
treetest: treetest.c backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
revindextest: revindextest.o backend.o query-backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
rhashtest: rhashtest.c backend.o query-backend.o lib4storage.a ../common/lib4store.a
mhashtest: mhashtest.c backend.o query-backend.o lib4storage.a ../common/lib4store.a
ptreetest: ptreetest.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
ptabletest: ptabletest.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
tbchaintest: tbchaintest.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
listtest: listtest.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
prefix-trie-test: prefix-trie.o prefix-trie-test.o ../common/lib4store.a

bcdump: bcdump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
treedump: treedump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
listdump: listdump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
resolvetest: resolvetest.o backend.o ../common/timing.o query-backend.o lib4storage.a ../common/lib4store.a
rhashdump: rhashdump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
mhashdump: mhashdump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
ptreedump: ptreedump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
ptreebind: ptreebind.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a
tbchaindump: tbchaindump.o backend.o ../common/timing.o lib4storage.a ../common/lib4store.a

lib4storage.a: $(LIB_OBJS) $(HEADERS)
	ar rvu lib4storage.a $(LIB_OBJS)
	ranlib lib4storage.a
