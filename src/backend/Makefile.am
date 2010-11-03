AM_CFLAGS = -Wall -g -std=gnu99 -O2 -I.. -DGIT_REV=@GIT_REV@ @RAPTOR_CFLAGS@ @GLIB_CFLAGS@ @MDNS_CFLAGS@
AM_LDFLAGS = -lz @RAPTOR_LIBS@ @GLIB_LIBS@ @MDNS_LIBS@

bin_PROGRAMS = 4s-backend

noinst_PROGRAMS = bctest bcdump listdump rhashtest rhashdump mhashtest mhashdump ptreetest ptreedump ptreebind ptabletest tbchaintest tbchaindump listtest prefix-trie-test treedump treetest

noinst_LIBRARIES = lib4storage.a

noinst_HEADERS = backend-intl.h backend.h bucket.h chain.h disk-space.h import-backend.h list.h lock.h metadata.h mhash.h prefix-trie.h ptable.h ptree.h query-backend.h rhash.h sort.h tbchain.h tlist.h tree-intl.h tree.h

LIB_OBJS = chain.o bucket.o list.o tlist.o rhash.o mhash.o sort.o \
	   lock.o metadata.o disk-space.o ptree.o ptable.o tbchain.o prefix-trie.o

test: all
	@mkdir -p /tmp/tstest/
	@./qbtest > /tmp/tstest/qbtest.txt
	@diff /tmp/tstest/qbtest.txt exemplar/qbtest.txt && echo "PASS" || echo "FAIL"

4s_backend_SOURCES = server.c backend.c import-backend.c query-backend.c ../common/timing.c
4s_backend_LDADD = lib4storage.a ../common/lib4store.a @MDNS_LIBS@

bctest_SOURCES = bctest.c ../common/timing.c
bctest_LDADD = lib4storage.a ../common/lib4store.a

treetest_SOURCES = treetest.c backend.c tree.c ../common/timing.c
treetest_LDADD = lib4storage.a ../common/lib4store.a

rhashtest_SOURCES = rhashtest.c backend.c query-backend.c
rhashtest_LDADD = lib4storage.a ../common/lib4store.a

mhashtest_SOURCES = mhashtest.c backend.c query-backend.c
mhashtest_LDADD = lib4storage.a ../common/lib4store.a

ptreetest_SOURCES = ptreetest.c backend.c ../common/timing.c
ptreetest_LDADD = lib4storage.a ../common/lib4store.a

ptabletest_SOURCES = ptabletest.c backend.c ../common/timing.c
ptabletest_LDADD = lib4storage.a ../common/lib4store.a

tbchaintest_SOURCES =  tbchaintest.c backend.c ../common/timing.c
tbchaintest_LDADD = lib4storage.a ../common/lib4store.a

listtest_SOURCES = listtest.c backend.c ../common/timing.c
listtest_LDADD = lib4storage.a ../common/lib4store.a

prefix_trie_test_SOURCES = prefix-trie.c prefix-trie-test.c
prefix_trie_test_LDADD = ../common/lib4store.a

bcdump_SOURCES = bcdump.c backend.c ../common/timing.c
bcdump_LDADD = lib4storage.a ../common/lib4store.a

treedump_SOURCES = treedump.c tree.c backend.c ../common/timing.c
treedump_LDADD = lib4storage.a ../common/lib4store.a

listdump_SOURCES = listdump.c backend.c ../common/timing.c
listdump_LDADD = lib4storage.a ../common/lib4store.a

rhashdump_SOURCES = rhashdump.c backend.c ../common/timing.c
rhashdump_LDADD = lib4storage.a ../common/lib4store.a

mhashdump_SOURCES = mhashdump.c backend.c ../common/timing.c
mhashdump_LDADD = lib4storage.a ../common/lib4store.a

ptreedump_SOURCES = ptreedump.c backend.c ../common/timing.c
ptreedump_LDADD = lib4storage.a ../common/lib4store.a

ptreebind_SOURCES = ptreebind.c backend.c ../common/timing.c
ptreebind_LDADD = lib4storage.a ../common/lib4store.a

tbchaindump_SOURCES = tbchaindump.c backend.c ../common/timing.c
tbchaindump_LDADD = lib4storage.a ../common/lib4store.a

lib4storage_a_SOURCES = chain.c bucket.c list.c tlist.c rhash.c mhash.c sort.c lock.c metadata.c disk-space.c ptree.c ptable.c tbchain.c prefix-trie.c
