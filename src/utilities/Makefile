include ../rev.mk
include ../darwin.mk

CFLAGS = -Wall -g -std=gnu99 -I.. -DGIT_REV=\"$(gitrev)\" `pkg-config --cflags glib-2.0`
LDFLAGS = $(ldfdarwin) $(ldflinux) -lz `pkg-config --libs glib-2.0 raptor` -lreadline -lncurses

BINS = 4s-backend-setup 4s-backend-destroy 4s-backend-info 4s-backend-copy \
 4s-backend-passwd 4s-rid
SCRIPTS = 4s-ssh-all 4s-ssh-all-parallel \
 4s-cluster-create 4s-cluster-destroy 4s-cluster-start 4s-cluster-stop 4s-cluster-info 4s-cluster-cache \
 4s-file-backup 4s-cluster-file-backup 4s-cluster-copy 4s-ssh-keyscan
TESTS = lex-file-verify

all: $(BINS) $(TESTS)

install: $(BINS)
	mkdir -p $(DESTDIR)/var/lib/4store
	chmod 1777 $(DESTDIR)/var/lib/4store
	mkdir -p $(DESTDIR)/usr/local/bin/
	install $(BINS) $(DESTDIR)/usr/local/bin/
	install $(SCRIPTS) $(DESTDIR)/usr/local/bin/

clean: 
	rm -f $(BINS) $(TESTS) *.o
	rm -rf *.dSYM

4s-backend-destroy: backend-destroy.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-backend-destroy $^

4s-backend-copy: backend-copy.o ../common/lib4store.a ../backend/metadata.o ../common/datatypes.o 
	$(CC) $(LDFLAGS) -o 4s-backend-copy $^

4s-backend-setup: backend-setup.o ../backend/backend.o ../backend/lib4storage.a ../common/timing.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-backend-setup $^

4s-rid: 4s-rid.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-rid $^

4s-backend-info: backend-info.o ../backend/backend.o ../backend/lib4storage.a ../common/timing.o ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-backend-info $^

4s-backend-passwd: passwd.o ../backend/backend.o ../backend/lib4storage.a ../common/lib4store.a
	$(CC) $(LDFLAGS) -o 4s-backend-passwd $^

