AM_CFLAGS = -Wall -g -std=gnu99 -I.. -DGIT_REV=@GIT_REV@ @GLIB_CFLAGS@
AM_LDFLAGS = -lz @GLIB_LIBS@ @RAPTOR_LIBS@

bin_PROGRAMS = 4s-backend-setup 4s-backend-destroy 4s-backend-info 4s-backend-copy 4s-backend-passwd

dist_bin_SCRIPTS = 4s-ssh-all 4s-ssh-all-parallel \
 4s-cluster-create 4s-cluster-destroy 4s-cluster-start 4s-cluster-stop \
 4s-cluster-info 4s-dump 4s-restore 4s-file-backup 4s-cluster-file-backup \
 4s-cluster-copy

EXTRA_DIST = 4s-cluster-cache 4s-ssh-keyscan

noinst_PROGRAMS = lex-file-verify 4s-rid

install-data-local: 
	mkdir -p $(DESTDIR)@FS_STORE_ROOT@
	chmod 1777 $(DESTDIR)@FS_STORE_ROOT@

4s_backend_destroy_SOURCES = backend-destroy.c
4s_backend_destroy_LDADD = ../common/lib4sintl.a

4s_backend_copy_SOURCES = backend-copy.c
4s_backend_copy_LDADD = ../common/lib4sintl.a ../backend/metadata.o ../common/datatypes.o 

4s_backend_setup_SOURCES = backend-setup.c ../common/timing.c
4s_backend_setup_LDADD = ../backend/backend.o ../backend/lib4storage.a ../common/lib4sintl.a @MDNS_LIBS@

4s_rid_SOURCES = 4s-rid.c
4s_rid_LDADD = ../common/lib4sintl.a

4s_backend_info_SOURCES = backend-info.c ../common/timing.c ../common/gnu-options.c
4s_backend_info_LDADD = ../backend/backend.o ../backend/lib4storage.a ../common/lib4sintl.a

4s_backend_passwd_SOURCES = passwd.c ../common/gnu-options.c
4s_backend_passwd_LDADD = ../backend/backend.o ../backend/lib4storage.a ../common/lib4sintl.a
