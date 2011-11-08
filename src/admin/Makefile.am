bin_PROGRAMS = 4s-boss 4s-admin

noinst_HEADERS = admin_backend.h admin_common.h admin_frontend.h admin_protocol.h

AM_CFLAGS = -std=gnu99 -Wall $(PROFILE) -g -O2 -I./ -I../ -DGIT_REV=@GIT_REV@ @RASQAL_CFLAGS@ @RAPTOR_CFLAGS@ @GLIB_CFLAGS@ @LIBXML_CFLAGS@ @GTHREAD_CFLAGS@ @MDNS_CFLAGS@ `pcre-config --cflags` -DFS_BIN_DIR=\"$(bindir)\"
AM_LDFLAGS = $(PROFILE) @RASQAL_LIBS@ @RAPTOR_LIBS@ @GLIB_LIBS@ @LIBXML_LIBS@ @GTHREAD_LIBS@ @MDNS_LIBS@ `pcre-config --libs`

4s_admin_SOURCES = admin.c
4s_admin_LDADD = ../common/lib4sintl.a 

4s_boss_SOURCES = admind.c admin_backend.c
4s_boss_LDADD = ../common/lib4sintl.a ../backend/metadata.o
