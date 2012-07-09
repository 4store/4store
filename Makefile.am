SUBDIRS = src man tests

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = 4store-0.pc

test: all
	(cd tests && $(MAKE) -w $@)

macapp: all
	app-aux/build-static.pl
	app-aux/build-app.sh

macdmg: macapp
	app-aux/build-dmg.sh

dist-hook:
	git describe --tags > $(distdir)/.version

EXTRA_DIST = version.sh make-tarball.sh 4store.spec autogen.sh data/config-test.ttl data/fulltext-test.ttl data/illegal.ttl data/lengthy.nt data/nasty.ttl data/swh.xrdf data/tiger data/tiger/README data/tiger/TGR06001.nt data/tiger/tiger.pl data/tiger/tiger_features.rdf data/tiger/tiger_fipsad.rdf data/tiger/tiger_fipscc.rdf data/tiger/tiger_fipspd.rdf data/tiger/tiger_vocab.rdf data/numbers.ttl data/dates.ttl data/loves-hates.ttl data/group-by.ttl 4store-0.pc.in
