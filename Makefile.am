SUBDIRS = src man tests

test:
	(cd tests && $(MAKE) -w $@)

macapp: all
	app-aux/build-app.sh

macdmg: macapp
	app-aux/build-dmg.sh

EXTRA_DIST = version.sh make-tarball.sh 4store.spec autogen.sh data/config-test.ttl data/fulltext-test.ttl data/illegal.ttl data/lengthy.nt data/nasty.ttl data/swh.xrdf data/tiger data/tiger/README data/tiger/TGR06001.nt data/tiger/tiger.pl data/tiger/tiger_features.rdf data/tiger/tiger_fipsad.rdf data/tiger/tiger_fipscc.rdf data/tiger/tiger_fipspd.rdf data/tiger/tiger_vocab.rdf
