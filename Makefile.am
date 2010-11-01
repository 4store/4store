SUBDIRS = src man tests

test:
	(cd tests && $(MAKE) -w $@)

macapp: all
	app-aux/build-app.sh

macdmg: macapp
	app-aux/build-dmg.sh
