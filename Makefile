all clean:
	(cd src && $(MAKE) -w $@)

install:
	(cd src && $(MAKE) -w $@)
	(cd man && $(MAKE) -w $@)

test:
	(cd tests && $(MAKE) -w $@)

macapp: all
	app-aux/build-app.sh

macdmg: macapp
	app-aux/build-dmg.sh
