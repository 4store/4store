all clean:
	(cd src && $(MAKE) -w $@)

install:
	(cd src && $(MAKE) -w $@)
	(cd man && $(MAKE) -w $@)

test:
	(cd tests && $(MAKE) -w $@)
