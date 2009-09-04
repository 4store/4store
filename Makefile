all clean:
	(cd src && $(MAKE) -w $@)

install:
	(cd src && $(MAKE) -w $@)
	(cd man && $(MAKE) -w $@)

test:
	(cd tests && $(MAKE) -w $@)

macapp:
	rm -f 4store.app/MacOS/{bin,lib}/*
	for i in src/frontend/4s-* src/backend/4s-* src/utilities/4s-backend-*; do \
		if test -x $$i ; then \
			cp $$i 4store.app/MacOS/bin/ ; \
		fi \
	done
	cp -p /usr/local/lib/librasqal*.dylib /usr/local/lib/libraptor*.dylib \
	/sw/lib/libglib*.dylib /sw/lib/libintl*.dylib /sw/lib/libiconv*.dylib \
	/sw/lib/libxml*.dylib /sw/lib/libpcre*.dylib	/sw/lib/libintl*.dylib \
	/sw/lib/libiconv*.dylib /sw/lib/libxml*.dylib \
	/sw/lib/libpcre*.dylib /sw/lib/libreadline*.dylib \
	/sw/lib/ncurses/libncurses*.dylib \
	4store.app/MacOS/lib/
