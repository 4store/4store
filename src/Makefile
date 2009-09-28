SUBDIRS = libs common backend frontend http utilities

all clean install:
	for i in $(SUBDIRS); do \
		(cd $$i && $(MAKE) -w $@) || break; \
	done
