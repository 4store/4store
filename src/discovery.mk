
export avahi := $(shell pkg-config --exists avahi-client avahi-glib && echo avahi-client avahi-glib)
ifndef avahi
	export dnssd := $(shell test -e /usr/include/dns_sd.h && echo yes)
endif
