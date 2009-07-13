
export avahi := $(shell pkg-config --exists avahi-client avahi-glib && echo avahi-client avahi-glib)

