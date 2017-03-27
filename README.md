# 4store

4store is an efficient, scalable and stable RDF database.

4store was designed by Steve Harris and developed at Garlik to underpin their
Semantic Web applications. It has been providing the base platform for around 3
years. At times holding and running queries over databases of 15GT, supporting a
Web application used by thousands of people.

## Getting started

In this section:

1. Installing prerequisites.
2. Installing 4store.
3. Running 4store.
4. Installing frontend tools only.
5. Other installation hints.

### Installing prerequisites

To install [Raptor](https://github.com/dajobe/raptor) (RDF parser) and
[Rasqal](https://github.com/dajobe/rasqal) (SPARQL parser):

    # install a 64-bit raptor from freshly extracted source
    ./configure --libdir=/usr/local/lib64 && make
    sudo make install

    # similarly for 64-bit rasqal
    ./configure "--enable-query-languages=laqrs sparql rdql" \
     --libdir=/usr/local/lib64 && make
    sudo make install

    # ensure PKG_CONFIG_PATH is set correctly
    # ensure /etc/ld.so.conf.d/ includes /usr/local/lib64
    sudo ldconfig

### Installing 4store

    ./autogen.sh
    ./configure
    make
    make install

### Running 4store

    /usr/local/bin/4s-boss -D

### Installing frontend tools only

To install just the frontend tools on non-cluster frontends:

    # pre-requisites for installing the frontend tools
    yum install pcre-devel avahi avahi-tools avahi-devel

    # src/common
    (cd src/common && make)

    # src/frontend
    (cd src/frontend && make && make install)


### Other installation hints

Make sure `/var/lib/4store/` exists (in a cluster, it only needs to exist on
backend nodes) and that the user or users who will create new KBs have
permission to write to this directory.

For clusters (or to test cluster tools on a single machine) the frontend must
have a file `/etc/4s-cluster`, which lists all machines in the cluster.


To avoid problems running out of Avahi DBUS connections, modify
`/etc/dbus-1/system.d/avahi-dbus.conf` to:

* Increase `max_connections_per_user` to 200 or so
* Increase `max_match_rules_per_connection` to 512 or so (optional)
