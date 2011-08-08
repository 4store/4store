
4s-boss and 4s-admin documentation
=================================


Summary
-------

4s-boss and 4s-admin are optional components of 4store, designed to make
cluster setup and administration a bit easier.


### 4s-boss ###

4s-boss is a daemon which is designed to be run on each storage node of a
cluster.  It provides two main features:

* Store discovery - it provides a lookup and discovery mechanism for stores
that doesn't rely on Avahi or mDNS

* Store administration - this is currently limited to getting runtime
information about stores, but will be extended to allow store creation,
starting, stopping, etc.


### 4s-admin ###

4s-admin is a command line tool used to communicate with all the 4s-boss
daemons running in a cluster.  At the moment it only supports a few read-only
operations, such and finding out which stores exist on which host, and whether
they're running or not.

It will later be extended to support full administration of a cluster.



Configuration
-------------

Configuration options are kept in /etc/4store.conf.  All options should be put
in the [default] section of the configuration file.


### Configuration for storage nodes ###

Optional:

    4s-boss_port = <port number>


#### Setting: 4s-boss_port ####

Default:

    4s-boss_port = 6733

This is read by the 4s-boss daemon at startup, and can be overridden by 
passing a command line option to 4s-boss instead.  If this isn't set in
/etc/4store.conf, 4s-boss will default to port 6733.


### Configuration for master nodes ###

Optional:

    nodes = <host1:port>;<host2:port>;<host3>;<host4>;...
    4s-boss_discovery = none|default|fallback|sole


#### Setting: nodes ####

Default:

    nodes = localhost

The 'nodes' option should be used to specify the storage nodes in a cluster,
each separated by a semicolon (';') character.

Each node should be specified as host:port, where host can be either a
hostname, fully qualified hostname, or IP address (note: IPv6 addresses
should be enclosed in square brackets, e.g. [2001:db8::1]:6733.  Port is the
numberic port number which 4s-boss is running on for that host, if this is
ommitted, the default port of 6733 will be assumed.

Example:

    nodes = foo1;foo2:6000;foo3.example.org:1234;127.0.0.1;[2001:db8::1]:6733


#### Setting: 4s-boss_discovery ####

Default:

    4s-boss_discovery = none

This field can take one of four values, 'none', 'default', 'fallback' or
'sole'.

This specifies how (and if) 4s-boss on storage nodes is used for store
discovery.  This setting will affect how things like 4s-httpd and 4s-import
find out about running 4s-backend processes.

Settings are:

* none - 4s-boss is not used for discovery, Avahi/mDNS will be used if they
were available when 4store was compiled.

* default - 4s-boss is checked first to discover stores.  If it's unavailable
or lookup fails, Avahi/mDNS will be used as a fallback (if available).

* fallback - Avahi/mDNS are used for discovery, but if store lookup fails, then
4s-boss will be used as a fallback.

* sole - 4s-boss will be used as the sole method of store discovery. Methods
such as Avahi/mDNS won't be used even if available.



Getting Started
---------------

### On each storage node:

Run 4s-boss. This will start the daemon running on port 6733 by default.

Run 4s-backend <kb_name> to start any stores for the cluster (this can be
done before or after starting 4s-boss).


### On master node:

Add the hostnames of each storage node, and how 4s-boss should be used
to /etc/4store.conf (create it if it doesn't exist).  A sample configuration
might look like:

    [default]
        4s-boss_discovery = default
        nodes = host1;host2;host3;host4

To check that the 4s-boss daemon on each node is reachable, use the 4s-admin
command:
    4s-admin list-stores

Assuming all is well, you should get a list of running stores from each storage
node.

You should then be able to use 4s-httpd and 4s-import as normal.
