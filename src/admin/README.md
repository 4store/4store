
4s-boss and 4s-admin documentation
=================================


Summary
-------

4s-boss and 4s-admin are optional components of 4store, designed to make
cluster setup and administration a bit easier.


### 4s-boss ###

4s-boss is a daemon which can be run on each storage node of a cluster.  It
provides two main features:

* Store discovery - it provides a lookup and discovery mechanism for stores
that doesn't rely on Avahi mDNS or DNS-SD.

* Store administration - it provides reporting on stores across a cluster,
and can start/stop stores on demand.


### 4s-admin ###

4s-admin is a command line tool used to communicate with all the 4s-boss
daemons running in a cluster.  It can be used to start/stop stores across a
cluster, and provide reporting on running or stopped stores.

It will later be extended to support creation and deletion of stores across a
cluster.



Quick Start
-----------

### On each storage node:

Run 4s-boss. This will start the daemon running on port 6733 by default.

To stop the 4s-boss daemon, use:

    killall 4s-boss

or

    kill <4s-boss pid>

Stores can be started/stopped by 4s-admin (see next section), but they will
need to be created first, using [4s-backend-setup][1].

### On master node:

Add the hostnames of each storage node, and how 4s-boss should be used
to /etc/4store.conf (create it if it doesn't exist).  A sample configuration
for a cluster with 4 storage nodes might look like:

    [4s-boss]
        discovery = default
        nodes = host1;host2;host3;host4

To check that the 4s-boss daemon on each node is reachable, use the 4s-admin
command:

    4s-admin list-stores

Assuming all is well, you should get a list of running stores from each storage
node.

To start stores across the cluster, use:

    4s-admin start-stores <store1> <store2> <store3> ...

        or

    4s-admin start-stores -a

You should then be able to use 4s-httpd and 4s-import as normal.



Configuration
-------------

Configuration options are kept in /etc/4store.conf.  All options should be put
in the [4s-boss] section of the configuration file.


### Configuration for storage nodes ###

Optional:

    port = <port number>


#### Setting: port ####

Default:

    port = 6733

This is read by the 4s-boss daemon at startup, and can be overridden by 
passing a command line option to 4s-boss instead.  If this isn't set in
/etc/4store.conf, 4s-boss will default to port 6733.


### Configuration for master nodes ###

Optional:

    nodes = <host1:port>;<host2:port>;<host3>;<host4>;...
    discovery = none|default|fallback|sole


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


#### Setting: discovery ####

Default:

    discovery = none

This field can take one of four values, 'none', 'default', 'fallback' or
'sole'.

This specifies how (and if) 4s-boss on is used for store discovery.  This
setting will affect how things like 4s-httpd and 4s-import find out about
running 4s-backend processes.

Settings are:

* none - 4s-boss is not used for discovery, Avahi/DNS-SD will be used if they
were available when 4store was compiled.

* default - 4s-boss is checked first to discover stores.  If it's unavailable
or lookup fails, Avahi/DNS-SD will be used as a fallback (if available).

* fallback - Avahi/DNS-SD are used for discovery, but if store lookup fails,
then 4s-boss will be used as a fallback.

* sole - 4s-boss will be used as the sole method of store discovery. Methods
such as Avahi/mDNS won't be used even if available.



Command Line Options
--------------------

For a full listing of command line options available for 4s-boss and 4s-admin,
and for a full list of 4s-admin commands, use:

    4s-boss --help

and

    4s-admin --help

For help on a specific 4s-admin command, use:

    4s-admin help <command>

e.g.

    4s-admin help start-stores



Troubleshooting / Notes
-----------------------

### 4s-boss ###

4s-boss should be run as a non-root user which has read access on each store's
directory (/var/lib/4store by default).  It uses PID files within those
directories to work whether stores are running or not (and the port that
they're running on).

Error messages when 4s-boss is running as a daemon will be sent to syslog.
For troubleshooting, you can send output to stderr, and increase the error
reporting level by using:

    4s-boss -D --debug


### 4s-admin ###

4s-admin reads the cluster configuration from /etc/4store.conf, it will need
to be readable by the user that runs 4s-admin.

It will attempt to communicate with 4s-boss on each node on port 6733 by
default, ensure that firewall rules are in place to allow this.

To enable full debugging output when troubleshooting, use:

    4s-admin --debug <command> [<command options>]

e.g.

    4s-admin --debug stop-stores -a



Contact / Support
-----------------

If you have any problems using 4s-boss or 4s-admin, you can find help at:

* [4store mailing list][2]
* [#4store on freenode IRC][3]
* email me - Dave Challis <suicas@gmail.com>

Send any bug reports/issues to:

* github issue tracker - https://github.com/garlik/4store


[1]: http://4store.org/trac/wiki/CreateDatabase
[2]: http://groups.google.com/group/4store-support
[3]: irc://irc.freenode.net/#4store
