# v1.1.6

* _Hack another localhost to be 127.0.0.1_ by Steve Harris (e45e869a)
* _Hack localhost to be 127.0.0.1, to avoid problem on Mavericks_ by Steve Harris (6385b4a8)
* _Merge pull request #115 from jaredjennings/revert-configure-switch-change_ by Steve Harris (6bdf1105)
* _back out nonessential changes from 2b2338b_ by Jared Jennings (a9ba5a4c)
* _Merge pull request #112 from jaredjennings/variable-store-root_ by Steve Harris (f2309b3c)
* _Merge pull request #111 from kgardas/master_ by Steve Harris (7d44d803)
* _fix httpd's handling of POST to /update/ for striping encoding info from the content type correctly_ by Karel Gardas (9a4f077c)
* _let user change store root at runtime_ by jared jennings (2b2338b1)
* _correction in httpd format test_ by msalvadores (e1ba372e)
* _avoid seg faults with strcmp when missing .lex attribute_ by msalvadores (ad4fc0c7)
* _Send correct MIME type for ntriples response_ by Steve Harris (276c0214)
* _Return NTriples for CONSTRUCT if a application/n-triples request is made_ by Steve Harris (617ba491)
* _fixed 4s-dump script to support large datasets with dumping directly to a file_ by unknown (3ab09139)
* _Fixed CORS for POST requests, as per patch from Yves Raimond_ by Steve Harris (7a551520)
* _Changed 4s-dumop to use strict mode, to make it safer_ by Steve Harris (e162b139)
* _Merge branch 'master' of github.com:garlik/4store_ by msalvadores (73a0df0b)
* _rows_returned -= offset in httpd log trace_ by msalvadores (95af3f20)
* _Accept "xml" as an output type, alias for "sparql"._ by Steve Harris (59fc4922)
* _tests for pagination with filters_ by msalvadores (901add8b)
* _pagination with offset & limit combined with filter_ by msalvadores (45d40d6d)
* _Added a missing update commit stage!_ by Steve Harris (e3086a6a)
* _More robust error handling_ by Steve Harris (796d3b8b)
* _Clearer resource symbols error text_ by Steve Harris (438032b1)
* _More modern auto* directives_ by Steve Harris (98b4f273)
* _Tracing log message for HTTP Content-Type_ by Steve Harris (e30b9446)
* _In some cases an fs_rid_set could contain duplicates._ by msalvadores (eaa549b4)
* _Exemplar data corrected (a consequence of #99)_ by msalvadores (92ac0484)
* _Patch for 'Comparison with negative number' #99_ by msalvadores (2a8004ed)
* _Test to check number comparisons on FILTER._ by msalvadores (a5fd5635)
* _Lang constants for the resolution of lang RIDs (close #100)_ by msalvadores (ca99301f)
* _Merge ncbo patch for (close #98)_ by msalvadores (a48ff455)
* _Only increment port number if we actually are going to try it_ by Alex Wilson (14f517ee)
* _Merge pull request #95 from arekinath/portincr_ by Steve Harris (3feefecd)
* _Increment port number if we fail to get a socket_ by Alex Wilson (392a3794)
* _Merge pull request #92 from CloCkWeRX/master_ by Steve Harris (c3d8593c)
* _Add a dropdown to append the output type to the test query UI_ by Daniel O'Connor (01a6d634)
* _Added JSONP support, using the callback= CGI argument_ by Steve Harris (5da0ba4e)
* _Merge pull request #91 from arekinath/more_mmap_ by Steve Harris (15cde67e)
* _Consistent mmap i/o for ptable and tbchain_ by Alex Wilson (b07ca4a0)
* _Merge pull request #90 from arekinath/endian_fix_ by Steve Harris (8801f4b4)
* _Detect endian/bitness correctly on OpenBSD and others_ by Alex Wilson (34c7f2e3)
* _Merge pull request #89 from arekinath/nopwrite_ by Steve Harris (0976a04a)
* _Avoid using pwrite with mmap (or use msync)_ by Alex Wilson (af86d874)
* _Remove admin tests from make test for now, can't figure out why it's failing_ by Steve Harris (c1666f44)
* _More testcases fixes_ by Steve Harris (bdf8ffd6)
* _Unbroke the httpd tests_ by Steve Harris (771322ce)
* _Some critical whitespace got removed without updating the matching exemplar_ by Steve Harris (d9e523d0)
* _Tweak test for uuid_string_t existence - now compiles on CentOS and Mac OS X_ by Steve Harris (add41324)
* _Merge pull request #88 from arekinath/openbsd_ by Steve Harris (eb3d7c81)
* _Replace /bin/bash in shebangs with /usr/bin/env bash_ by Alex Wilson (5cc1819e)
* _Use listen sockets properly for split IPv6/IPv4 stacks_ by Alex Wilson (f48625cd)
* _Compile on OpenBSD, use different variants of libuuid_ by Alex Wilson (89ec910c)
* _Fixed pointer aritmetic bug, spotted by Alex Wilson._ by Steve Harris (d3e32083)
* _Patch for #87 Update with syntax error returns HTTP 200_ by msalvadores (9a505781)
* _removed query rewriting message in garlik repo_ by msalvadores (480f397d)
* _merged correct INFO trace levels for some messages_ by msalvadores (8fd3256b)
* _Patch from Josef Nygrin which adds &output= support to POST query requests._ by Steve Harris (4123edab)
* _Make configure fail if required libraries aren't found._ by Steve Harris (b3bf40bb)
* _Changes from last release_ by Steve Harris (c289dc21)
* _Added a configure flag --with-http-log which can be used to set the HTTP log directory_ by Steve Harris (ffabb82b)
* _Fix typo in hash test script, reported by Leif Warner_ by Steve Harris (f8eeeca8)

# v1.1.5

New features

* Non MDNS admin system, for clusters with lots of KBs, and systems with no
  multicast (e.g. Amazon EC2) — Dave Challis
* RDF 1.1 bNode Skolemsation (enabled with --enable-skolemization)
* Implementation of IN and NOT IN
* Increase maximum number of segments in one DB
* OFFSET and LIMIT now works with GROUP BY
* New functions: STRBEFORE, STRAFTER, UUID, STRUUID
* BIND implemented
* Chache stats in HTTP server status page
* Support skolemisation in CONSTRUCT and DESCRIBE
* Some support for xsd:date
* Access control support (graph level), see
  http://4store.org/trac/wiki/GraphAccessControl — Manuel Salvadores
* DISTINCT in Aggregates

Bugfixes

* Loads of improvements to test system — everyone
* Fix bug in disjunctive FILTER optimisation
* Handle Turtle MIME type correctly
* Bugfix for DESCRIBE, returning wrong RDF syntax — Mischa Tuffield
* Bugfix for DESCRIBE in graphs with cycles - Manuel Salvadores
* Bugfix to URI prefix compression - Manuel Salvadores
* Bugfix for numeric types in INSERT/DELETE DATA
* Better XML result output
* Bind all variables when WHERE pattern forms a triangle of triples - Manuel
  Salvadores
* Fix for shell tools — Pavol Rusnak
* Make FROM work with DESCRIBE
* Better portability across Linux distros — Rob Syme, Leif Warner
* SPARQL Update and language tag fixes
* Various optimiser improvments
* INSERT ... WHERE ... bugfixes
* ORDER BY on AS projected variables — Manuel Salvadores
* Better static build tools — Nicholas Humfrey


## v1.1.4

## New features

Many improvements to test system

MD5, SHA1, SHA256 hash functions

RAND, FLOOR, CEIL, ROUND, ABS maths functions

Support ORDER BY with GROUP BY

Support for MINUS keyword

TSV output now compliant with W3C draft

## Bugfixes

Major bugfix to quad delete code

Better ISO8601 date handling

SUBSTR bugfixes

Fix many small memory leaks (some remain)

Static built mac binaries, for more reliable .app

Threadsafeness imrovements in HTTP server

Fix for strange bug relating to rasqal parse error

Fix for xsd:int / xsd:integer handling

Fix to stuff deep in query engine

Fixes to date / dateTime comparison functions

Fix crash under heavy HTTP query load

Faster cluster discovery on Macs

Unary minus maths bugfix

Better UNION handling

# v1.1.3

Includes a lib4store that lets C programs connect to 4store backends
directly and either do low-level operations or run SPARQL queries (Mischa
Tuffield and Florian Ragwitz)

4store will no longer emit invalid ntriples triples from CONSTRUCT

Fix a bug that caused the query engine to abort under certain circumstances
with OPTIONAL { } blocks with an empty left hand side.

Fix a caching bug relating to ASK-like queries (i.e. with no variables)

Better handling for a known failure in common prefix compression of URIs

Fix invalid JSON output (msalvadores and Dave Challis)

Improvements to the Aggregate support (msalvadores)

Improvements to the query logging (Dave Challis and William Waites)

Improvements to autoconf setup (Dave Beckett)

Bugfix to media-type handling (Dave Challis)

Bugfix to handling of DISTINCT (Dave Challis)

Bugfix to handling of CONSTRUCT with UNION (Dave Challis)

Handle FILTER in Aggregated binding sets (msalvadores)

Fix cast bug in FILTER (msalvadores)

Cluster management tools bugfix (Ian Millard)

Various improvements to man pages

Ability to use EXPLAIN in 4s-query, using standard SPARQL syntax (msalvadores)

Performance improvements in resolver cache (msalvadores)

# v1.1.2

Added config file for configuring http server behaviour (see
http://4store.org/trac/wiki/SparqlServer#ConfigFile)

Reverted fix for Content-Length bug, as it was causing serious side effects

Fixed some memory leaks in various places, including one serious backend one

Capped cache size in query engine

Some debug tool improvements

Some improvements to the CORS support

# Older

See http://github.com/garlik/4store/commits/master
