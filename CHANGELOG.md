# v1.1.7

* Merge pull request #153 from berezovskyi/b147-travis
* Add Travis build config
* Merge pull request #151 from berezovskyi/b146-readme
* Remove 'make get-deps' instruction
* Updated README
  * use docs/INSTALL as a base
  * convert to Markdown
  * add description

# v1.1.6

* Hack another localhost to be 127.0.0.1 (Steve Harris in e45e869a7a86dd92c791632d2ca5eaab37dd82bd)
* Hack localhost to be 127.0.0.1, to avoid problem on Mavericks (Steve Harris in 6385b4a8ea4acea4be0343e8535289dc496b6b4a)
* Merge pull request #115 from jaredjennings/revert-configure-switch-change (Steve Harris in 6bdf110519ef67976921c0113161850458107518)
* back out nonessential changes from 2b2338b (Jared Jennings in a9ba5a4c6bd306769f340765c0375012bdd7f7b6)
* Merge pull request #112 from jaredjennings/variable-store-root (Steve Harris in f2309b3c266142e49e2ff9d175b94c77315d3b04)
* Merge pull request #111 from kgardas/master (Steve Harris in 7d44d803a51834f1ce9eeb489562f39f0abc18d6)
* fix httpd's handling of POST to /update/ for striping encoding info from the content type correctly (Karel Gardas in 9a4f077c1bbd3e0f6b3832bce8a8e822100acc8f)
* let user change store root at runtime (jared jennings in 2b2338b15c29f25a3d790beb6679e0c1588f8b75)
* correction in httpd format test (msalvadores in e1ba372ee8ac6eadd84c963c971c45b19a0df371)
* avoid seg faults with strcmp when missing .lex attribute (msalvadores in ad4fc0c79464246bf7b68a8393627c1e8c1ad0f0)
* Send correct MIME type for ntriples response (Steve Harris in 276c021481bd44b077477065b92b0b9a123dce86)
* Return NTriples for CONSTRUCT if a application/n-triples request is made (Steve Harris in 617ba491303f1c60512b0e022ee78bc67e6649ec)
* fixed 4s-dump script to support large datasets with dumping directly to a file (unknown in 3ab09139011de383fb32a61d7ceaa2944d434ea2)
* Fixed CORS for POST requests, as per patch from Yves Raimond (Steve Harris in 7a551520569fa9f25785831fb2ac91da0adaaedb)
* Changed 4s-dumop to use strict mode, to make it safer (Steve Harris in e162b139ef644890a5beacfb4c3592d70a5c5c46)
* Merge branch 'master' of github.com:garlik/4store (msalvadores in 73a0df0baa3053b397721ec0f25ac3c0dcb8d26e)
* rows_returned -= offset in httpd log trace (msalvadores in 95af3f20aca453fb9260e65ebea6e9fe1f2677a8)
* Accept "xml" as an output type, alias for "sparql". (Steve Harris in 59fc49220f0a8ad016508aef2e4b9905837136ee)
* tests for pagination with filters (msalvadores in 901add8b784201691b29b082346ef5b12ab506b6)
* pagination with offset & limit combined with filter (msalvadores in 45d40d6d75ce8266b4ed4dbda62e7a03269b3d1b)
* Added a missing update commit stage! (Steve Harris in e3086a6a8da9a960a98ecf066bfd5db8dd9e9e7f)
* More robust error handling (Steve Harris in 796d3b8b740c3b22c8a6e8baa39c347f07a71a51)
* Clearer resource symbols error text (Steve Harris in 438032b1bcc9325b6787dd4be0dddfb6eeffc79f)
* More modern auto* directives (Steve Harris in 98b4f273073f06f7e355934ed7047d8878a11eca)
* Tracing log message for HTTP Content-Type (Steve Harris in e30b9446941b9ca1df78ac3512f25948faef4982)
* In some cases an fs_rid_set could contain duplicates. (msalvadores in eaa549b452a6ad424c70944278b9577fc35a7956)
* Exemplar data corrected (a consequence of #99) (msalvadores in 92ac04844fa2018b34c62134ddb7cf094e5a04bd)
* Patch for 'Comparison with negative number' #99 (msalvadores in 2a8004ed901288509e5dfb0300c9cc8fd9d38132)
* Test to check number comparisons on FILTER. (msalvadores in a5fd5635cc9f233273aed3894ef7c7ff789839bf)
* Lang constants for the resolution of lang RIDs (close #100) (msalvadores in ca99301f38f9cd76ef64c53752d8319cd597dce4)
* Merge ncbo patch for (close #98) (msalvadores in a48ff455870d4189be6e35d138b996b4aff3adaa)
* Only increment port number if we actually are going to try it (Alex Wilson in 14f517eea871d5d35c6b8b93d992f2ef0791932f)
* Merge pull request #95 from arekinath/portincr (Steve Harris in 3feefecdf1b7f87a64d2cabf00078a172f0f1273)
* Increment port number if we fail to get a socket (Alex Wilson in 392a3794d0bfec631bd690f623b3976eea8dc7df)
* Merge pull request #92 from CloCkWeRX/master (Steve Harris in c3d8593c39dc04a39e272f729e486c1576f6aeba)
* Add a dropdown to append the output type to the test query UI (Daniel O'Connor in 01a6d63475ca28ce51b81b98803160477b85f4a1)
* Added JSONP support, using the callback= CGI argument (Steve Harris in 5da0ba4ef8153da3e6bf55582ae0a4e6df787c2b)
* Merge pull request #91 from arekinath/more_mmap (Steve Harris in 15cde67ebe82ff99d0d8df2874176bd148db94e4)
* Consistent mmap i/o for ptable and tbchain (Alex Wilson in b07ca4a090cafad1424a3135f2e4e7e677dffb7a)
* Merge pull request #90 from arekinath/endian_fix (Steve Harris in 8801f4b49d58658dd18ff0dce532e25dff0d5c1e)
* Detect endian/bitness correctly on OpenBSD and others (Alex Wilson in 34c7f2e3df7637224daa4e8c63ee399b5b8d0d73)
* Merge pull request #89 from arekinath/nopwrite (Steve Harris in 0976a04a02b81da1a5229469d169f1ffe1c99a26)
* Avoid using pwrite with mmap (or use msync) (Alex Wilson in af86d87414dfd70312222d7e6569f431cda106fb)
* Remove admin tests from make test for now, can't figure out why it's failing (Steve Harris in c1666f4467ae4ec7175cdd17469e0c67bcf82176)
* More testcases fixes (Steve Harris in bdf8ffd6a4b2e960c93046c11f0bc8c356651739)
* Unbroke the httpd tests (Steve Harris in 771322cea8ea2588b4f21544ff1dffb1c60ffeb8)
* Some critical whitespace got removed without updating the matching exemplar (Steve Harris in d9e523d076df7d09346771043bd56c48381ff063)
* Tweak test for uuid_string_t existence - now compiles on CentOS and Mac OS X (Steve Harris in add4132470ca796bb7d23bf5971fca92c9298c42)
* Merge pull request #88 from arekinath/openbsd (Steve Harris in eb3d7c8190e4bee36dcafaa20d70b14c8f777f09)
* Replace /bin/bash in shebangs with /usr/bin/env bash (Alex Wilson in 5cc1819ec2630f76a4d46d312b4a3d036e41d0a0)
* Use listen sockets properly for split IPv6/IPv4 stacks (Alex Wilson in f48625cdc02a2ffefded36f48e10e984eee19c54)
* Compile on OpenBSD, use different variants of libuuid (Alex Wilson in 89ec910c8460bd171825ed44e402983251466a0b)
* Fixed pointer aritmetic bug, spotted by Alex Wilson. (Steve Harris in d3e32083cafb5784d781f9da29e1c5d65bba0ec0)
* Patch for #87 Update with syntax error returns HTTP 200 (msalvadores in 9a505781de5fe368c5be6a72956a95c9a3c22ee2)
* removed query rewriting message in garlik repo (msalvadores in 480f397d22889d16d46d300aec4fb779e7ccedb5)
* merged correct INFO trace levels for some messages (msalvadores in 8fd3256b8a2318e6f29d8325af71c23460267455)
* Patch from Josef Nygrin which adds &output= support to POST query requests. (Steve Harris in 4123edab59e0c4538faa6ca482f019c5211cfcd3)
* Make configure fail if required libraries aren't found. (Steve Harris in b3bf40bbb1a6c8de64311d600122a1e0dc12d96c)
* Changes from last release (Steve Harris in c289dc2128bb57d096fd0f1975c1d41c87d942b2)
* Added a configure flag --with-http-log which can be used to set the HTTP log directory (Steve Harris in ffabb82b2827b745e59d8937c800a692254f730d)
* Fix typo in hash test script, reported by Leif Warner (Steve Harris in f8eeeca8fca7d63edf8305acbdb407e3c8029dcf)


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


# v1.1.4

## New features

* Many improvements to test system
* MD5, SHA1, SHA256 hash functions
* RAND, FLOOR, CEIL, ROUND, ABS maths functions
* Support ORDER BY with GROUP BY
* Support for MINUS keyword
* TSV output now compliant with W3C draft

## Bugfixes

* Major bugfix to quad delete code
* Better ISO8601 date handling
* SUBSTR bugfixes
* Fix many small memory leaks (some remain)
* Static built mac binaries, for more reliable .app
* Threadsafeness imrovements in HTTP server
* Fix for strange bug relating to rasqal parse error
* Fix for xsd:int / xsd:integer handling
* Fix to stuff deep in query engine
* Fixes to date / dateTime comparison functions
* Fix crash under heavy HTTP query load
* Faster cluster discovery on Macs
* Unary minus maths bugfix
* Better UNION handling

# v1.1.3

* Includes a lib4store that lets C programs connect to 4store backends directly and either do low-level operations or run SPARQL queries (Mischa Tuffield and Florian Ragwitz)
* 4store will no longer emit invalid ntriples triples from CONSTRUCT
* Fix a bug that caused the query engine to abort under certain circumstances
* with OPTIONAL { } blocks with an empty left hand side.
* Fix a caching bug relating to ASK-like queries (i.e. with no variables)
* Better handling for a known failure in common prefix compression of URIs
* Fix invalid JSON output (msalvadores and Dave Challis)
* Improvements to the Aggregate support (msalvadores)
* Improvements to the query logging (Dave Challis and William Waites)
* Improvements to autoconf setup (Dave Beckett)
* Bugfix to media-type handling (Dave Challis)
* Bugfix to handling of DISTINCT (Dave Challis)
* Bugfix to handling of CONSTRUCT with UNION (Dave Challis)
* Handle FILTER in Aggregated binding sets (msalvadores)
* Fix cast bug in FILTER (msalvadores)
* Cluster management tools bugfix (Ian Millard)
* Various improvements to man pages
* Ability to use EXPLAIN in 4s-query, using standard SPARQL syntax (msalvadores)
* Performance improvements in resolver cache (msalvadores)

# v1.1.2

* Added config file for configuring http server behaviour (see http://4store.org/trac/wiki/SparqlServer#ConfigFile)
* Reverted fix for Content-Length bug, as it was causing serious side effects
* Fixed some memory leaks in various places, including one serious backend one
* Capped cache size in query engine
* Some debug tool improvements
* Some improvements to the CORS support

# Older

See http://github.com/garlik/4store/commits/master
