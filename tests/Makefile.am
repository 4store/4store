all:

test: test-query test-httpd test-httpd-extras test-admin

test-query:
	(cd query && pwd && ./setup.sh --autorun)

test-httpd:
	(cd httpd && pwd && ./run.pl)

test-httpd-extras:
	(cd httpd-extras && pwd && ./run.pl)

test-admin:
	(cd admin && pwd && ./run.pl)


EXTRA_DIST = query/setup.sh query/run.pl query/exemplar query/scripts \
	     httpd/run.pl httpd/sparql.sh httpd/exemplar httpd/scripts
