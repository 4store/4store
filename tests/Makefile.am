all:

test: test-query test-httpd test-httpd-extras

test-query:
	(cd query && pwd && ./setup.sh --autorun)

test-httpd:
	(cd httpd && pwd && ./run.pl)

test-httpd-extras:
	(cd httpd-extras && pwd && ./run.pl)

test-admin:
	(cd admin && pwd && ./run.pl)


EXTRA_DIST = query/setup.sh query/run.pl query/exemplar query/scripts \
	     httpd/run.pl httpd/sparql.sh httpd/exemplar httpd/scripts \
	     httpd-extras/run.pl httpd-extras/sparql.sh httpd-extras/exemplar httpd-extras/scripts \
	     admin/admin_tests.conf admin/scripts admin/exemplar admin/run.pl admin/vars.sh
