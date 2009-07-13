#!/usr/bin/perl -w

use Time::HiRes qw(time);

$qpb = 1;	# queries per block

$kb = "tiger_umac";

$its = shift;
$offset = shift;
$offset *= $its;

#if (undef $offset) {
#	die "usage: $0 <iterations> <offset>";
#}

$tlidq = "../../../src/frontend/4s-query $kb -f text 'PREFIX vocab: <http://www.census.gov/tiger/2002/vocab#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT * WHERE { ?line rdf:type vocab:Line } LIMIT $its OFFSET $offset' |";
open(QTEST, $tlidq);

@uris = <QTEST>;
close(QTEST);
if (@uris < $its-1) {
	print STDERR "\n".$tlidq."\n";
	die "not enough URIs, ".@uris."/$its";
};
shift @uris;

$then = time();
open(QPROC, "| ../../../src/frontend/4s-query $kb -P > /dev/null");
for $i (0..$its-1) {
	sleep(0.001) if $i > 0;

        select QPROC;

	print "PREFIX vocab: <http://www.census.gov/tiger/2002/vocab#>\n";
	print "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n";
	print "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n";
	print "SELECT * WHERE {\n";

	print "_:place vocab:path $uris[$i] .\n";
	print "_:place rdfs:label ?label .\n";
	print "$uris[$i] vocab:start _:start .\n";
	print "_:start vocab:long ?startlong . _:start vocab:lat ?startlat .\n";
	print "$uris[$i] vocab:end _:end .\n";
	print "_:end vocab:long ?endlong . _:end vocab:lat ?endlat .\n";
#	print "$uris[$i] rdf:type _:subclass .\n";
#	print "_:subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> _:class .\n";
#	print "_:class rdfs:label ?description .\n";
	print "OPTIONAL {\n";
	print "_:join vocab:long ?endlong . _:join vocab:lat ?endlat .\n";
	print "?next vocab:start _:join .\n";
	print "} } LIMIT 20\n";;

        print "#EOQ\n";

        select STDOUT;
}
close(QPROC);
$now = time();

printf("%f queries/sec\n", $its*$qpb/($now-$then));
