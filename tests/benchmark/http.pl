#!/usr/bin/perl -w

use lib '../../../../qdos/trunk/extractors';
use sparql;
use Time::HiRes qw(time);
use IPC::Open2;
use URI::Escape;

$ep = "http://localhost:8080/sparql/";

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";

@queries = `ls queries`;
grep {chomp $_} @queries;

for $q (@queries) {
	open(Q, "queries/$q") || die "Cannot open file: $!";
	$qstr{$q} = join("", <Q>);
	close(Q);
}

@lists = `ls lists`;
grep {chomp $_} @lists;

for $l (@lists) {
	open(L, "lists/$l") || die "Cannot open file: $!";
	my @array = <L>;
	$lv = $l;
	$lv =~ s/\.[a-z0-9]+$//;
	$list{$lv} = \@array;
	close(L);
}


$qs = "SELECT ?x WHERE { ?x ?y ?z } LIMIT 10";
sparql_query($ep, $qs);

for $q (sort @queries) {
	my $best = 9999999.0;
	my $worst = 0.0;
	my $total = 0.0;
	my $its = 0;
	if ($qstr{$q} =~ /@([a-z]+)@/) {
		my $var = $1;
		for $val (@{ $list{$var} }) {
			$its++;
			my $then = time();
			my $qs = $qstr{$q};
			$qs =~ s/\@$var@/$val/g;
			for $sqs (split("#EOQ", $qs)) {
			sparql_query($ep, $sqs);
			}
			$t = (time() - $then) * 1000.0;
			$best = $t if $t < $best;
			$worst = $t if $t > $worst;
			$total += $t;
		}
	} else {
		for (1..10) {
			$its++;
			my $then = time();
			sparql_query($ep, $qstr{$q});
			$t = (time() - $then) * 1000.0;
			$best = $t if $t < $best;
			$worst = $t if $t > $worst;
			$total += $t;
		}
	}
	if ($its == 0) {
		printf("%-40s ERROR\n", $q);
	} else {
		printf("%-40s %10.3fms %10.3fms %10.3fms\n", $q, $best, $total/$its, $worst);
	}
}
