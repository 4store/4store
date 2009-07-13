#!/usr/bin/perl -w

use Time::HiRes qw(time);
use IPC::Open2;

$kb_name = shift;
if (!$kb_name) {
	die "Usage: $0 <kb-name>\n";
}

$cmd = '../../frontend/4s-query';

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

local(*IN, *OUT);
open2(*OUT, *IN, $cmd, $kb_name, '-f', 'text', '-P') || die "Cannot open pipe to query command: $!";

# push some junk through to make sure the process is running
print(IN "SELECT ?x WHERE { ?x ?y ?z } LIMIT 10\n");
print(IN "#EOQ\n");
while (<OUT>) {
	last if /^#EOR/;
}

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
				print(IN $sqs."\n#EOQ\n");
				while (<OUT>) {
					last if /^#EOR/;
					if (/^#/) {
						print $sqs."\n";
						die $_;
					}
				}
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
			print(IN $qstr{$q}."\n#EOQ\n");
			while (<OUT>) {
				last if /^#EOR/;
				if (/^#/) {
					print $qstr{$q}."\n";
					die $_;
				}
			}
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
