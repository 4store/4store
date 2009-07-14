#!/usr/bin/perl -w

$kb_name = "cluster_test";
system("4s-cluster-start $kb_name");

$outdir = "results";
$test = 1;
my @tests = ();

if ($ARGV[0]) {
	if ($ARGV[0] eq "--exemplar") {
		$outdir = "exemplar";
		$test = 0;
		shift;
	} elsif ($ARGV[0] eq "--outdir") {
		shift;
		$outdir = shift;
		$test = 1;
	}
	while ($t = shift) {
		$t =~ s/^(.\/)?scripts\///;
		push @tests, $t;
	}
}
mkdir($outdir);

if (!@tests) {
	@tests = `ls scripts`;
}

for $t (@tests) {
	chomp $t;
	if (!stat("exemplar/$t") && $test) {
		print("SKIP $t (no exemplar)\n");
		next;
	}
	unlink("$outdir/".$t);
	system("FORMAT=ascii LANG=C TESTPATH=../../src scripts/$t $kb_name > $outdir/$t 2>$outdir/$t-errs");
	if ($test) {
		@diff = `diff exemplar/$t $outdir/$t 2>/dev/null`;
		if (@diff) {
			print("FAIL $t\n");
			open(RES, ">> $outdir/$t-errs") || die 'cannot open error file';
			print RES "\n";
			print RES @diff;
			close(RES);
		} else {
			print("PASS $t\n");
		}
	}
}
system("4s-cluster-stop $kb_name");
