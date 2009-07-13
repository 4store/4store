#!/usr/bin/perl -w

$kb_prefix = "test_import_";

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";
$outdir = "results";
$test = 1;
my @tests = ();
my $errs = 1;

$SIG{USR2} = 'IGNORE';

if ($ARGV[0]) {
	if ($ARGV[0] eq "--exemplar") {
		$outdir = "exemplar";
		$test = 0;
		$errs = 0;
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
	if ($errs) {
		$errout = "2>$outdir/$t-errs";
	} else {
		$errout = "";
	}
	$kb_name = $kb_prefix . $t;
	$kb_name =~s/-/_/g;
	printf(".... $t\r");
	system("CLUSTER=yes FORMAT=ascii LANG=C scripts/$t $kb_name > $outdir/$t $errout");
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
			unlink("$outdir/$t-errs");
		}
	}
	system("4s-cluster-destroy $kb_name >/dev/null 2>&1");
}
