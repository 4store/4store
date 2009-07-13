#!/usr/bin/perl -w

use Term::ANSIColor;

$kb_name = "query_test_".$ENV{'USER'};

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";
$outdir = "results";
$test = 1;
my @tests = ();
my $errs = 1;
my $spawn = 1;

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
	} elsif ($ARGV[0] eq "--nospawn") {
		shift;
		$spawn = 0;
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

if ($pid = fork()) {
	sleep(2);
	my $fails = 0;
	my $passes = 0;
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
		print("[....] $t\r");
		my $ret = system("FORMAT=ascii LANG=C TESTPATH=../../src scripts/$t $kb_name > $outdir/$t $errout");
		if ($ret == 2) {
			exit(2);
		}
		if ($test) {
			@diff = `diff exemplar/$t $outdir/$t 2>/dev/null`;
			if (@diff) {
				print("[");
				print color 'bold red';
				print("FAIL");
				print color 'reset';
				print("] $t\n");
				open(RES, ">> $outdir/$t-errs") || die 'cannot open error file';
				print RES "\n";
				print RES @diff;
				close(RES);
				$fails++;
			} else {
				print("[");
				print color 'bold green';
				print("PASS");
				print color 'reset';
				print("] $t\n");
				$passes++;
			}
		}
	}
	print("Tests completed: passed $passes/".($fails+$passes)."\n");
	$ret = kill 15, $pid;
	if (!$ret) {
		warn("failed to kill server process, pid $pid");
	} else {
		waitpid($pid, 0);
	}
} else {
	# child
	if ($spawn) {
		exec("../../src/backend/4s-backend", "-D", "-l", "2.0", "$kb_name");
		die "failed to exec sever: $!";
	}
}
