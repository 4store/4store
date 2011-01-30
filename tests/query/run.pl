#!/usr/bin/perl -w

use Term::ANSIColor;

$kb_name = "query_test_".$ENV{'USER'};

# names of tests that require LAQRS support
my @need_laqrs = ('count', 'union-nobind');

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";
$outdir = "results";
$test = 1;
my @tests = ();
my $errs = 1;
my $spawn = 1;
my $valgrind = 0;

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
	} elsif ($ARGV[0] eq "--valgrind") {
		shift;
		$valgrind = 1;
	}
	while ($t = shift) {
		$t =~ s/^(.\/)?scripts\///;
		push @tests, $t;
	}
}
mkdir($outdir);

if (!@tests) {
	@tests = `ls scripts`;
	chomp @tests;
}

if (`../../src/frontend/4s-query -h 2>&1 | grep LAQRS` eq '') {
	my %tmp;
	foreach $t (@tests) { $tmp{$t} ++; }
	foreach $t (@need_laqrs) { delete $tmp{$t}; }
	@tests = sort keys %tmp;
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
		my $ret = system("FORMAT=ascii LANG=C LC_ALL=C TESTPATH=../../src scripts/$t $kb_name > $outdir/$t $errout");
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
		} else {
			print("[PROC] $t\n");
		}

	}
	print("Tests completed: passed $passes/".($fails+$passes)." ($fails fails)\n");
	$ret = kill 15, $pid;
	if (!$ret) {
		warn("failed to kill server process, pid $pid");
	} else {
		waitpid($pid, 0);
	}

	if ($fails) {
		exit(1);
	}

	exit(0);
} else {
	# child
	if ($spawn) {
		my @pre = ();
		if ($valgrind) {
			push(@pre, "valgrind");
			push(@pre, " --leak-check=full");
			if (`uname -a` =~ /Darwin/) {
				push(@pre, "--dsymutil=yes");
			}
		}
		push(@pre, "../../src/backend/4s-backend");
print(join(" ", @pre)."\n");
		exec(@pre, "-D", "$kb_name");
		die "failed to exec sever: $!";
	}
}
