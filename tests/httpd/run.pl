#!/usr/bin/perl -w

use Term::ANSIColor;
use POSIX ":sys_wait_h";

$kb_name = "http_test_".$ENV{'USER'};

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
$SIG{TERM} = 'IGNORE';

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
}

if ($pid = fork()) {
	sleep(2);
	if ($httppid = fork()) {
		if ($valgrind) {
			sleep(4);
		} else {
			sleep(1);
		}
	} else {
		my @cmd = ("../../src/http/4s-httpd", "-c", "../tests_4store.conf", "-X", "-D", "-p", "13579", $kb_name);
		if ($valgrind) {
			print("Running httpd under valgrind, output in valgrind.txt\n");
			unshift(@cmd, 'valgrind', '-v', '--trace-children=yes', '--log-file=valgrind.txt');
		}
		close STDERR;
		print(join(" ", @cmd)."\n");
		exec(@cmd);
		die "failed to exec HTTP server: $!";
	}
	print("4s-httpd running on PID $httppid\n");
	sleep(1);
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
		print("[....] $t\r[");
		my $ret = system("EPR=http://127.0.0.1:13579 LANG=C LC_ALL=C TESTPATH=../../src scripts/$t > $outdir/$t $errout");
		if ($ret == 2) {
			exit(2);
		}
		if ($ret >> 8 == 3) {
			print color 'bold yellow';
			print("SKIP");
			print color 'reset';
			print("] $t\n");
		} elsif ($test) {
			@diff = `diff exemplar/$t $outdir/$t 2>/dev/null`;
			if (@diff) {
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
				print color 'bold green';
				print("PASS");
				print color 'reset';
				print("] $t\n");
				$passes++;
			}
		} else {
			print("PROC] $t\n");
		}

	}
	print("Tests completed: passed $passes/".($fails+$passes)." ($fails fails)\n");
	$ret = kill 15, $httppid;
	if (!$ret) {
		warn("failed to kill HTTP server process, pid $httppid");
	} else {
		waitpid($httppid, 0);
	}
	$ret = kill 15, $pid;
	if (!$ret) {
		warn("failed to kill backend server process, pid $pid");
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
		system('../../src/utilities/4s-backend-setup', "$kb_name");
		close(STDERR);
		exec("../../src/backend/4s-backend", "-D", "-l", "2.0", "$kb_name");
		die "failed to exec sever: $!";
	}
}
