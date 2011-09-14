#!/usr/bin/perl -w

use Term::ANSIColor;
use POSIX ":sys_wait_h";

$kb_name = "query_test_".$ENV{'USER'};

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";
my @tests = ();
my $errs = 1;

$SIG{USR2} = 'IGNORE';
$SIG{TERM} = 'IGNORE';

if ($pid = fork()) {
	sleep(2);
	if ($httppid = fork()) {
		sleep(1);
	} else {
		#exec('valgrind', '--tool=helgrind', '--num-callers=20', '--trace-children=yes', '--log-file=valgrind.txt', "../../src/http/4s-httpd", "-X", "-D", "-p", "13579", $kb_name);
		exec("../../src/http/4s-httpd", "-X", "-D", "-p", "13579", $kb_name);
		die "failed to exec HTTP sever: $!";
	}
	print("4s-httpd running on PID $httppid\n");
	sleep(1);
	my $fails = 0;
	my $passes = 0;
	my $children = 0;
	for my $t (1..40) {
		unless (fork()) {
			exec("EPR=http://localhost:13579 LANG=C LC_ALL=C TESTPATH=../../src ./query.sh > results-$t.txt");
		}
		$children++;
	}
	while (my $cpid = wait()) {
		$children--;
		last if $cpid == -1 || $children == 0;
		# do nothing
	}
	for my $t (1..40) {
		unlink("results-$t.txt");
	}
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
	close(STDERR);
	exec("../../src/backend/4s-backend", "-D", $kb_name);
	die "failed to exec sever: $!";
}
