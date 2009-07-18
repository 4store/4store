#!/usr/bin/perl -w

use Term::ANSIColor;

$kb_prefix = "test_import_";

$dirname=`dirname '$0'`;
chomp $dirname;
chdir($dirname) || die "cannot cd to $dirname";
$outdir = "results";
$pre = "";
$test = 1;
$valgrind = 0;
my @tests = ();
my $errs = 1;

$SIG{'USR2'} = 'IGNORE';
$SIG{'INT'} = 'handler';
$SIG{'KILL'} = 'handler';

while ($arg = shift @ARGV) {
	if ($arg eq "--exemplar") {
		$outdir = "exemplar";
		$test = 0;
		$errs = 0;
	} elsif ($arg eq "--outdir") {
		$outdir = shift;
		$test = 1;
	} elsif ($arg eq "--valgrind-frontend") {
		$pre .= " PRECMD=valgrind";
		$valgrind = 1;
	} elsif ($arg eq "--valgrind-backend") {
		$pre .= " BACKPRECMD=valgrind";
		$valgrind = 1;
	} else {
		$arg =~ s/^(.\/)?scripts\///;
		push @tests, $arg;
	}
}
mkdir($outdir);

if (!@tests) {
	@tests = `ls scripts`;
}

my $errors = 0;

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
		$errout = "2>/dev/null";
	}
	$kb_name = $kb_prefix . $t;
	$kb_name =~s/-/_/g;
	printf("[....] $t\r");
	$ret = system("FORMAT=text LANG=C TESTPATH=../../src ${pre} scripts/$t $kb_name > $outdir/$t $errout");
	if ($ret == 2) {
		printf("\n");
		system("./test-stop.sh $kb_name");
		system("../../src/utilities/4s-backend-destroy $kb_name 2>/dev/null");
		exit(2);
	}
	my $destroy = 1;
	if ($test) {
		#system("../../src/backend/slistdump --check s /var/lib/4store/$kb_name/*/s.slist >> $outdir/$t");
		@diff = `diff exemplar/$t $outdir/$t 2>/dev/null`;
		if (@diff) {
			$destroy = 0;
			$errors = 1;
			print("[");
			print color 'bold red';
			print("FAIL");
			print color 'reset';
			print("] $t\n");
			open(RES, ">> $outdir/$t-errs") || die 'cannot open error file';
			print RES "\n";
			print RES @diff;
			close(RES);
		} else {
			print("[");
			print color 'bold green';
			print("PASS");
			print color 'reset';
			print("] $t\n");
			if ($valgrind) {
				system("grep 'ERROR SUMMARY' $outdir/$t-errs | grep -v '0 errors from 0 contexts'");
			} else {
				unlink("$outdir/$t-errs");
			}
		}
	} else {
		print("[PROC] $t\n");
	}
	system("../../src/utilities/4s-backend-destroy $kb_name 2>/dev/null") if $destroy;
}

exit($errors);

sub handler {
	local($sig) = @_;
	print "Caught a SIG$sig\n";
	exit(0);
}
