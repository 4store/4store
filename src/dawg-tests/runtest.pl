#!/usr/bin/perl -w

my $summary = 0;
my $optlevel = 0;

my $pass = 0;
my $fail = 0;
my $exc = 0;
my $total = 0;

$SIG{USR2} = 'IGNORE';

print <<EOB;
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN"
 "http://www.w3.org/TR/REC-html40/loose.dtd">
 <html>
   <head>
     <title>4store SPARQL test results</title>
   </head>
   <body>
     <h1>4store SPARQL test results</h1>

     <p>Results for approved tests are shown with bold colours, unapproved tests in pale.</p>

     <table border cellpadding="4" cellspacing="0" cellborder="1">
       <tr><th>Name</th><th>Comment</th><th>Result</th></tr>
EOB

system("mkdir -p explan");
system("rm -f explan/*");

open(TEXT, "> tests.html") || die "Cannot create text output file: $!";
open(EARL, "> earl.ttl") || die "Cannot create EARL output file: $!";

$rev = "4s-".`make -f rev.mk`;
chomp $rev;

print EARL <<EOB;
\@prefix : <http://www.garlik.com/#> .
\@prefix doap: <http://usefulinc.com/ns/doap#> .
\@prefix earl: <http://www.w3.org/ns/earl#> .
\@prefix data: <http://www.w3.org/2001/sw/DataAccess/tests/data-r2/> .
\@prefix foaf: <http://xmlns.com/foaf/0.1/> .
\@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
\@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

:steve a foaf:Person ;
  foaf:homepage <http://plugin.org.uk/> ;
  rdfs:seeAlso <http://plugin.org.uk/swh.xrdf> ;
  foaf:name "Steve Harris" .

:4store a doap:Project ;
  doap:name "4store" ;
  doap:release [
    a doap:Version ;
    doap:name "$rev" ;
  ] .

:skipped a earl:ResultProperty ;
  earl:validity earl:NotTested ;
  earl:confidence earl:Medium .
EOB

my %excuses;

open(EXC, "excuses.txt") || die "Cannot find excuses file: $!";
while (<EXC>) {
	next if /^\s*#/;
	if (/^(.*?)\s+(.*)/) {
		$excuses{$1} = $2;
	}
}

while ($file = shift) {
	my %name;
	my %comment;
	my %query;
	my %data;
	my %action;
	my %result;
	my %approval;
	my %negative;

	if ($file eq "--summary") {
		$summary = 1;
		next;
	}

	print STDERR "Parsing <file:$file>\n";
	open(MANIF, "rapper -q -i turtle file:$file |") || die "Cannot open $file: $!";

	while (<MANIF>) {
		if (m/^(.*?) <[^ ]*?#(.*?)> [<"]?(.*?)[>"]? .$/) {
			if ($2 eq 'name') {
				$name{$1} = $3;
			} elsif ($2 eq 'comment') {
				$comment{$1} = $3;
			} elsif ($2 eq 'action') {
				$action{$1} = $3;
			} elsif ($2 eq 'result') {
				$result{$1} = $3;
			} elsif ($2 eq 'approval') {
				$approval{$1} = $3;
			} elsif ($2 eq 'query') {
				$node = $1;
				$uri = $3;
				$uri =~ s/^file://;
				$query{$node} = `cat $uri`;
			} elsif ($2 eq 'data' || $2 eq 'graphData') {
				if (!$data{$1}) {
					my @list = ();
					$data{$1} = \@list;
				}
				push @{ $data{$1} }, $3;
			} elsif ($2 eq 'type') {
				my $subj = $1;
				if ($3 =~ /Negative/) {
					$negative{$subj} = 1;
				}
			} elsif ($2 eq 'first' || $2 eq 'rest' ||
			         $2 eq 'entries' || $2 eq 'approvedBy' ||
				 $2 eq 'seeAlso' || $2 eq 'label') {
				# ignore
			} elsif ($2 eq 'requires') {
				my $uri = $1;
				my $feat = $3;
				$feat =~ s/.*[#\/]//;
				$comment{$uri} .= ", <i>requires :$feat</i>";
				$sname = "\L$name{$uri}";
				$sname =~ s/[^a-z0-9]+/-/g;
				if ($feat eq "date") {
					$excuses{$sname} .= "Requires xsd:date";
				}
			} else {
				$comment{$1} .= " <b>ignoring :$2 property</b>";
			}
		} elsif (m/^rapper:/) {
		} else {
			print "??? $_";
		}
	}
	close(MANIF);

# [  mf:name    "dawg-triple-pattern-001" ;
#      rdfs:comment
#         "Simple triple match" ;
#    mf:action
#      [ qt:query  <dawg-tp-01.rq> ;
#        qt:data   <data-01.ttl> ] ;
#    mf:result  <result-tp-01.ttl> ;
#    dawgt:approval dawgt:Approved
#  ]

	for $i (sort keys %name) {
		#next if (!$approval{$i} || $approval{$i} !~ /#Approved/); # XXX skips unapproved tests
		$sname = "\L$name{$i}";
		$sname =~ s/[^a-z0-9]+/-/g;
		next if ($summary && $excuses{$sname});
		print "       <tr>";
		print "<td><a href=\"tests.html#$sname\">$name{$i}</a></td>";
		if ($comment{$i}) {
			$c = $comment{$i};
			print "<td><small>$c</small></td>";
		} else {
			print "<td></td>";
		}
		$total++;
		if ($excuses{$sname}) {
			$exc++;
			print "<td style=\"background-color: #EEEE99\">".$excuses{$sname}."</td>";
			my $qname = $uri;
			$qname =~ s/.*data\///;
			$qname =~ s/>$//;
			print EARL <<EOB;

[ a earl:Assertion;
  earl:assertedBy :steve ;
  earl:result [
    a earl:TestResult ;
    earl:outcome :skipped ;
    rdfs:comment "$excuses{$sname}" ;
  ] ;
  earl:subject :4store ;
  earl:test $i ;
] .
EOB
		} else {
			my $q = "";
			if ($query{$action{$i}}) {
				$q = $query{$action{$i}};
			} else {
				my $uri = $action{$i};
				$uri =~ s/^file://;
				$q = `cat $uri`;
			}
			&qtest($i, $sname, $data{$action{$i}}, $q, $result{$i}, $approval{$i}, $negative{$i});
		}
		print "</tr>\n";
	}

}

close(TEXT);

print <<EOB;
    </table>

    <h2>Summary</h2>
    <table border>
      <tr><th></th><th>Num</th><th>\%age</th></tr>
EOB

$ppass = sprintf("%.1f", (100.0*$pass)/($pass+$fail));
$pfail = sprintf("%.1f", (100.0*$fail)/($pass+$fail));

print STDERR "Passed:  $pass\n";
print STDERR "Failed:  $fail\n";
print STDERR "Excused: $exc\n";

print "<tr><th>Passed</th><td align=\"right\">$pass</td><td align=\"right\">$ppass</td></tr>\n";
print "<tr><th>Failed</th><td align=\"right\">$fail</td><td align=\"right\">$pfail</td></tr>\n";
print "<tr><th>Excused</th><td align=\"right\">$exc</td><td align=\"right\">&mdash;</td></tr>\n" if !$summary;
print "<tr><th>Total</th><td align=\"right\">$total</td><td align=\"right\"></td></tr>\n";

print <<EOB;
    </table>
  </body>
</html>
EOB

sub qtest {
	local($uri, $name, $data, $query, $result, $approval, $negative) = @_;

	$query = "" if !$query;
	my $approved = 0;
	if ($approval && $approval =~ /#Approved/i) {
		$approved = 1;
	}
	if ($approved) {
		$failcol = '#FF0000';
		$passcol = '#00FF00';
	} else {
		$failcol = '#FFDDDD';
		$passcol = '#BBFFBB';
	}
	$tmpfile = "/tmp/tc-$$.ttl";
	print("\n<!-- ");
	&exec($name, "../frontend/4s-delete-model dawg_test --all");
	for $file (@{ $data }) {
		if (&exec($name, "../frontend/4s-import dawg_test $file")) {
			&fail($uri);
			if ($summary || !(-e "explan/$name-error.txt")) {
				print("-->\n<td style=\"background-color: $failcol\">FAIL/td>");
			} else {
				print("-->\n<td style=\"background-color: $failcol\">FAIL <a href=\"explan/$name-error.txt\">err</a></td>");
			}

			return;
		}
	}
	print TEXT "<h2><a name=\"$name\">$name</a></h2>\n";
	$query =~ s/offset\s+(\d+)\s+limit\s+(\d+)/limit $2 offset $1/gis;
	$htmlquery = $query;
	$htmlquery =~ s/&/\&amp;/g;
	$htmlquery =~ s/</\&lt;/g;
	$htmlquery =~ s/>/\&gt;/g;
	print TEXT "<h3>Data</h3>\n";
	for $file (@{ $data }) {
		print TEXT "<a href=\"$file\">$file</a>\n";
	}
	print TEXT "<h3>Query</h3>\n";
	print TEXT "<pre>$htmlquery</pre>\n";
	$query =~ s/'/'\\''/g;
	my $execret = &exec($name, "../frontend/4s-query dawg_test -f testcase '$query' > $tmpfile");
	my $expected = $negative ? 1 : 0;
	if ($execret != $expected) {
		&fail($uri);
		cp($tmpfile, "explan/$name-result.ttl");
		if ($summary || !(-e "explan/$name-error.txt")) {
			print("-->\n<td style=\"background-color: $failcol\">FAIL <small>silent</small></td>");
		} else {
			print("-->\n<td style=\"background-color: $failcol\">FAIL <a href=\"explan/$name-error.txt\">err</a></td>");
		}
		unlink($tmpfile);

		return;
	}
	print(" -->\n");
	$res_tmp = "";
	$cmp_res = "";
	if ($result && $result =~ /\.srx$/) {
		$res_tmp = "/tmp/$$.ttl";
		system("xsltproc srx2ttl.xsl $result > $res_tmp");
		$cmp_res = $res_tmp;
	} else {
		$cmp_res = $result;
	}
	my $isotest = ($query =~ /CONSTRUCT/ || $query =~ /ASK/);
	if ($result) {
		if ($isotest) {
			my $file = $cmp_res;
			$file =~ s/^file:..//;
			open(RF, "$file");
			print TEXT "<h3>Results</h3>";
			print TEXT "<pre>";
			while (<RF>) {
				s/&/\&amp;/g;
				s/</\&lt;/g;
				s/>/\&gt;/g;
				print TEXT $_;
			}
			close(RF);
			print TEXT "</pre>";
		} else {
			system("roqet -q -r xml -e 'PREFIX : <http://www.w3.org/2001/sw/DataAccess/tests/result-set#> SELECT ?row ?var ?val WHERE { ?row :variable ?var ; :value ?val }' -s '$cmp_res' > results.xml");
			open(RES, "xsltproc sparql2html.xslt results.xml |");
			while (<RES>) {
				print TEXT $_;
			}
			close(RES);
			unlink("results.xml");
		}
	}
	if ($result) {
		if ($isotest) {
			$ntfilea = "/tmp/nta-$$.nt";
			$ntfileb = "/tmp/ntb-$$.nt";
			system("rapper -g -o ntriples -q $tmpfile | sed 's/_\\([0-9][0-9]*\\) /b\\1 /' > $ntfilea");
			#system("rapper -g -o ntriples -q $tmpfile > $ntfilea");
			system("rapper -g -o ntriples -q $cmp_res > $ntfileb");
#print STDERR "TP=$tmpfile CR=$cmp_res\n"
#if ($name eq "dawg-construct-reification-1") {
#print STDERR "$ntfilea $ntfileb\n";
#exit();
#}
		}
		if ($isotest && `./rdfdiff.py $ntfilea $ntfileb` !~ /yes/) {
			&fail($uri);
			print("<td style=\"background-color: $failcol\">FAIL <a href=\"$result\">A</a>&nbsp;<a href=\"explan/$name.diff\"><b>&mdash;</b></a>&nbsp;<a href=\"explan/$name-result.ttl\">B</a></td>");
			my $file = $tmpfile;
			$file =~ s/^file:..//;
			cp($file, "explan/$name-result.ttl");
			#print STDERR "copied to explan/$name-result.ttl\n";
		} elsif (!$isotest && system("./result-diff.pl $tmpfile $cmp_res > explan/$name.diff 2> explan/$name-error.txt")) {
			&fail($uri);
			if ($summary) {
				print("<td style=\"background-color: $failcol\">FAIL</td>");
			} else {
				print("<td style=\"background-color: $failcol\">FAIL <a href=\"$result\">A</a>&nbsp;<a href=\"explan/$name.diff\"><b>&mdash;</b></a>&nbsp;<a href=\"explan/$name-result.ttl\">B</a></td>");
			}
			cp($tmpfile, "explan/$name-result.ttl");
		} else {
			&pass($uri);
			print("<td style=\"background-color: $passcol\">PASS");
			if ($result && !$summary) {
				cp($tmpfile, "explan/$name-result.ttl");
				print(" <a href=\"$result\">A</a> <a href=\"explan/$name-result.ttl\">B</a>");
			}
			print("</td>");
			unlink("explan/$name.diff");
			unlink("explan/$name-error.txt");
		}
		if ($isotest) {
			unlink($ntfilea);
			unlink($ntfileb);
			undef($ntfilea);
		}
	} else {
		&pass($uri);
		print("<td style=\"background-color: $passcol\">PASS");
		print("</td>");
	}
	unlink($res_tmp) if $res_tmp;
	unlink($tmpfile);
}

sub exec {
	local ($name, $cmd) = @_;

	$xmlcmd = $cmd;
	$xmlcmd =~ s/--/__/;
	print "$xmlcmd\n";
	if (system($cmd." 2>explan/$name-error.txt")) {
		$out = $?;
		open(ERR, ">> explan/$name-error.txt");
		if ($out == 35328) {
			print STDERR "$cmd failed: $out\n";
			print ERR "Bus error\n";
			print ERR "-----------\n$cmd failed: $out\n";

			exit 1;
		}
		print ERR "-----------\n$cmd failed: $out\n";

		return 1;
	}
	unlink("explan/$name-error.txt");

	return 0;
}

sub pass {
	local($uri) = @_;

	$pass++;
	$uri =~ s/.*data\///;
	#$uri =~ s/>$//;
	print EARL <<EOB;

[ a earl:Assertion;
  earl:assertedBy :steve ;
  earl:result [
    a earl:TestResult ;
    earl:outcome earl:pass ;
  ] ;
  earl:subject :4store ;
  earl:test $uri ;
] .
EOB
}

sub fail {
	local($uri) = @_;

	$fail++;
	$uri =~ s/.*data\///;
	#$uri =~ s/>$//;
	print EARL <<EOB;

[ a earl:Assertion;
  earl:assertedBy :steve ;
  earl:result [
    a earl:TestResult ;
    earl:outcome earl:fail ;
  ] ;
  earl:subject :4store ;
  earl:test $uri ;
] .
EOB
}

sub cp {
	local($a, $b) = @_;

	if (system("cp '$a' '$b'") != 0) {
		die "cannot copy $a to $b: $!\n";
	}
}
