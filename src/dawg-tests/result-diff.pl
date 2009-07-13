#!/usr/bin/perl -w

#use Strict;

$filea = shift;
$fileb = shift;

if (!$filea || !$fileb) {
	die "Usage: $0 <file a> <file b>";
}

@ra = &parse_res($filea);
@rb = &parse_res($fileb);

@ra = sort { &row_tostring($a) cmp &row_tostring($b) } @ra;
@rb = sort { &row_tostring($a) cmp &row_tostring($b) } @rb;

@ra = &uniq(@ra);
@rb = &uniq(@rb);

#&print_res("A ", @ra);
#&print_res("B ", @rb);

%matched = ();

for $i (@ra) {
	if (&remove($i, \@rb)) {
		$i = \%matched;
	}
}

my $diffs = 0;
$diffs += &print_res("< ", @ra);
$diffs += &print_res("> ", @rb);
exit($diffs);

sub parse_res {
	local($file) = @_;
	my %vars = ();
	my %vals = ();
	my %sols = ();

	my %seen = ();

	my %indexes;
	my $ff;

	if ($file =~ /.rdf$/i) {
		$ff = "rdfxml";
	} else {
		$ff = "turtle";
	}
	open(NTRIP, "rapper -i $ff '$file' |");

	while (<NTRIP>) {
		next if /^rapper:/;

		if (/^(.*?) (<.*?>) ([<"_].*?) .$/) {
			if ($2 eq "<http://www.w3.org/2001/sw/DataAccess/tests/result-set#variable>") {
				$node = $1;
				$var = $3;
				$var =~ s/"//g;
				$vars{$node} = $var;
			}
			if ($2 eq "<http://www.w3.org/2001/sw/DataAccess/tests/result-set#value>") {
				$sub = $1;
				$val = $3;
				$val =~ s/^_:.*/bNode/;
				$val =~ s/"@([a-z-]+)/"@\L$1/i;
				# hack to make 1.0 == 1.0e0 true
				if ($val =~ /^"(.*?)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#double>/) {
					$val = ($1 + 0.0)."^^<http://www.w3.org/2001/XMLSchema#double>";
				}
				# hack to make 1 == 01 true
				if ($val =~ /^"(.*?)"\^\^<http:\/\/www.w3.org\/2001\/XMLSchema#integer>/) {
					$val = ($1 + 0)."^^<http://www.w3.org/2001/XMLSchema#integer>";
				}
				$vals{$sub} = $val;
			}
			if ($2 eq "<http://www.w3.org/2001/sw/DataAccess/tests/result-set#index>") {
				$sub = $1;
				$val = $3;
				$val =~ s/^"//;
				$val =~ s/".*//;
				$indexes{$sub} = $val;
			}
			if ($2 eq "<http://www.w3.org/2001/sw/DataAccess/tests/result-set#binding>") {
				$sub = $1;
				$val = $3;
				if (!$sols{$sub}) {
					my @s = ();
					$sols{$sub} = \@s;
				}
				push(@{ $sols{$sub} }, $val);
			}
		}
	}

	my @res = ();

	for $k (keys %sols) {
		my @s = @{ $sols{$k} };
		my %row = ();
		for $i (@s) {
			$row{$vars{$i}} = $vals{$i};
		}
		if ($indexes{$k}) {
			$row{"_index"} = $indexes{$k};
		}
		push(@res, \%row);
	}

	return @res;
}

sub print_res {
	my $prefix = shift @_;
	my $cnt = 0;

	for $i (@_) {
		next if $i eq \%matched;
		$cnt++;
		print $prefix, &row_tostring($i)."\n";
	}

	return $cnt;
}

sub row_tostring {
	local($i) = @_;
	my $ret = "";
	my $cnt = 0;

	for $k (sort keys %{ $i }) {
		if ($cnt++) {
			$ret .= "\t";
		}
		$ret .= "$k: ".$i->{$k};
	}

	return $ret;
}

sub uniq {
	my $last = "XXX";
	my @ret = ();

	for $i (@_) {
		my $curr = &row_tostring($i);
		if ($last ne $curr) {
			push @ret, $i;
		}
		$last = $curr;
	}

	return @ret;
}

sub remove {
	local($a, $res) = @_;
	my $cnt = 0;

	for $i (@{ $res }) {
		my $this = &row_tostring($i);
		if (&row_tostring($a) eq $this) {
			splice @{ $res }, $cnt, 1;

			return 1;
		}
		$cnt++;
	}

	return 0;
}
