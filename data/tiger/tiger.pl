#!/usr/bin/env perl

use strict;
use Digest::MD5 qw(md5 md5_hex md5_base64);

sub rtrim($)
{
	my $string = shift;
	$string =~ s/\s+$//;
	return $string;
}

# _:b$bnode serialised, one-off
# _:l$lnode B-node by $lat_$long
# _:r$md5   B-node by $fullname
# _:f$feat  B-node by $feat


my $bnode = 1;
my $edition = "2006se";
my $xsd = 'http://www.w3.org/2001/XMLSchema';

if ($ARGV[0] eq '--prefix') {
  $edition = $ARGV[1];
  shift;
  shift;
}

while (<>) {
  my $line = $_;
  my $class = substr($line, 0, 1);

  if ($class eq '1') {
    my $tlid = substr($line, 5, 10) + 0;
    print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/vocab#Line> .\n";
    my $cfcc = rtrim(substr($line, 55, 3));
    print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/CFCC/$cfcc> .\n";
    my $start_long = substr($line, 190, 10) * 0.000001;
    my $start_lat = substr($line, 200, 9) * 0.000001;
    my $lnode = join ('x', (substr($line, 200, 9) + 999999999), (substr($line, 190, 10) + 999999999));
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#start> _:l$lnode .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#lat> \"$start_lat\"^^<$xsd#decimal> .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#long> \"$start_long\"^^<$xsd#decimal> .\n";
    my $end_long = substr($line, 209, 10) * 0.000001;
    my $end_lat = substr($line, 219, 9) * 0.000001;
    my $lnode = join ('x', (substr($line, 210, 9) + 999999999), (substr($line, 209, 10) + 999999999));
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#end> _:l$lnode .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#lat> \"$end_lat\"^^<$xsd#decimal> .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#long> \"$end_long\"^^<$xsd#decimal> .\n";

    my $prefix = rtrim(substr($line, 17, 2));
    my $name = rtrim(substr($line, 19, 30));
    my $type = rtrim(substr($line, 49, 4));
    my $suffix = rtrim(substr($line, 53, 2));
    my $fullname = $name;
    $fullname = "$prefix $fullname" unless length($prefix) == 0;
    $fullname = "$fullname $type" unless length($type) == 0;
    $fullname = "$fullname $suffix" unless length($suffix) == 0;
    my $node;
    if (length($fullname) != 0) {
      my $digest = md5_hex($fullname);
      $node = "_:r$digest";
      print "$node <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n";
      print "$node <http://www.census.gov/tiger/2002/vocab#name> \"$name\" .\n" unless length($name) == 0;
      print "$node <http://www.w3.org/2000/01/rdf-schema#label> \"$fullname\" .\n" unless length($fullname) == 0;
      print "$node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/featid/type/$type> .\n" unless length($type) == 0;
      print "$node <http://www.census.gov/tiger/2002/vocab#directionPrefix> <http://www.census.gov/tiger/2002/featid/direction/$prefix> .\n" unless length($prefix) == 0;
      print "$node <http://www.census.gov/tiger/2002/vocab#directionSuffix> <http://www.census.gov/tiger/2002/featid/direction/$suffix> .\n" unless length($suffix) == 0;
    }

    my $zipl = rtrim(substr($line, 106, 5));
    if (length($zipl) != 0) {
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#zip> \"$zipl\" .\n";
    }
    my $zipr = rtrim(substr($line, 111, 5));
    if (length($zipr) != 0 && $zipl ne $zipr) {
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#zip> \"$zipr\" .\n";
    }

    my $lstate = substr($line, 130, 2);
    my $rstate = substr($line, 132, 2);
    my $lcounty = substr($line, 134, 3);
    my $rcounty = substr($line, 137, 3);
    print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#state> <http://www.census.gov/tiger/2002/fips/$lstate> .\n";
    print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#county> <http://www.census.gov/tiger/2002/fips/$lstate/$lcounty> .\n";
    if ($lstate ne $rstate) {
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#state> <http://www.census.gov/tiger/2002/fips/$rstate> .\n";
    }
    if ($lstate ne $rstate || $lcounty ne $rcounty) {
      print "<http://www.census.gov/tiger/tlid/$tlid> <http://www.census.gov/tiger/2002/vocab#county> <http://www.census.gov/tiger/2002/fips/$rstate/$rcounty> .\n";
    }

  } elsif ($class eq '4') {
    my $tlid = substr($line, 5, 10) + 0;
    my $feat1 = substr($line, 18, 8) + 0;
    my $feat2 = substr($line, 26, 8) + 0;
    my $feat3 = substr($line, 34, 8) + 0;
    my $feat4 = substr($line, 42, 8) + 0;
    my $feat5 = substr($line, 50, 8) + 0;
    print "_:f$feat1 <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $feat1 == 0;
    print "_:f$feat2 <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $feat2 == 0;
    print "_:f$feat3 <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $feat3 == 0;
    print "_:f$feat4 <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $feat4 == 0;
    print "_:f$feat5 <http://www.census.gov/tiger/2002/vocab#path> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $feat5 == 0;

  } elsif ($class eq '5') {
    my $file = substr($line, 1, 9) + 0;
    my $feat = substr($line, 10, 8) + 0;
    my $prefix = rtrim(substr($line, 18, 2));
    my $name = rtrim(substr($line, 20, 30));
    my $type = rtrim(substr($line, 50, 4));
    my $suffix = rtrim(substr($line, 18, 2));
    my $fullname = $name;
    $fullname = "$prefix $fullname" unless length($prefix) == 0;
    $fullname = "$fullname $type" unless length($type) == 0;
    $fullname = "$fullname $suffix" unless length($suffix) == 0;
    print "_:f$feat <http://www.census.gov/tiger/2002/vocab#name> \"$name\" .\n";
    print "_:f$feat <http://www.w3.org/2000/01/rdf-schema#label> \"$fullname\" .\n" unless length($fullname) == 0;
    print "_:f$feat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/featid/type/$type> .\n" unless length($type) == 0;
    print "_:f$feat <http://www.census.gov/tiger/2002/vocab#directionPrefix> <http://www.census.gov/tiger/2002/featid/direction/$prefix> .\n" unless length($prefix) == 0;
    print "_:f$feat <http://www.census.gov/tiger/2002/vocab#directionSuffix> <http://www.census.gov/tiger/2002/featid/direction/$suffix> .\n" unless length($suffix) == 0;

  } elsif ($class eq '7') {
    my $file = substr($line, 5, 5) + 0;
    my $id = substr($line, 10, 10) + 0;
    print "<http://www.census.gov/tiger/$edition/landmark/$file/$id> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/vocab#Landmark> .\n";
    my $cfcc = rtrim(substr($line, 21, 3));
    print "<http://www.census.gov/tiger/$edition/landmark/$file/$id> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/CFCC/$cfcc> .\n";
    my $name = rtrim(substr($line, 24, 30));
    print "<http://www.census.gov/tiger/$edition/landmark/$file/$id> <http://www.census.gov/tiger/2002/vocab#name> \"$name\" .\n" unless length($name) == 0;
    my $long = substr($line, 54, 10) * 0.000001;
    my $lat = substr($line, 64, 9) * 0.000001;
    my $lnode = join ('x', (substr($line, 64, 9) + 999999999), (substr($line, 54, 10) + 999999999));
    if ($lat != 0 && $long != 0) {
      print "<http://www.census.gov/tiger/$edition/landmark/$file/$id> <http://www.census.gov/tiger/2002/vocab#location> _:l$lnode .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#lat> \"$lat\"^^<$xsd#decimal> .\n";
      print "_:l$lnode <http://www.census.gov/tiger/2002/vocab#long> \"$long\"^^<$xsd#decimal> .\n";
    }
  
  } elsif ($class eq '8') {
    my $file = substr($line, 5, 5) + 0;
    my $landmark = substr($line, 25, 10) + 0;
    my $cenid = substr($line, 10, 5);
    my $polygon = substr($line, 15, 10) + 0;
    print "<http://www.census.gov/tiger/$edition/polygon/$cenid/$polygon> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/vocab#Polygon> .\n";
    print "<http://www.census.gov/tiger/$edition/landmark/$file/$landmark> <http://www.census.gov/tiger/2002/vocab#area> <http://www.census.gov/tiger/$edition/polygon/$cenid/$polygon> .\n";
  
  } elsif ($class eq 'A') {
    my $file = substr($line, 5, 5) + 0;
    my $cenid = substr($line, 10, 5);
    my $polygon = substr($line, 15, 10) + 0;
    my $state = substr($line, 25, 2);
    my $county = substr($line, 27, 3);
    print "<http://www.census.gov/tiger/$edition/polygon/$cenid/$polygon> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/vocab#Polygon> .\n";
    print "<http://www.census.gov/tiger/2002/fips/$state/$county> <http://www.census.gov/tiger/2002/vocab#area> <http://www.census.gov/tiger/$edition/polygon/$cenid/$polygon> .\n";
    print "<http://www.census.gov/tiger/2002/fips/$state> <http://www.census.gov/tiger/2002/vocab#area> <http://www.census.gov/tiger/$edition/polygon/$cenid/$polygon> .\n";

  } elsif ($class eq 'C') {
    my $subtype = substr($line, 24, 1);
    my $name = rtrim(substr($line, 62, 60));
    if ($subtype eq 'C') {
      my $lsadc = substr($line, 22, 2);
      my $cc = substr($line, 19, 2);
      my $state = substr($line, 5, 2);
      my $county = substr($line, 7, 3);
      print "<http://www.census.gov/tiger/2002/fips/$state/$county> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/fips/class_code/$cc> .\n";
      print "<http://www.census.gov/tiger/2002/fips/$state/$county> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/fips/area_description/$lsadc> .\n";
      print "<http://www.census.gov/tiger/2002/fips/$state/$county> <http://www.census.gov/tiger/2002/vocab#name> \"$name\" .\n";

    } elsif ($subtype eq 'S') {
      my $lsadc = substr($line, 22, 2);
      my $state = substr($line, 5, 2);
      print "<http://www.census.gov/tiger/2002/fips/$state> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.census.gov/tiger/2002/fips/area_description/$lsadc> .\n";
      print "<http://www.census.gov/tiger/2002/fips/$state> <http://www.census.gov/tiger/2002/vocab#name> \"$name\" .\n";
    }

  } elsif ($class eq 'I') {
    my $tlid = substr($line, 10, 10) + 0;
    my $cenidl = substr($line, 40, 5);
    my $polygonl = substr($line, 45, 10) + 0;
    print "<http://www.census.gov/tiger/$edition/polygon/$cenidl/$polygonl> <http://www.census.gov/tiger/2002/vocab#leftOf> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $polygonl == 0;
    my $cenidr = substr($line, 55, 5);
    my $polygonr = substr($line, 60, 10) + 0;
    print "<http://www.census.gov/tiger/$edition/polygon/$cenidr/$polygonr> <http://www.census.gov/tiger/2002/vocab#rightOf> <http://www.census.gov/tiger/tlid/$tlid> .\n" unless $polygonr == 0;

  }

}

