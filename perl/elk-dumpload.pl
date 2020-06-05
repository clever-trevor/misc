#!/usr/bin/perl
 
# This script dumps an index and mappings to text files, which can then be reloaded onto another
# server with the LOAD option.
# Example
# ./elk-dumpload.pl DUMP .kibana
# cp /apps/elk/data/esdump* <target host>
# ./elk-dumpload.pl LOAD .kibana
# 
# Add an optional number on the end to rotate previous backups through a number of versions
# ./elk-dumpload.pl DUMP .kibana 7 
#   Will keep last 7 dumps online

use lib "z:\\perl";
use es;
use utils;
use Data::Dumper;
#use strict;
use JSON;
 
my $es = "http://localhost:9200";
 
my $method = @ARGV[0];
my $index = @ARGV[1];
my $backups = @ARGV[2];   # Number of previous backups to keep
 
my $output = "z:\\elk\\dump";

if ( $index eq "" ) {
  print "Pass two variables; {DUMP|LOAD} and <INDEX_NAME>\n";
  exit 0;
}
 
# Keep no old copies by default
if ( $backups eq "" ) {
  $backups = 0;
}

my $data = "$output/esdump-${index}-data.json";
my $mapping = "$output/esdump-${index}-mappings.json";
 
if ( $method eq "DUMP" ) {

  versionFile($mapping, $backups ) ;
  versionFile($data, $backups ) ;
 
  # Get all mappings and dump to file
  (my $hits, my $results ) = runEsMethod("GET", "$es/$index/_mappings" , "", 20);
  open(JSON,">$mapping");
  print JSON to_json($results->{$index});
  close(JSON);
 
  # Get all documents and dump to file
  open(JSON,">$data");
  (my $hits, my $results ) = runEsMethod("POST", "$es/$index/_search/?size=10000" , "", 20);
  my $data = $results->{'hits'}->{'hits'};
 
  # Write documents out in format so that they can be bulk loaded
  foreach my $entry ( @$data ) {
    my $action = "{ \"index\" : { \"_index\" : \"$entry->{_index}\", \"_type\" : \"$entry->{_type}\", \"_id\" : \"$entry->{_id}\" } }";
    print JSON "$action\n";
 
    my $json = encode_json $entry->{_source};
    print JSON "$json\n";
 
  }
  close(JSON);
}
 
elsif ( $method eq "LOAD" ) {
 
  print "The load option will DELETE index $index!!!\n";
  print "Enter YES (in capitals) to continue\n";
  my $confirm = <STDIN>;
  chomp $confirm;
  if ( $confirm ne "YES" ) {
    print "Exiting with no action\n";
    exit 0;
  }

  # Recreate the index
  (my $hits, my $results ) = runEsMethod("DELETE", "$es/$index" , "" , 5);
  (my $hits, my $results ) = runEsMethod("POST", "$es/$index" , "" , 5);
 
  # Load the mappings first
  open my $fh, '<', $mapping or die "Can't open file $!";
  my $mappings = do { local $/; <$fh> };
  my $decoded = decode_json($mappings);
 
  # No bulk facility so we have to load mappings individually
  foreach my $mapping ( keys %{$decoded} ) {
    my $json = encode_json $decoded->{$mapping};
    my $payload = "{ \"$mapping\" : $json }";
    (my $hits, my $results ) = runEsMethod("PUT", "$es/$index" , $payload, 5);
	print Dumper($results);
  }
 
  # Now load the documents via the bulk facility
  open my $fh, '<', $data or die "Can't open file $!";
  my $data_load = do { local $/; <$fh> };
  (my $hits, my $results ) = runEsMethod("POST", "$es/_bulk/" , $data_load, 120);
  print Dumper($results);
}
