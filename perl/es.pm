use strict; 
use lib "/apps/elk/scripts/perllib";
use lib "/apps/elk/scripts/perllib/share/perl5";


use JSON::XS;
use LWP::UserAgent;
use LWP::Simple;
use Sys::Hostname;
use Data::Dumper;
use URI::Escape;

sub statusCheck {
  my $target = shift;
  my $command = shift;
  my $endpoint = "$target/$command";
  my $output = get($endpoint);

  my $result;
  eval {
    $result = decode_json($output);
  };
 
  return($result);
  
};

# Run a generic ES action
sub runEsMethod {
  my $method = shift;
  my $target = shift;
  my $payload = shift;
  my $timeout = shift;
  my $ua = LWP::UserAgent->new;
  $ua->timeout($timeout);

  my $h = [ 'Content-Type' => "application/json" ];
  my $req = HTTP::Request->new($method => "$target", $h );
  $req->content($payload);
  my $resp = $ua->request($req);
  
  my $message = $resp->decoded_content;
  my $result;
  eval {
    $result = decode_json($message);
  };
  my $hits = $result->{'hits'}->{'total'};
  return ($hits, $result);

#my $data = $result->{'hits'}->{'hits'};
#foreach my $line ( @$result) {
#  foreach $key ( $line ) {
#    foreach $key1 ( keys %$key ) {
#      print "$key1\n";
#    }
#  }
#}
};

# This routine returns the name of the master node and a value of "true" if the
# node being run on is master
sub getMaster {
  my $target = shift;
  my $resp = get("$target/_cat/master");
  my ( $id, $fqdn, $ip, $master ) = split '\s', $resp;
  my $host = hostname();
  $host =~ s/\..*//g;
  $master =~ s/\..*//g;
  my $me = "false";
  if ( $host eq $master ) {
    $me = "true";
  }
  return ($me, $master );
};

# This runs a search against an index (with params if required) and flattens the structure
# contained within _source to a hash array
sub searchToHash {

  use POSIX;

  my $target = shift;
  my $index = shift;
  my $query = shift;
  my $keyfield = shift; 
  my $maxResults = shift;
  my $debug = shift;
  my $total = 0;
  my $rows =0 ;
  my $batch = 5000;
  my $scroll_id = "";
  my %output = "";
  my $hits = 0;
  my $results = "";
  my $keys = 0;     # Count number of unique keys in the hash (used to determine max results)
  if ( $maxResults eq "" ) {
    $maxResults = 500;
  }

  if ( $debug ne "" ) {
  }
  # As the result set may be large, we need to use the scroll method to get all records
  do {
    # On first run, we need to pass in index
    if ( $debug ne "" ) {
      my $date = strftime "%d/%m/%Y %H:%M:%S", localtime;
      print "Starting query at $date\n";
    }
    if ( $scroll_id eq "" ) {
      ($hits, $results ) = runEsMethod("GET","$target/$index/_search?scroll=1m&size=$batch&q=$query","",60);
    }
    # On subsequent runs, we just need the previous scroll_id
    else {
      ($hits, $results ) = runEsMethod("GET","$target/_search/scroll?scroll=1m&scroll_id=$scroll_id","",60);
    }
    # On first run, we need to pass in index
    if ( $debug ne "" ) {
      my $date = strftime "%d/%m/%Y %H:%M:%S", localtime;
      print "Completed query at $date\n";
    }
    $scroll_id = $results->{_scroll_id};  # Capture the assigned scroll id for next set of pages
    my $data = $results->{'hits'}->{'hits'};
    $rows = $#$data;
    if ( $rows >= 0 ) {
      foreach my $row ( @$data ) {
        $total++;
        next if ( $keys >= $maxResults );
        my $doc = $row->{'_source'};
        my $key = lc $doc->{$keyfield}; # Lowercase the key field to avoid confusion
        # If _id was passed as key field, then use the Elastic document id
        if ( $keyfield eq '_id' ) {
          $key = lc $row->{'_id'}; # Lowercase the key field to avoid confusion
        }
        if ( $output{$key} eq "" ) {
          $keys++;
        }
        if ( $debug ne "" ) {
          print "$key \n";
        }
        foreach my $field ( keys %$doc ) {
          $output{$key}{$field} = $doc->{$field};
        }
      }
    }
  } while( $rows > 0 and $keys <= $maxResults );

  return %output;
}

# This routine will return a single value from an agggregated query
sub getSingleValue {
  my $target = shift;      # ES Node
  my $index = shift;       # Index to query
  my $query = shift;       # Filter for query
  my $dateField = shift;   # Date Field
  my $startDate = shift;   # StartDate for Query
  my $endDate = shift;     # EndDate for Query
  my $dateFormat = shift;  # Format of Date 
  my $agg = shift;         # Aggregation type to use (max, avg, etc)
  my $field = shift;       # Field to aggregate on
  my $resultField = shift; # Output field containing value (usually value or value_string)
  my $debug = shift;       # Any value here will dump entire results as output

  # OMit the date format field is a ralative format was passed (e.g. now-1d)
  if ( $dateFormat eq "relative" ) {
    $dateFormat = "";
  } 
  else {
    $dateFormat = "\"format\" :\"$dateFormat\",";
  }
  my $json = <<EOF;
{
  "size": 0,
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "$query",
          "analyze_wildcard": true
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "$dateField": {
                  $dateFormat
                  "gte": "$startDate",
                  "lte": "$endDate"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  },
  "aggs": {
    "1": {
      "$agg": {
        "field": "$field"
      }
    }
  }
}
EOF

  my ($hits, $results ) = runEsMethod("GET","$target/$index/_search",$json,60);

  my $result = $results->{aggregations}->{1}->{$resultField};
  # Return entire results set if debug was set
  if ( $debug ne "" ) {
    print "JSON: $json \n";
    print "Results: " . Dumper($results) . "\n";
    print "Hits: " . Dumper($hits) . "\n";
  }
  
  return $result;
 
}

# This routine checks elasticsearch results writes errors to a logfile
sub check_results {
  my $output = shift;
  my $bulkLoad = shift;
  my $logfile = shift;
  my $errors = 0;
  foreach my $row ( $output->{items} ) {
    foreach my $result ( @$row ) {
      if ( $result->{index}{status} ne "" and $result->{index}{status} !~ /^(200|201)$/ ) {
        logMessage($logfile,"Error: Status:$result->{index}{status} Id:$result->{index}{_id} Reason:$result->{index}{error}{caused_by}{reason}");
        $errors++;
print ">> $errors\n";
      }
    }
  }
  if ( $errors > 0 ) {
    logMessage($logfile,"Error: $bulkLoad");
  }
  return $errors;
}


# This routine is almost the same as searchToHash but it returns a hash 
# reference instead
sub searchToHash2 {

  use POSIX;

  my $target = shift;
  my $index = shift;
  my $query = shift;
  my $maxResults = shift;
  my $debug = shift;
  my $total = 0;
  my $rows =0 ;
  my $batch = 5000;
  my $scroll_id = "";
  my %output = "";
  my $hits = 0;
  my $results = "";
  my $keys = 0;     # Count number of unique keys in the hash (used to determine max results)
  if ( $maxResults eq "" ) {
    $maxResults = 500;
  }

  do {
    # On first run, we need to pass in index
    if ( $debug ne "" ) {
      my $date = strftime "%d/%m/%Y %H:%M:%S", localtime;
      print "Starting query at $date\n";
    }
    if ( $scroll_id eq "" ) {
      ($hits, $results ) = runEsMethod("GET","$target/$index/_search?scroll=1m&size=$batch&q=$query","",60);
    }
    # On subsequent runs, we just need the previous scroll_id
    else {
      ($hits, $results ) = runEsMethod("GET","$target/_search/scroll?scroll=1m&scroll_id=$scroll_id","",60);
    }

    $scroll_id = $results->{_scroll_id};  # Capture the assigned scroll id for next set of pages
    my $data = $results->{'hits'}->{'hits'};
    $rows = $#$data;
    if ( $rows >= 0 ) {
      foreach my $row ( @$data ) {
        $total++;
        next if ( $keys >= $maxResults );
        my $doc = $row->{'_source'};
        my $id = $row->{'_id'};
        $keys++;
        foreach my $field ( keys %$doc ) {
          $output{$id}{$field} = $doc->{$field};
        }

      }
    }
  } while( $rows > 0 and $keys < $maxResults );

  return \%output;
}

1;
