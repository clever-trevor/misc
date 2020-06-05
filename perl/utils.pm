use strict;

sub logMessage {
  use POSIX;
  my $log = shift;
  my $message = shift;
  my $date = strftime "%d/%m/%Y %H:%M:%S", localtime;
  print "$date $message\n";
  print $log "$date $message\n";
  return;
};

# Rename a logfile to .bak if larger than supplied threshold
sub rotateFile {
  my $input = shift;       # Input file
  my $maxSizeMb = shift;   # Max size
  $maxSizeMb = $maxSizeMb * 1024 * 1000;   # Convert to Bytes
  my $rotated = 0;
   # Check if file exists first
  if ( -f $input ) {
    my $size = -s $input;      # Get size of file in bytes
    if ( $size > $maxSizeMb ) {
      print "Log file over max Size $maxSizeMb.\n";
      # Rename file to .bak (remove existing .bak file first)
      my $newFile = "${input}.bak";
      if ( -f $newFile ) {
        unlink $newFile;
      }
      rename $input, $newFile;
      $rotated = 1;
    }
  }
  else {
    print "No file matching $input found.\n";
  }
  return $rotated;
};

# Delete files matching a pattern which have last Mod date 
# older than thresholds.
sub archiveFiles {
  my $directory = shift;  # Directory to scan
  my $pattern = shift;    # File pattern match
  my $age = shift;        # LastMod threshold in days
  my $deleted = 0;

  # List matching files
  opendir(DIR, $directory);
  my @files = grep {/$pattern/} readdir(DIR);
  closedir(DIR);

  # Check each file age
  foreach my $file ( @files ) {
    my $lastMod = -M "$directory/$file";
    # Older so deleter file
    if ( $lastMod > $age ) {
      print "$file older than $age.  Deleting.\n";
      unlink "$directory/$file";
      $deleted++;
    }
  }

  return $deleted;
};

sub replaceText {
  my $source = shift;
  my $target = shift;
  my $replace = shift;
  $source =~ s/$target/$replace/g;
  return $source;
}

# Splits a date into it's constituent parts
sub splitDate {
  my $dateIn = shift;
  my $separators = "\.\/\\: -";
  return split "[$separators]", $dateIn;
}

# Return month name from the month number. 
sub getMonthName {
  my $monthNum = shift;
  my $monthLength = shift;

  my %mons = ("1"=>'January',"2"=>'February',"3"=>'March',"4"=>'April',"5"=>'May',"6"=>'June',"7"=>'July',"8"=>'August',"9"=>'September',"10"=>'October',"11"=>'November',"12"=>'December');

  # Strip of any leading zeroes to make it easier :)
  $monthNum =~ s/^0//g;

  my $monthName = $mons{$monthNum};
  if ( $monthLength > 0 ) {
    $monthName = substr($monthName,0,$monthLength);
  }
  return $monthName;
}

# Rotates a given file name through "versions" by appending the number to the end and removing old copies
# Version .1 is the newest
sub versionFile {
  my $file = shift;
  my $versions = shift;

  return if ( $versions == 0 );

  for ( my $i = $versions ; $i > 0 ; $i-- ) {
    my $oldFile = "${file}.$i";
    if ( -f $oldFile ) {
      unlink $oldFile;
    }
    my $nextNum = $i - 1;
    my $newFile = $file;

    if ( $nextNum > 0 ) {
      $newFile = "${file}.$nextNum";
    }
    rename $newFile, $oldFile;
  }
}

1;
