##############################################################################
# Copyright (c) Members of the EGEE Collaboration. 2010.
# See http://www.eu-egee.org/partners/ for details on the copyright
# holders.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################
#
# NAME :        Common.pm
#
# DESCRIPTION : Common functionality for schema upgrades
#
# AUTHORS :     Alejandro Alvarez Ayllón, based on Sophie Lemaitre
#
##############################################################################
package Common;

use DBI;
use strict;
use warnings;
use Getopt::Long;
use Env qw(ORACLE_HOME);

# Connects to the database
# It expects five parameters: database name, user, password, vendor (MySQL or Oracle) and host
sub connectToDatabase($$$$$) {
  my ($db, $dbuser, $dbpasswd, $db_vendor, $host) = @_;

  if ($db_vendor eq "Oracle") {
	  return DBI->connect("DBI:Oracle:$host", $dbuser, $dbpasswd, { RaiseError => 1, PrintError => 0, AutoCommit => 0 })
      or die "Can't open database $host:$dbuser: $DBI::errstr\n";
  }

  if ($db_vendor eq "MySQL") {
    return DBI->connect("dbi:mysql:$db:$host", $dbuser, $dbpasswd, { RaiseError => 1, PrintError => 0, AutoCommit => 0 })
      or die "Can't open database $db:$dbuser: $DBI::errstr\n";
  }
}

# Gets the schema version
sub querySchemaVersion($) {
  my ($dbh) = @_;
  my ($sth,$ret);
  my @data;
  eval {
    $sth = $dbh->prepare("SELECT major, minor, patch FROM schema_version");
    if ($sth) {
      $ret = $sth->execute();
    }
  };
  if (!$ret) {
    die "Could not query cns schema version\n";
  }

  @data = $sth->fetchrow_array();
  print $data[0]."\t".$data[1]."\t".$data[2]."\n";
  $sth->finish;
}

# Parse the arguments and create the DB connection
sub main() {
  my ($db_vendor, $db, $db_host, $user, $pwd_file, $verbose, $show_version);
  GetOptions("db-vendor:s", => \$db_vendor,
             "db-host:s", => \$db_host,
             "db:s", => \$db,
             "user:s", => \$user,
             "pwd-file:s", => \$pwd_file,
             "verbose" => \$verbose,
             "show-version-only" => \$show_version );

  # Defaults
  $user = "root" unless(defined $user);

  # Check consistence
  die "The database vendor must be specified. It can either be \'Oracle\' or \'MySQL\'" unless(defined $db_vendor);
  die "The file containing the LFC/DPNS database password must be specified" unless(defined $pwd_file);

  if ($db_vendor eq "MySQL") {
    die "The MySQL server host must be specified" unless(defined $db_host);
  }
  elsif ($db_vendor eq "Oracle") {
    die "The Oracle server host must be specified" unless(defined $db_host);
    die "ORACLE_HOME is not set" unless(defined $ORACLE_HOME);
    my @drivers = DBI->available_drivers;
    if ((my $result = grep  /Oracle/ , @drivers) == 0){
      print STDERR "Error: Oracle DBD Module is not installed.\n";
      exit(1);
    }
  }
  else {
	  die "The database vendor can either be \'Oracle\' or \'MySQL\'";
  }

  # Read database password from file
  open(FILE, $pwd_file) or die("Unable to open password file");
  my @data = <FILE>;
  my $pwd = $data[0];
  $pwd =~ s/\n//;
  close(FILE);

  # Connect to the database
  my $conn = connectToDatabase($db, $user, $pwd, $db_vendor, $db_host);

  if ($show_version) {
    Common::querySchemaVersion($db);
    exit(0);
  }

  # Return vendor and connection
  return ($db_vendor, $conn);
}

1;

