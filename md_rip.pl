#! /usr/bin/perl -w

# A simple script for filling out minti-style metadata tables.
# This is sickeningly MySQL-specific.

use DBIx::Abstract;
use Getopt::Std;
use strict;

# La la constants

my %opts;                       # command-line options

getopt('D:r:d:u:p:h:P:', \%opts);          # fill it up

my %connect;

my $md_table = 'md_table';
my $md_field = 'md_field';
my $md_relation = 'md_relation';


if ($opts{'D'}) {  # If they passed a datasource
    $connect{'datasource'} = $opts{'d'};
} else {
    $connect{'driver'} = $opts{'r'} || 'mysql';
    $connect{'dbname'} = $opts{'d'} || 'minti';
    if (defined($opts{'h'})) {
        $connect{'host'} = $opts{'h'};
    }
    if (defined($opts{'P'})) {
        $connect{'port'} = $opts{'P'};
    }
}

if ($opts{'u'}) {
    $connect{'user'} = $opts{'u'};
    if (defined($opts{'p'})) {
        $connect{'password'} = $opts{'p'};
    }
}

my $dbh = DBIx::Abstract->connect(\%connect)
    || die "Couldn't connect to Database\n";

print "Working on $connect{'dbname'}.\n";


# Create missing tables.

# 0 if missing, 1 if found
my %tables_found = ($md_table=>0,$md_field=>0,$md_relation=>0);

$dbh->query('show tables');
while (my ($table) = $dbh->fetchrow_array()) {
    $tables_found{$table} = 1 if exists($tables_found{$table});
}

foreach (keys(%tables_found)) {
    md_table_create($_) unless $tables_found{$_};
}

# Dump current contents of metadata tables
foreach ($md_table, $md_field, $md_relation) {
    $dbh->delete($_);
}

# Create list of tables
my @tables;
$dbh->query('show tables');
while (my ($table) = $dbh->fetchrow_array()) {
    push (@tables, $table);
}

print "Found: @tables\n";

# Create lists of fields for each table, fill out table and field tables.
# Create a hash of table keys, keyed to tables (!!), too.
my %table_keys;			# {tableid} = id_of_its_primary_key_field
my %field_ids;			# {foo.bar} = foo.bar's metdata id
my %table_ids;			# {foo} = foo's metadata id
foreach my $table (@tables) {
    my @fields;
    my %key_fields;
    $dbh->insert($md_table, {name=>$table});
    my ($table_id) = $dbh->query("select last_insert_id() from $md_table")->fetchrow_array();
    $table_ids{$table} = $table_id;
    my $dbh2 = $dbh->clone();
    $dbh->query("desc $table");

    while (my %desc = $dbh->fetchrow_hash()) {
	push (@fields, $desc{'Field'});
	$key_fields{$desc{'Field'}} = 1 if $desc{'Key'} eq 'PRI';
    }
    foreach my $field (@fields) {
	$dbh2->insert($md_field,{name=>$field, $md_table.'_id'=>$table_id});
	my ($field_id) = $dbh2->query("select last_insert_id() from $md_field")->fetchrow_array();
	if ($key_fields{$field}) {
	    $table_keys{$table} = $field_id;
	}
	$field_ids{"$table.$field"} = $field_id;
    }
}

# Try to describe field relationships sanely
my $dbh2 = $dbh->clone();
my $dbh3 = $dbh->clone();
foreach (keys(%field_ids)) {
    my ($table, $field) = /(.*)\.(.*)/;
    if ($field =~ /(.*)_id$/) {
	# Looks suspiciously like a child... let's get the probable parent
	next if $field_ids{$_} == $table_keys{$table};
	if (my $parent_field_id = $table_keys{$1}) {
	    $dbh3->insert($md_relation, {parent=>$parent_field_id, child=>$field_ids{$_}});
	}
    }
}

# Table creation method.

sub md_table_create {
    my ($create) = @_;		# table that needs creating
    if ($create eq $md_field) {
	$dbh->query("CREATE TABLE $md_field (id int(11) DEFAULT '0' NOT NULL auto_increment, name char(32), md_table_id int(11), alias char(32), file int(11) DEFAULT '0' NOT NULL, PRIMARY KEY (id))");
    } elsif ($create eq $md_relation) {
	$dbh->query("CREATE TABLE $md_relation (id int(11) DEFAULT '0' NOT NULL auto_increment, parent int(11), child int(11), PRIMARY KEY (id))");
    } elsif ($create eq $md_table) {
	$dbh->query("CREATE TABLE $md_table (id int(11) DEFAULT '0' NOT NULL auto_increment, name char(32), PRIMARY KEY (id))");
    }
}

=pod

=head1 NAME

md_rip.pl -- create data dictionary tables

=head1 DESCRIPTION

This program assists in the creation and maintenance of metadata (a.k.a. data dictionary) tables used by modules like Schema.

=head1 PREREQUISITES

You will need DBI (with appropriate DBD modules), as well as DBIx::Abstract, all of which are available from CPAN.

=head1 USAGE

Run this program from the command line, passing in flags as
appropriate. This will create and populate a new set of data
dictionary tables inside the specified database, or rebuild the
existing ones -- reflecting any structural changes the database might
have seen since this program was last run -- if they're already
present.

=head2 Flags

You can use either the -D flag to pass in a whole datasource, or some combination of the other flags to let the program build one.

Example:

$ ./md_rip.pl -d my_db -u root -p RootPassword -h localhost

=over 4

=item -D

A DBI-ready datasource string.

=item -r

A driver DBI will recognize. Defaults to 'mysql'.

=item -d

The database to be dictionarified. Defaults to 'minti'. (Long story.)

=item -h

The host upon which this database lives.

=item -P

The port to use when connecting to said host.

=item -u

The username the program will use when connecting.

=item -p

The password the program will use when connecting.

=back

=head1 COPYRIGHT

This software is copyright (c) 2000 The Maine InterNetworks, Inc.

=head1 AUTHOR

Jason McIntosh <jmac@jmac.org>, with some improvements by Andrew
Turner <turner@mikomi.org>.

=cut
