# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl test.pl'

######################### We start with some black magic to print on failure.

# Change 1..1 below to 1..last_test_to_print .
# (It may become useful if the test is moved to ./t subdirectory.)

BEGIN { 
  $| = 1; 
  print "1..1\n"; 
  if ($] < '5.00405') {
    use lib 'lib';
  }
  eval 'use Test';
  die $@ if $@;
}
  
END {
  print "not ok 1\n" unless $loaded;
}

use DBIx::Schema;
use DBIx::Abstract;
$loaded = 1;
print "ok 1\n";

my $dsn  = q{~~test_dsn~~};
my $user = q{~~test_user~~} || undef;
my $pass = q{~~test_pass~~} || undef;

# Set up a playground schema.
# This assumes (perhaps foolishly) that DBIx::Abstract is installed
# and works fine.

# foo and bar share a one-to-many relationship.
# foo and baz share a many-to-many relationship by way of foobaz.

plan(test=>11);

# Making an DBIx::Abstract connection
my $dbh = DBIx::Abstract->connect({dsn=>$dsn, user=>$user, password=>$pass});
ok($dbh);

# This isn't a test, la la la .

# Evalling, cuz any of these may or may not exist to drop.
eval { $dbh->query('drop table foo') } ;
eval { $dbh->query('drop table bar') } ;
eval { $dbh->query('drop table baz') } ;
eval { $dbh->query('drop table foobaz') } ;

$dbh->query('create table foo (id int not null primary key,name char(30) not null,value char(30) null)');
$dbh->query('create table bar (id int not null primary key,foo_id int null,name char(30) not null)');
$dbh->query('create table baz (id int not null primary key,name char(30) not null)');
$dbh->query('create table foobaz (id int not null primary key,foo_id int null,baz_id int null)');


$dbh->insert('foo',{id=>1,name=>'foo 1',value=>'this'});
$dbh->insert('foo',{id=>2,name=>'foo 2',value=>'baz'});
$dbh->insert('foo',{id=>3,name=>'foo 3',value=>'test'});
$dbh->insert('foo',{id=>4,name=>'foo 4',value=>'bar'});

$dbh->insert('bar',{id=>1,foo_id=>4,name=>'heh'});
$dbh->insert('bar',{id=>2,foo_id=>3,name=>'heh'});
$dbh->insert('bar',{id=>3,foo_id=>2,name=>'heh'});
$dbh->insert('bar',{id=>4,foo_id=>1,name=>'baz'});

$dbh->insert('baz',{id=>1,name=>'bazval 1'});
$dbh->insert('baz',{id=>2,name=>'bazval 2'});
$dbh->insert('baz',{id=>3,name=>'bazval 3'});
$dbh->insert('baz',{id=>4,name=>'bazval 4'});

$dbh->insert('foobaz',{id=>1,foo_id=>1,baz_id=>4});
$dbh->insert('foobaz',{id=>2,foo_id=>1,baz_id=>3});
$dbh->insert('foobaz',{id=>3,foo_id=>3,baz_id=>2});
$dbh->insert('foobaz',{id=>4,foo_id=>3,baz_id=>1});

# now that that's done, let's get into some testing

# Two ways of schema creation
my $schema;
$schema = DBIx::Schema->connect({dsn=>$dsn, user=>$user, password=>$pass});
ok($schema);
undef($schema);
$schema = DBIx::Schema->connect($dbh);
ok($schema);

# Statement handle creation
my $sth;
$sth = $schema->select({table=>'foo', where=>{'foo.id'=>1}});
ok($sth);

# Rows method
ok(1, $sth->rows());

# Fetchrow
my $row;
ok($row = $sth->fetchrow());

# Statement handle projection
ok(2, $row->baz->rows());

# Miscellaneous statement handle fun...
$sth = $schema->select({table=>'foo'});
ok(4, $sth->rows());

$sth = $schema->select({table=>'foo', where=>{'foo.id'=>['<',6]}});
ok(4, $sth->rows());

$sth = $schema->select({table=>'foo', where=>{'bar.name'=>'heh'}});
ok(3, $sth->rows());

# I don't know why this doesn't work. :( It does outside the test script. GRR.
#$sth = $schema->select({table=>'foo', where=>{'baz.name'=>'bazval 2'}});

$sth = $schema->select({table=>'foo', where=>[{'bar.name'=>'heh'},'OR',{'bar.id'=>4}]});
ok(1, $sth->rows());

# cleanup
$dbh->query('drop table foo');
$dbh->query('drop table bar');
$dbh->query('drop table baz');
$dbh->query('drop table foobaz');

