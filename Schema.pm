package DBIx::Schema;
use strict;
use DBIx::Abstract;
use DBIx::Datadict;

use vars qw($VERSION);

$VERSION = '0.06';

sub connect {
    # This is just an alias for new
    my $self = shift->new(@_);
    return $self;
}

sub new {
    # Create a new object an initialize it
    my $class = shift; $class = ref($class) || $class;
    my $self = {};

    bless($self, $class);
    if ($self->initialize(@_)) { 
        return $self; 
    } else { 
        return 0;
    }
}

sub initialize {
    # Initialize our new object.  Our first parameter should be either
    # a DBIx::Abstract object or a DBIx::Abstract connect string.  Our
    # second parameter is a datadict object.  If we aren't given one
    # then we'll create one using our database object.

    my $self = shift;
    if ( (ref($_[0]) eq 'DBIx::Abstract') || (ref($_[0]) eq 'SQL::DBI') ) {
        # We've been passed a legal dbh object
        $self->{'dbh'} = $_[0];
    } else {
        # This ought to be a datasource connect hashref, then
        $self->{'dbh'} = DBIx::Abstract->connect($_[0]);
    }
    if ( (ref($_[1]) eq 'DBIx::Datadict') ) {
        # We've been passed a legal datadict object
        $self->{'dd'} = $_[1];
    } else {
        $self->{'dd'} = DBIx::Datadict->new({dbh=>$self->{'dbh'},preload=>1});
    }
    # Define instance variables as hashrefs or arrayrefs
    $self->{'field_names'} = {};
    $self->{'table_relations'} = {};
    $self->{'field_table_names'} = {};

    # These seem redundent since they're set in this manor when select
    # is called.
    #$self->{'visited_tables'} = {};
    #$self->{'parent_links'} = [];
    #$self->{'child_links'} = [];
    #$self->{'seen_tables'}= {};
    #$self->{'needed_tables'} = {};
}

sub select {
    # This is the primary method to start of using this module.
    my $self = shift;
    my ($args) = @_;
    $self->error("The 'select' method requires a single hashref as an argument.") unless ref($args) eq 'HASH';

    # The limits for the query.  Same format as DBIx::Abstract
    my $where = $$args{'where'};

    # Clear out our instance variables from prior runs
    $self->{'visited_tables'} = {};
    $self->{'parent_links'} = [];
    $self->{'child_links'} = [];
    $self->{'seen_tables'} = {};
    $self->{'needed_tables'} = {};

    # Build list of additional required tables.  This should only be needed
    # if tables are being used in a way invisible to Schema, like if the
    # where is a scalar.
    my @tables;
    if (ref($$args{'tables'}) eq 'ARRAY') {
        @tables = @{$$args{'tables'}};
    } elsif (exists($$args{'tables'}) and !ref($$args{'tables'})) {
        @tables = split(/\s*,\s*/,$$args{'tables'});
    }

    # Set our key_table.  The term key_table is an internal one and probably
    # shouldn't be used by programs using this library.
    my $key_table = $$args{'key_table'} || $$args{'table'} || $tables[0];

    # If key_table still isn't set by this point, we can't go on.
    $self->error("Sorry, can't perform a schema select without a key table.  Please pass a 'table' or 'tables' argument.") unless $key_table;

    $self->{'key_table'} = $key_table;

    # These add all the tables that we'll be needing
    $self->require_tables_from_where($where) if $where;
    $self->require_tables_from_order($$args{'order'}) if exists($$args{'order'});
    $self->require_tables($key_table,@tables);

    # It shouldn't be possible to get to this stage without required tables.
    $self->error("Ack, I don't have any required tables, can't perform schema crawl, glub glub glub.") unless keys(%{$self->{'needed_tables'}});

    # Swim the schema till we find all required tables.
    $self->schema_crawl($key_table, 0);

    # Create our list of fields.
    $self->{'fields'} = $self->field_list_generator($key_table);

    my $query = $self->generate_query($args);

    # We now have enough information to do a select.
    my $dbh = $self->{'dbh'};
    $dbh->select($query);
    # Prepare a new statment handle object. This is our return value.
    my $sth = {};
    bless ($sth, 'Statement');
    $sth->{' _dbh'} = $dbh;
    $sth->{' _dd'} = $self->{'dd'};
    $sth->{' _key_table'} = $key_table;
    $sth->{' _schema'} = {%$self};
    $sth->{' _original_args'} = $args;
    $sth->fetchrow();
    # Add it to myself.
    $self->{'sths'}{$sth} = $sth;

    return $sth;
    # The database handle is now ready for fetchrowing!
}

sub count {
    # This is the primary method to start of using this module.
    my $self = shift;
    my ($args) = @_;
    $self->error("The 'select' method requires a single hashref as an argument.") unless ref($args) eq 'HASH';

    # The limits for the query.  Same format as DBIx::Abstract
    my $where = $$args{'where'};

    # Clear out our instance variables from prior runs
    $self->{'visited_tables'} = {};
    $self->{'parent_links'} = [];
    $self->{'child_links'} = [];
    $self->{'seen_tables'} = {};
    $self->{'needed_tables'} = {};

    # Build list of additional required tables.  This should only be needed
    # if tables are being used in a way invisible to Schema, like if the
    # where is a scalar.
    my @tables;
    if (ref($$args{'tables'}) eq 'ARRAY') {
        @tables = @{$$args{'tables'}};
    } elsif (exists($$args{'tables'}) and !ref($$args{'tables'})) {
        @tables = split(/\s*,\s*/,$$args{'tables'});
    }

    # Set our key_table.  The term key_table is an internal one and probably
    # shouldn't be used by programs using this library.
    my $key_table = $$args{'key_table'} || $$args{'table'} || $tables[0];

    # If key_table still isn't set by this point, we can't go on.
    $self->error("Sorry, can't perform a schema select without a key table.  Please pass a 'table' or 'tables' argument.") unless $key_table;

    $self->{'key_table'} = $key_table;

    # These add all the tables that we'll be needing
    $self->require_tables_from_where($where) if $where;
    $self->require_tables_from_order($$args{'order'}) if exists($$args{'order'});
    $self->require_tables($key_table,@tables);

    # It shouldn't be possible to get to this stage without required tables.
    $self->error("Ack, I don't have any required tables, can't perform schema crawl, glub glub glub.") unless keys(%{$self->{'needed_tables'}});

    # Swim the schema till we find all required tables.
    $self->schema_crawl($key_table, 0);

    # Create our list of fields.
    $self->{'fields'} = 'count(*)';

    my $query = $self->generate_query($args);
    delete($$query{'group'});

    # We now have enough information to do a select.
    my $dbh = $self->{'dbh'};
    $dbh->select($query);

    return ($dbh->fetchrow_array())[0];
}

sub error {
    # A fatal error has occurred. Report something, then throw an exception.
    my $self = shift;
    my ($message) = @_;
    # Very simple for now.
    die "DBIx::Schema error: $message \n";
}

sub require_tables_from_where {
    # Because DBIx::Abstract's 'where' clauses can be so joyously recursive,
    # we must crawl though them similarly to extract all the table to which
    # they might refer.
    my $self = shift;
    my ($where_piece, $depth) = @_;
    $depth ||= 0;
    if ($depth > 20) {
        $self->warning("Yipes, overdeep recursion while parsing 'where'. Strange behavior may result.");
        return;
    }
    if (ref($where_piece) eq 'HASH') {
        $self->require_tables(keys(%$where_piece));
    } elsif (ref($where_piece) eq 'ARRAY') {
        foreach (@$where_piece) {
            $self->require_tables_from_where($_, $depth++) if ref($_);
            # If it's not a ref, it's just a conjunction string, like 'AND'.
        }
    } else {
        $self->warning("Unable to parse a 'where' piece: $where_piece .");
    }
}

sub warning {
    # Report a warning.
    my $self = shift;
    my ($message) = @_;
    # Very simple for now.
    warn "DBIx::Schema warning: $message \n";
}

sub require_tables {
    # Take a list of tables and mark them as needed.
    my $self = shift;
    my(@tables) = @_;
    foreach my $table (@tables) {
        # Strip off any field part of this
        $table =~ s/\.(.*)//g;
        $self->{'needed_tables'}{$table} = 0; # Will be 1 when found.
    }
}

sub require_tables_from_order {
    # Find the tables in the order and require them.
    my $self = shift;
    my ($order) = @_;
    if (ref($order) eq 'ARRAY') {
        $self->require_tables(@$order);
    } elsif (!ref($order)) {
        $self->require_tables(split(/\s*,\s*/,$order));
    } else {
        $self->warning("Unable to parse a 'order': $order");
    }
}

sub schema_crawl {
    # A recursive method for building a path between a bunvh of tables.
    # The passed-in parameter is the table it's sitting on now.
    my $self = shift;
    my ($table, $depth, $v_relations, $v_tables, $relation_id) = @_;
    
    $v_relations ||= [];
    push(@$v_relations, $relation_id) if defined($relation_id);

    $v_tables ||= {};
    # Make sure we're not spiraling down too deep
    $depth++;
    if ($depth > 25) {
        $self->warning("Overdeep recursion with $table!!");
        return 0;
    }

    # Mark this table as visited.
    $self->{'seen_tables'}{$table} = 1;

    $$v_tables{$table} = 1;
    my $loser = 0;                # debug
    # $self->{'needed_tables'} is a hashref whose keys are tables, and values are
    # true if they've been traversed and false if they haven't.
    if (exists($self->{'needed_tables'}{$table})) {
        # Aha, this table contributes to the goal
        $self->{'needed_tables'}{$table} = 1;
        foreach (@$v_relations) {
            push (@{$self->{'parent_links'}}, $$_{parent});
            push (@{$self->{'child_links'}}, $$_{child});
        }
        @{$self->{'visited_tables'}}{keys(%$v_tables)} = 1;
        # Check to see if we're done
        # I wonder if there's a faster way to do this


        my $all_found = 1;
        foreach (values(%{$self->{'needed_tables'}})) {
            if ($_ == 0) {
                $all_found = 0;
                last;
            }
        }
        if ($all_found) {       # Yay, we are done
            return 1;
        }
    }
    # Gather up links.
    
    
    my $dd = $self->{'dd'};
    my @my_relations;
    
    # Skip all this stuff if we've gone through this already.
    if (my $my_relations = $self->{'table_relations'}{$table}) {
        # OK, it's been cached. Use that copy.
        @my_relations = @$my_relations;
    } else {
        my $table_info = $dd->lookup_table($table);
        foreach (values(%{$$table_info{'parent'}})) {
            if ($dd->lookup_field($$_{'parent'})->{'md_table_id'} == $$table_info{'id'}) {
                push(@my_relations, [{parent=>$$_{'child'}, child=>$$_{'parent'}}, $$_{'child'}]);
            } else {
                push(@my_relations, [{parent=>$$_{'parent'}, child=>$$_{'child'}}, $$_{'parent'}]);
            }
        }
        # Cache relations.
        $self->{'table_relations'}{$table} = \@my_relations;
    }
    # Loop through this table's links.
    unless (@my_relations) {
        return;
    }  
    
    foreach (@my_relations) {
        my ($relation_id, $field_id) = @$_;
        # See which table this is from
        my $table;
        unless ($table = $self->{'field_table_names'}{$field_id}) {
            $table = $self->{'dd'}->lookup_table($self->{'dd'}->lookup_field($field_id)->{'md_table_id'})->{'name'};
            $self->{'field_table_names'}{$field_id} = $table;
        }
        if (exists($self->{'seen_tables'}{$table})) {
            next;
        }
        # Ah, a strange table. Let's dive down a level.
        # First, gotta dereference and then re-reference things, to prevent
        # sharing. This is egregiously dumb and uses too much memory, and
        # I'll need to think of something better soon.
        $self->schema_crawl($table, $depth, [@$v_relations], {%$v_tables}, $relation_id);
    }
}  

sub field_list_generator {
    # Convert table list (stored in selectables) into a list of all the
    # fields to select with
    my $self = shift;
    my(@tables) = @_;
    my $output = [];
    foreach my $table (@tables) {
        my $table_info = $self->{'dd'}->lookup_table($table);
        if ($table_info) {
            foreach (keys(%{$table_info->{'field'}})) {
                push(@$output, "$table.$_ AS '$table.$_'");
            }
        } else {
            $self->error("Unknown table: $table");
        }
    }
    $self->error("All of your tables are bogus!") unless @$output;
    return $output;
}

sub generate_query {
    # Generate a query for DBIx::Abstract
    my $self = shift;
    my ($args) = @_;
    my $parents = $self->{'parent_links'};
    my $children = $self->{'child_links'};
    my $joins;
    my $query;                    # return value (hashref)

    if ($parents) {
        for (my $i=0;$i<scalar(@$parents);$i++) {
            my $parent_name = $self->field_name($$parents[$i]);
            my $child_name = $self->field_name($$children[$i]);
            push (@$joins, "$parent_name = $child_name")  if $parent_name and $child_name;
        }
    }

    # These three values we always need:
    $query = {
              fields=>$self->{'fields'},
              tables=>[keys(%{$self->{'visited_tables'}})],
              'join'=>$joins,
              };

    $$args{'group'} ||= [];
    if (!ref($$args{'group'})) {
        $$args{'group'} = [split(/\s*,\s*/,$$args{'group'})];
    } elsif (ref($$args{'group'}) ne 'ARRAY') {
    	$self->warning("Unparsable group ARRAY");
    	$$args{'group'} = [];
    }
    my @group = @{$$args{'group'}};
    push(@group,$self->{'key_table'}.'.id');

    # Now take care of optional, $args-based pieces:
    $$query{'where'} = $$args{'where'} if defined($$args{'where'});
    $$query{'order'} = $$args{'order'} if defined($$args{'order'});
    $$query{'extra'} = $$args{'limit'} if defined($$args{'limit'});
    $$query{'group'} = \@group;

    # Handle extra fields
    if (defined($$args{'fields'})) {
        if (ref($$args{'fields'}) eq 'ARRAY') {
            push(@{$$query{'fields'}},@{$$args{'fields'}});
        } elsif (!ref($$args{'fields'})) {
            $$query{'fields'} = join(', ',@{$$query{'fields'}},$$args{'fields'});
        }
    }

    return $query;
}

sub field_name {
    # Returns the name of a given field id. Uses a cache.
    my $self = shift;
    my ($field_id) = @_;
    my $field_names = $self->{'field_names'};
    unless (defined($$field_names{$field_id})) {
        my $field = $self->{'dd'}->lookup_field($field_id);
        my $table = $self->{'dd'}->lookup_table($$field{'md_table_id'});
        $$field_names{$field_id} = "$$table{'name'}.$$field{'name'}";
    }
    return $$field_names{$field_id};
}

sub flush_cache {
    # Nix the object's internal caches.
    my $self = shift;
    $self->{'table_relations'} = {};
    $self->{'field_table_names'} = {};
    $self->{'field_names'} = {};
}

sub DESTROY {
    # Make sure all surviving statement handles are slain, and
    # avoid the yuckyness of circular references.
    my $self = shift;
    foreach my $possession (qw(sths dbhs)) {
        foreach (values(%{$self->{$possession}})) {
            %$_ = ();
        }
    }
}



#####################################################################

package Row;

use vars qw($AUTOLOAD);

sub AUTOLOAD {
    my $self = shift;
    my $name = $AUTOLOAD;
    $name =~ s/.*://;             # strip pacakge name

    return if $name eq 'DESTROY';   # We have no destructor

    my $new_sth;
    if ($self->{' _sub_sth'} && $self->{' _sub_sth'}{' _key_table'} eq $name) {
        $new_sth = $self->{' _sub_sth'};
    } else {
        my $sth = $self->{' _sth'};
        my $args = {%{$sth->{' _original_args'}}};
        my $key_table = $sth->{' _key_table'};
        # Gotta deref & reref the present 'where' to avoid circular reference
        # in the new 'where'...
        if ($$args{'where'}) {
            $$args{'where'} = [$$args{'where'}, 'AND', {"$key_table.id"=>$self->{'id'}}];
        } else {
            $$args{'where'} = {"$key_table.id"=>$self->{'id'}};
        }
        $$args{'key_table'} = $name;
        $$args{'fields'} = ["$name.*"];
        #    my $schema = $sth->{'schema'};
        my $schema = DBIx::Schema->new($sth->{' _dbh'}->clone(),$sth->{' _dd'});
        # Hmm... I think limits are causing a problem
        delete($$args{'limit'});
        # Store this old schema on the old one
        $sth->{' _schema'}->{'dbhs'} = $schema;

        $new_sth = $schema->select($args);
        $self->{' _sub_sth'} = $new_sth;
    }

    return $new_sth;
}

sub sth {
    my $self = shift;
    # Returns the statement handle from which this row was created.
    return bless($self->{' _sth'}, 'Statement');
}

#####################################################################

package Statement;

sub fetchrow {
    # The other major user-invocable method.
    # Like the select method, it's a DBIx::Abstract wrapper.
    my $self = shift;
    my $dbh = $self->{' _dbh'};
    my $last_row_obj = $self->{' _current_row_obj'};
    if (ref($last_row_obj) and not %$last_row_obj) {
        # We were given an empty hashref... this is the last iteration...
        return undef;
    }
    $last_row_obj ||= {};
    if (my $row = $dbh->fetchrow_hashref()) {
        # Fill me up...
        $self->fill_self($row);
        # Set a bunch of instance variables.
        $self->{' _current_row'} = $row;
        my $row_obj = {};
        bless ($row_obj, 'Row');
        # Stick a copy of the schema object onto the row object.
        # Note the use of somewhat bogus private-variable naming scheme.
        $row_obj->{' _sth'} = {%$self};
        $self->fill_row($row, $row_obj, $self->{' _key_table'});
        # Boy, this is sloppy. Still deciding what to do with these objects.
        $self->{' _current_row_obj'} = $row_obj;
        return $last_row_obj;
    } else {
        # Got nuthin.
        if (defined($last_row_obj)) {
            $self->{' _current_row_obj'} = {};
            return $last_row_obj;
        } else {
            return undef;
        }
    }
}

sub key_table {
    # Return a string describing this handle's key table.
    my $self = shift;
    return $self->{' _key_table'};
}

sub rows {
    # Returns result of the DBIx::Abstract method of the same name.
    my $self = shift;
    return $self->{' _dbh'}->rows();
}

sub fill_row {
    # Fills out a row object's instance variables with various goodies.
    my $self = shift;
    my ($row, $row_obj, $key_table) = @_;
    # I represent this data very schizophrenically whilst I try to ascertain
    # the best way to represent it. Hee.
    foreach (keys(%$row)) {
        my ($table, $field) = split(/\./);
        $table ||='';
        $field ||= '';
        if (defined($table) and defined($key_table) and $table eq $key_table) {
            $row_obj->{$field} = $$row{$_};
            $row_obj->{"$table.$field"} = $$row{$_};
        } else {
            $row_obj->{"$table.$field"} = $$row{$_};
        }
    }
}  

sub fill_self {
    # Like fill_row, except it paints all over itself instead.
    my $self = shift;
    my ($row) = @_;
    my $key_table = $self->{' _key_table'};
    foreach (keys(%$row)) {
        my ($table, $field) = split(/\./);
        $table ||='';
        $field ||= '';
        if ($table eq $key_table) {
            $self->{$field} = $$row{$_};
            $self->{"$table.$field"} = $$row{$_};
        } else {
            $self->{"$table.$field"} = $$row{$_};
        }
    }
}  

=pod

=head1 NAME

DBIx::Schema -- An SQL Abstration layer for working with whole schemas

=head1 SYNOPSIS

 use Schema;
 my $schema = DBIx::Schema->new({db=>'my_db',user=>'db_user',password=>'gigglesnark'});

***

 my $schema = DBIx::Schema->new($my_dbix_abstract_handle);

 $sth = $schema->select({table=>'product', where=>{'product.id'=>['<',6]}});
 while (my $row = $sth->fetchrow()) {
   print $row->{'name'}."\n";
   print $row->color->{'name'}."\n";
   print $row->{'price'}."\n";
   print $row->{'fish'}."\n";
 }

=head1 DESCRIPTION

Basically, this module lets you construct and use DBI-style statement
handles involving arbitrarily large schemas of related SQL tables
without concern about how exactly they're related; in essence, it
builds the join clauses for you, as necessary, from case to case. This
can be a boon to programs that want to knit together their own
complex, relational SQL queries on the fly; through the use of this
module, if they know that some tables are somehow related, even if
they're two or more steps removed from one another, they can simply
name them, and start pulling out data toot-suite.

Of course, you will need to prepare your databases with some metadata
tables ahead of time in order for any of this to work. See the
B<DATABASE PREPARATION> section below for more.

=head1 PREREQUISITES

You most certainly need DBI (as well as appropriate DBD modules for
your setup) for this to work.

At this time, you also need Andrew Turner's DBIx::Abstract
module. Much of the user-level syntax for this module is inherited
from it, so it's good to be familiar with it, as well. This, like DBI
and DBD, is available from CPAN.

=head1 DATABASE PREPARATION

You will need to create three SQL tables in every database to which
you'd like to apply this module. These will act as a data dictionary
for all contents of the database. They will be called md_table,
md_field, and md_relation ('md' stands for 'metadata').

You should have received a Perl script named md_rip.pl as part of the
distribution within which you got this module. Running it will create
these tables inside a given database if they're not already present,
or rebuild and repopulate them if they are. See its perldocs for more
information on its usage.

B<Note> that at this time you must name your each of tables' primary
key column 'id' for md_rip.pl to work, and you also must name columns
relating to them "${table_name}_id". So a column in the 'foo' table
relating to the 'baz' table's primary key must be named 'baz_id'. Of
course, it's not a very complicated script, so you can hack it to
behave differently. :) Future versions will be more flexible.

=head1 METHODS

=head2 Schema handle Methods

=over 4

=item new

This is the schema object constructor. It requires, as an argument,
either a DBIx::Abstract database handle object, or a hashref ready for
feeding to DBIx::Abstract's 'connect' method.

=item connect

An alias to the 'new' method. Takes the same arguments, returns the same thing.

=item select

Returns a statement handle object, primed with an SQL query and ready
for fetchrow calls (see below).

This method takes one hashref as an argument. You must specify a table
that you will be seleting from with the 'table' key.  You can specify
multiple tables by using the 'tables' key instead.  You should only
need to specify a 'tables' key if you are using a table that is
invisible to schema (for instance, if it is in a scalar where).

Optionally, you can have a 'where' key, which will be passed on to the
underlying DBIx::Abstract object, so see that module. Note that this
key's value needs only to hold the limit on results, the aspect of the
where necessary to join tables will be generated by schema for you.

For example:

 $sth = $schema->select({table=>'product', where=>{'product.id'=>['<',6]}});

You can also specify a list of fields to be included beyond the normal
ones.  This allows you to do some special things like:

 $sth = $schema->select({
     table=>'product',
     fields=>[
         'lower(substring(product.name,1,1)) as 'product.letter',
         'substring(product.description,1,50) as 'product.shortdesc',
         ],
     where=>{'product.id'=>['<',6']},
     });

=item flush_cache

The object keeps an internal cache to help it crawl through the
database's relationships faster, but it doesn't check to see if the
database's structure may have changed since the last time it performed
a full crawl. Calling this method deletes the cache, forcing the
object to reexamine the actual tables and start a new cache the next
time it needs to know their structure.

=back

=head2 Statement handle Methods

=over 4

=item fetchrow

Returns a row object, or undef if no rows are available.

As with DBI (and DBIx::Abstract), subsequent calls to fetchrow return
the next row available to this statement handle, and undef once all
rows have been exhausted (or no rows were available in the first
place). Thus, a common code idiom is a while() loop, something like:

 while (my $row = $sth->fetchrow()) {
   # Do something with data from this row
   my $id = $row->{'id'};
   my $foo = $row->{'foo'};
   print "The value of foo for row $id is $foo. \n";
 }

=item rows

Returns the number of rows returned from the SQL query within this
statement handle. 

=item key_table

Returns, as a string, the name of the handle's key table.

=back

=head2 Row objects

Row object methods are special; see below.

=over 4

=item sth

Returns the statement handle from which this row emerged.

=back

Row objects don't have any predefined methods (except for 'sth'). You
can fetch data from them through directly accessing their instance
variables (hash keys), one of which will exist for each column of the
row.

For example, if a row represented with object $row has a 'foo' column,
that column's value is available through $row->{'foo'}.

You can also pull additional statement handles out of a row by
invoking them as methods; an AUTOLOAD method inside the row object
will take care of the rest for you, and return a statement handle
primed with the named table as the key table, and with a where clause
identical to that of the row's statement handle, B<with the addition
of> a phrase requiring that the current key table's id field match
this row's value of same.

For example:

 # I already have a $schema object defined.
 # I'll make a simple statement handle.

 $sth = $schema->select({table=>'product', where=>{'product.price'=>['<',6]}});
 # OK, $sth is now primed to return all products costing less than
 # $6.00.
 
 while (my $product_row = $sth->fetchrow) {
   print "I am on product ".$product_row->{'name'}."\n";
   # Let's say I have a many-to-many relationship in my schema that
   # allows products to exist in any number of categories. I want to
   # display all categories to which this product belongs. The current
   # statement handle doesn't know or care about categories, so it's
   # time to pull out a new one.
   if ($product_row->category->rows) {
     print "It is in the following categories:\n";
     while (my $cat_row = $product_row->category->fetchrow) {
       print $cat_row->{'name'}."\n";
     }
   } else {
     print "It is not in any category.\n";
 }

=head1 CAVEATS

I find the row object as it now stands a little sketchy due to the
fact that it's essentially user-definable, since its instance
variables and legal method names will depend on the nature of the data
fetched from its statement handle. This requires that its actual
methods, 'AUTOLOAD' and 'sth' (and whatever might be added in the
future) be reserved words. So, for now, don't name your tables after
the Row class's methods. (Not that you'd want to, since they'd make
pretty lousy table names, in my humble opinion)

=head1 TODO

It seems to warn about 'Unknown where piece' a bit too often, and
unnecessarily.

The format of the data dictionaries needs to be far more configurable
than it now is.

=head1 BUGS

This software is quite young, having received testing with only a
handful of database systems and Perl versions, and having only a few
users at the time of this writing (though it is in use in a production
environment). The author welcomes bug reports and other feedback at
the email address listed below.

=head1 AUTHOR

Jason McIntosh <jmac@jmac.org>

=head1 HOMEPAGE

http://www.jmac.org/projects/DBIx-Schema/

=head1 VERSION

This documentation corresponds with version 0.06 of DBIx::Schema.

=head1 COPYRIGHT

This software is copyright (c) 2000 Adelphia.

This program is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut


1;

