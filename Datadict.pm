package DBIx::Datadict;
use DBIx::Abstract;
use strict;

BEGIN {
    $DBIx::Datadict::VERSION = '0.01';
}

sub new {
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
    my $self = shift;
    my($params) = @_;
    if ( (ref($$params{'dbh'}) eq 'DBIx::Abstract') || (ref($$params{'dbh'}) eq 'SQL::DBI') ) {
        # We've been passed a legal dbh object
        $self->{'dbh'} = $$params{'dbh'};
    } else {
        # This ought to be a datasource connect hashref, then
        $self->{'dbh'} = DBIx::Abstract->connect($$params{'db_connect'});
    }
    return 0 unless defined($self->{'dbh'});
    if ($$params{'preload'}) {
        $self->{'loaded'} = 1;
        $self->dbh->select('*','md_table');
        while (my $table = $self->dbh->fetchrow_hashref()) {
            $self->add_table($table);
        }
        $self->dbh->select('*','md_field');
        while (my $field = $self->dbh->fetchrow_hashref()) {
            $self->add_field($field);
        }
        $self->dbh->select('*','md_relation');
        while (my $relation = $self->dbh->fetchrow_hashref()) {
            $self->add_relation($relation);
        }
    }
    return 1;
}

sub dbh {
    my $self = shift;
#    warn "Call to DBH\n";
    return $self->{'dbh'};
}

sub add_table {
    my $self = shift;
    my($table) = @_;
#    warn "Adding table $$table{'name'}\n";
    $self->{'cache'}{'table'}{'name'}{$$table{'name'}} = $table;
    $self->{'cache'}{'table'}{'id'}{$$table{'id'}} = $table;
}

sub add_field {
    my $self = shift;
    my($field) = @_;
#    warn "Adding field $$field{'name'}\n";
    $self->{'cache'}{'field'}{'id'}{$$field{'id'}} = $field;
    $self->{'cache'}{'table'}{'id'}{$$field{'md_table_id'}}{'field'}{$$field{'name'}} = $field;
}

sub add_relation {
    my $self = shift;
    my($relation) = @_;
#    warn "Adding relation $$relation{'child'}, $$relation{'parent'}\n";
    $self->{'cache'}{'relation'}{'id'}{$$relation{'id'}} = $relation;
    $self->{'cache'}{'field'}{'id'}{$$relation{'parent'}}{'child'}{$$relation{'child'}} = $relation;
    $self->{'cache'}{'field'}{'id'}{$$relation{'child'}}{'parent'}{$$relation{'parent'}} = $relation;

    $self->{'cache'}{'table'}{'id'}{ $self->{'cache'}{'field'}{'id'}{ $$relation{'parent'} }{'md_table_id'} }{'parent'}{ $$relation{'id'} } = $relation;
    $self->{'cache'}{'table'}{'id'}{$self->{'cache'}{'field'}{'id'}{$$relation{'parent'}}{'md_table_id'}}{'child'}{$$relation{'id'}} = $relation;

    $self->{'cache'}{'table'}{'id'}{ $self->{'cache'}{'field'}{'id'}{ $$relation{'child'} }{'md_table_id'} }{'parent'}{ $$relation{'id'} } = $relation;
    $self->{'cache'}{'table'}{'id'}{$self->{'cache'}{'field'}{'id'}{$$relation{'child'}}{'md_table_id'}}{'child'}{$$relation{'id'}} = $relation;


}

sub lookup_table {
    my $self = shift;
    my($lookup) = @_;
    my $lookup_type;

#    warn "Looking up table $lookup\n";

    if ($lookup =~ /^\d+$/) {
        $lookup_type = 'id';
    } else {
        $lookup_type = 'name';
    }
    unless ($self->{'loaded'} or exists($self->{'cache'}{'table'}{$lookup_type}{$lookup})) {
        $self->{'dbh'}->select('*','md_table',{$lookup_type=>$lookup});
        if ($self->dbh->rows) {
            while (my $table = $self->dbh->fetchrow_hashref()) {
                $self->add_table($table);
            }
        } else {
            $self->{'cache'}{'table'}{$lookup_type}{$lookup} = undef;
        }
    }
    return $self->{'cache'}{'table'}{$lookup_type}{$lookup};
}

sub lookup_field {
    my $self = shift;
    my($lookup,$lookup2) = @_;
    my $lookup_table;
    my $lookup_type;

#    warn "Looking up field $lookup $lookup2\n";


    if ($lookup =~ /^\d+$/) {
        $lookup_type = 'id';
    } else {
        $lookup_type = 'name';
        if (defined($lookup2)) {
            $lookup_table = $lookup;
            $lookup = $lookup2;
        } else {
            $lookup =~ m/([^.]*)\.(.*)/;
            $lookup = $2;
            my $table;
            if ($table = $self->lookup_table($1)) {
                $lookup_table = $$table{'id'};
            }
        }
    }
    unless ($self->{'loaded'} or 
            ($lookup_type eq 'id' and 
             exists($self->{'cache'}{'field'}{$lookup_type}{$lookup})) or
            (exists($self->{'cache'}{'table'}{'id'}{$lookup_table}{'field'}{$lookup_type}{$lookup}))) {
        if ($lookup_type eq 'id') {
            $self->dbh->select('*','md_field',{$lookup_type=>$lookup});
        } else {
            $self->dbh->select('*','md_field',{md_table_id=>$lookup_table,name=>$lookup});
        }
        if ($self->dbh->rows) {
            while (my $field = $self->dbh->fetchrow_hashref()) {
                $self->add_field($field);
            }
        } else {
            if ($lookup_type eq 'id') {
                $self->{'cache'}{'field'}{$lookup_type}{$lookup} = undef;
            } else {
                $self->{'cache'}{'table'}{'id'}{$lookup_table}{'field'}{$lookup_type}{$lookup} = undef;
            }
        }
    }
    if ($lookup_type eq 'id') {
        return $self->{'cache'}{'field'}{$lookup_type}{$lookup};
    } else {
        return $self->{'cache'}{'table'}{'id'}{$lookup_table}{'field'}{$lookup};
    }
}

sub lookup_relation {
    my $self = shift;
    my($lookup,$lookup2) = @_;
    my $lookup_type;

#    warn "Looking up relation $lookup $lookup2\n";

    if (!defined($lookup2)) {
        $lookup_type = 'id';
    } else {
        $lookup_type = 'parent_child';
        my $field;
        if ($field = $self->lookup_field($lookup)) {
            $lookup = $$field{'id'};
        }
        if ($field = $self->lookup_field($lookup2)) {
            $lookup2 = $$field{'id'};
        }
    }

    unless ($self->{'loaded'} or 
            ($lookup_type eq 'id' and 
             exists($self->{'cache'}{'relation'}{$lookup_type}{$lookup})) or
            (exists($self->{'cache'}{'field'}{'id'}{$lookup}{'child'}{$lookup2}) or
             exists($self->{'cache'}{'field'}{'id'}{$lookup}{'child'}{$lookup2}))) {
        if ($lookup_type eq 'id') {
            $self->dbh->select('*','md_relation',{$lookup_type=>$lookup});
        } else {
            $self->dbh->select('*','md_relation',[{parent=>$lookup,child=>$lookup2},'OR',{parent=>$lookup2,child=>$lookup}]);
        }
        if ($self->dbh->rows) {
            while (my $relation = $self->dbh->fetchrow_hashref()) {
                $self->add_relation($relation);
            }
        } else {
            if ($lookup_type eq 'id') {
                $self->{'cache'}{'relation'}{$lookup_type}{$lookup} = undef;
            } else {
                $self->{'cache'}{'field'}{'id'}{$lookup}{'child'}{$lookup2} = undef;
                $self->{'cache'}{'field'}{'id'}{$lookup2}{'child'}{$lookup} = undef
            }
        }
    }


    if ($lookup_type eq 'id') {
        return $self->{'cache'}{'relation'}{$lookup_type}{$lookup};
    } else {
        return $self->{'cache'}{'field'}{'id'}{$lookup}{'child'}{$lookup2} or
          $self->{'cache'}{'field'}{'id'}{$lookup2}{'child'}{$lookup};
    }
}

1;
