use Getopt::Long ();
use ExtUtils::MakeMaker qw( prompt WriteMakefile );
use lib "lib";
use ExtUtils::PerlPP ();

# All this stuff is copied largely from the Makefile.PL of
# DBIx::Abstract, which I strongly suspect is based on a DBD's
# Makefile.PL. Except not really.

use vars qw( $options );

$options = { 'prompt' => 1 };

{
    Getopt::Long::GetOptions($options, qw(
        help verbose noprompt prompt! 
        test-dsn=s
        test-user=s
        test-pass=s
        ));

    if ($options->{'help'}) {
        print <<'EOF';
Usage: perl Makefile.PL <options>

Possible Options are:

  --verbose     Enable verbose mode
  --noprompt    Disable interactive dialog

  --test-dsn=dsn        Sets data source for testing.  Uses dbi:mysql:test
                        by default.
  --test-user=user      Username and password to use with the dsn above
  --test-pass=password  during testing.  They both default to undef.
EOF
        exit 1;
    }
    eval 'use Data::Dumper';
    if ($@) {               
        print q{
You don't have installed the Data::Dumper module, which is
required for building this package. Missing modules are available
from any CPAN mirror, for example

   ftp://ftp.funet.fi/pub/languages/perl/CPAN/modules/by-module
  
};
        exit 1;
    }          
    if (!$$options{'test-dsn'}) {
        $$options{'test-dsn'} = 'dbi:mysql:test';
        $$options{'test-user'} = 'undef';
        $$options{'test-pass'} = 'undef';
        if ($$options{'prompt'}) {
            $$options{'test-dsn'} = prompt(
	            "Which data source should I use for testing the module?",
                $$options{'test-dsn'});
            $$options{'test-user'} = prompt(
	            "Which username should I use for testing the module?",
                $$options{'test-user'});
            $$options{'test-pass'} = prompt(
	            "Which passwordshould I use for testing the module?",
                $$options{'test-pass'});
        }
        $$options{'test-user'} = undef if $$options{'test-user'} eq 'undef';
        $$options{'test-pass'} = undef if $$options{'test-pass'} eq 'undef';
    }
    $Data::Dumper::Indent = 1;
    my $config;
    foreach my $key (keys(%$options)) {
        $newkey = $key;
        $newkey =~ s/-/_/g;
        $$config{$newkey} = $$options{$key};
    }
    my $cf = Data::Dumper::Dumper($config);
    my $time = localtime();
    my $cfg_file = <<"CFG_FILE";
# -*- perl -*-
#
#    This file was automatically generated at $time
#    by Makefile.PL. Do not edit, instead do a "make realclean" in
#    the toplevel directory and rerun "perl makefile.PL".
#

my $cf
CFG_FILE
    open(CFG_FILE,">config.pl");
    print CFG_FILE $cfg_file;
    close(CFG_FILE);
}

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'DBIx::Schema',
    'VERSION_FROM' => 'Schema.pm', # finds $VERSION
    'DISTNAME'      => 'DBIx-Schema',
    'PREREQ_PM'     => { 'DBIx::Abstract' => 0.90 },
    'clean'         => {FILES=>'config.pl t/basic.t test.log'},
    'PL_FILES'      => ['t/basic.t.PL', 't/basic.t'],
    'PM'            => {'Schema.pm'=>'blib/lib/DBIx/Schema.pm',
                        'Datadict.pm'=>'blib/lib/DBIx/Datadict.pm'},
    'linkext'       => { LINKTYPE=>'' },
    'dist'          => {'COMPRESS'=>'gzip -9f', 'SUFFIX' => 'gz'},

);

package MY;

sub libscan {
    my($self, $path) = @_;
    if ($path =~ /(PerlPP\.pm|Config\.pm|Install\.pm|,v|~)$/) { return undef; }
    $path;                                                                     
}

sub processPL {
    my($self) = shift;
    my @output;
    my @files = @{$self->{'PL_FILES'}};
    while (@files) {
        my $from = shift @files;
        my $to = shift @files;  
        my $cfg = 'config.pl';
        push(@output, qq|
pm_to_blib: $to

$to: $from
	\$(PERL) -I\$(INST_ARCHLIB) -I\$(INST_LIB) -I\$(PERL_ARCHLIB) \\
			-I\$(PERL_LIB) -Ilib -MExtUtils::PerlPP \\
			-e ppp '$from' '$to' '$cfg'

|);
    }
    join('', @output);
}
