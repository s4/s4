use IO::S4::Client;
use Data::Dumper;

$rm = $ARGV[0];
$wm = $ARGV[1];

$c = new IO::S4::Client("localhost", 2334);

print ref($c) . "\n";

$c->init() or die "Failed to initialize.";

print "--------------------------------------------------------------------------------\n";
print "Initialized: " . Dumper($c);
print "--------------------------------------------------------------------------------\n";

my $mode = {};
$mode->{'readMode'} = $rm if defined $rm;
$mode->{'writeMode'} = $wm if defined $wm;

print Dumper($rm, $wm, $mode);
$c->connect($mode) or die "Failed to connect.";

my $l;
while ($l = $c->recv()) {
  print Dumper($l);
}
