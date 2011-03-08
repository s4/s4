use IO::S4::Client;
use Data::Dumper;

my $mode = eval $ARGV[0];

$c = new IO::S4::Client("localhost", 2334);

print ref($c) . "\n";

$c->init() or die "Failed to initialize.";

print "--------------------------------------------------------------------------------\n";
print "Initialized: " . Dumper($c);
print "--------------------------------------------------------------------------------\n";

print Dumper($rm, $wm, $mode);
$c->connect($mode) or die "Failed to connect.";

my $l;
while ($l = $c->recv()) {
  print Dumper($l);
}
