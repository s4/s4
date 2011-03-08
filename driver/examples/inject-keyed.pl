use IO::S4::Client;
use Data::Dumper;

$sn = shift @ARGV;
$cn = shift @ARGV;
$kn = [@ARGV]; # remaining args are keys

$c = new IO::S4::Client("localhost", 2334);
$c->init();
print Dumper($c);

$c->connect();

while (<STDIN>) {
  chomp;
  $c->sendKeyed($sn, $cn, $kn, $_);
  select(undef, undef, undef, 0.1);
  print STDERR '.';
}

$c->disconnect();
