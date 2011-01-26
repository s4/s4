use IO::S4::Client;
use Data::Dumper;

$sn = $ARGV[0];
$cn = $ARGV[1];

$c = new IO::S4::Client("localhost", 2334);
$c->init();
print Dumper($c);

$c->connect();

while (<STDIN>) {
  chomp;
  $c->send($sn, $cn, $_);
  select(undef, undef, undef, 0.1);
}

$c->disconnect();
