#
# Copyright (c) 2010 Yahoo! Inc. All rights reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 	        http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the
# License. See accompanying LICENSE file. 
#

package IO::S4::Client;

use strict;

use IO::Socket;
use IO::Select;
use JSON;

use constant PROTO_NAME => "generic-json";
use constant PROTO_MAJOR => 1;
use constant PROTO_MINOR => 0;

# Constructor
# Args: (hostname, port)
# E.g. new IO::S4::Client(somehost, 2334)
sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;

    my $host = shift;
    my $port = shift;

    my $h = {'host' => $host, 'port' => $port};

    bless $h, $class;

    return $h;
}

my $versionCompatible = sub {
    my $p = shift;

    return ($p->{"name"} eq PROTO_NAME)
           and ($p->{"versionMajor"} == PROTO_MAJOR)
           and ($p->{"versionMinor"} >= PROTO_MINOR);
};

my $readBytes = sub {
    my ($sock, $sel, $buffer, $len) = @_;

    my $r = 0;

    do {
        if (eof $sock) {
            warn "connection closed by peer";
            return 0;
        }

        my $p = read($sock, $$buffer, $len-$r, $r); # read remaining bytes.
        if (not defined $p) {
            warn $!;
            $$buffer = "";
            return 0;
        }

        $r += $p;

        if ($r < $len and defined $sel) {
            $sel->can_read();
        }
    } while ($r < $len);

    return $r;
};

my $readByteArray = sub {
    my ($sock, $sel) = @_;

    my $s = "";

    return undef if $readBytes->($sock, $sel, \$s, 4) == 0;

    my ($len) = unpack("N", $s);

    return undef if $readBytes->($sock, $sel, \$s, $len) < $len;

    print "[$len]$s\n";

    return $s;
};

my $sendByteArray = sub {
    my ($sock, $data) = @_;

# first the length: 32-bin integer, big-endian
    my $message = pack("N", (length($data)));

# followed by data array
    $message .= $data;

    print "send: [" . length($data) . "]$data\n";

# send and flush
    $sock->write($message);
    $sock->flush();
};

# Initialize the client.
# S4 adapter issues a unique ID to this client and 
# lists the protocol that it supports. The driver
# tests that the protocol is compatible.
#
# Returns the unique id upon success. Undef otherwise.
sub init {
    my $h = shift;

    return if defined $h->{'uuid'};

    my $host = $h->{'host'};
    my $port = $h->{'port'};

    my $sock = new IO::Socket::INET ( PeerAddr => $host, PeerPort => $port, Proto => 'tcp') or return undef;

    $sock->write(pack('N', 0));
    $sock->flush();

    my $sel = new IO::Select($sock);
    my $response = $readByteArray->($sock, $sel);

    print "RESPONSE: $response\n";

    my $info = decode_json($response);

    $sock->close();

    my $uuid = $info->{"uuid"};
    my $protoName = $info->{"protocol"}->{"name"};
    my $protoMajor = $info->{"protocol"}->{"versionMajor"};
    my $protoMinor = $info->{"protocol"}->{"versionMinor"};

    print "$protoName $protoMajor.$protoMinor: $uuid\n";

    return undef unless $versionCompatible->($info->{"protocol"});

    @{$h}{keys %$info} = values %$info;

    return $uuid;
}

# Connect to the S4 Client API.
# This sets up a persistent conneciton to the S4 client API.
# Events can be sent and received.
#
# The client must first be initialized for this to succeed.
#
# Returns the unique ID of the client upon success. Undef otherwise.
sub connect {
    my ($h, $mode) = @_;

    return $h->{'uuid'} if defined $h->{'sock'} and defined $h->{'uuid'};

    my $host = $h->{'host'};
    my $port = $h->{'port'};
    my $uuid = $h->{'uuid'};

    return undef if not defined $uuid;

    my $sock = new IO::Socket::INET ( PeerAddr => $host, PeerPort => $port, Proto => 'tcp') or return undef;

    my %info;
    %info = %$mode if ref $mode eq "HASH" ;
    $info{'uuid'} = $uuid;
    my $j = encode_json(\%info);

    $sendByteArray->($sock, $j);

    my $sel = new IO::Select($sock);
    my $resp = $readByteArray->($sock, $sel);

    if (not defined $resp) {
        close $sock or warn $!;
        return undef;
    }

    my $r = decode_json($resp);

    if (lc $r->{'status'} eq 'ok') {

        $h->{'sock'} = $sock;
        $h->{'select'} = $sel;

        return $uuid;

    } else {
        warn "Connect failed by Adapter. Reason: "
             . ($r->{'reason'} or 'undef');

        close $sock or warn $!;

        return undef;
    }
}


# Disconnect from the client API.
sub disconnect {
    my $h = shift;

    return if not defined $h->{'sock'};

    my $sock = $h->{'sock'};

    print $sock pack('N', 0);
    close $sock or warn $!;

    delete $h->{'sock'};
    delete $h->{'select'};
}

# Send a message to the S4 cluster.
# The message must be a JSON string representing the object
# to be injected into the cluster.representing the object
# to be injected into the cluster. The JSON string is converted
# into a Java object within S4 using the Gson library.
#
# Arguments:
#     stream:  Nname of the stream on which the event
#              must be dispatched within the S4 cluster.
#     class:   Class of the message object.
#     message: Message encoded in JSON.
#
# Returns the number of bytes that were sent. Undef if send failed.
sub send {
    my ($h, $stream, $class, $message) = @_;
    my $sock = $h->{'sock'};

    if (not defined $sock) {
        warn "Trying to send via disconnected client";
        return undef;
    }

    my $m = {'stream' => $stream, 'class' => $class, 'object' => $message};

    my $j = encode_json($m);

    $sendByteArray->($sock, $j);
#    my $len = pack('N', (length $j));
#    print $sock $len.$j or return undef;

    return length $j;
}

# Receive a message from the S4 cluster.
# Returns a HASH ref representing the message that
# was received. It has the following entries:
#    stream: Name of stream on which this message was received
#    class: Class of Java object corresponding to this message
#    object: JSON representation of the message object.
#
# If an error occurs during the receive, undef is returned.
sub recv {
    my ($h) = @_;
    my $sock = $h->{'sock'};
    my $sel = $h->{'select'};
    if (not defined $sock) {
        warn "Trying to recv via disconnected client";
        return undef;
    }

    my $b = $readByteArray->($sock, $sel);

    my $message = ($b ? decode_json($b) : undef);

    return $message;
}

1;

