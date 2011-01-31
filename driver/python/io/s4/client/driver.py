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

import socket
import sys
import json
from struct import (pack, unpack)

from helper import _ByteIO

class Driver:

    protocolName = "generic-json"
    versionMajor = 1;
    versionMinor = 0;

    _bio = _ByteIO();
    _debug = False;

    def setDebug(self, d):
        self._debug = d;
        self._bio._debug = d;

    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.state = "null"


    def initialize(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(True)
        sock.connect((self.hostname, self.port))

        self._bio.send_byte_array(sock, bytearray(0));

        [response, t] = self._bio.recv_byte_array(sock)

        sock.close()

        r = json.loads(response.decode('utf-8'))

        if not self._iscompatible(r['protocol']):
            return False;

        self.uuid = r['uuid'];
        self.state = "initialized"

        if self._debug:
          print >> sys.stderr, "Initialized. uuid: " + self.uuid;

        return True


    def connect(self, spec = {'readMode': 'private', 'writeMode': 'enabled'}):
        if self.state != 'initialized':
            return False;

        conn = spec;
        conn['uuid'] = self.uuid;
        cstr = json.dumps(conn).encode('utf-8');
 
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(True)
        sock.connect((self.hostname, self.port))
 
        self._bio.send_byte_array(sock, cstr);
 
        [response, t]  = self._bio.recv_byte_array(sock);
        r = json.loads(response.decode('utf-8'));
 
        if r['status'] == 'ok':
            if self._debug:
               print >> sys.stderr, "Connected"
            self.state = "connected"
            self.sock = sock;
            return True;
        else:
            if self._debug:
                print >> sys.stderr, "Connect failed. " + response;
            sock.close();
            return False;

    def sendKeyed(self, stream, clazz, keys, object):
        if self.state != 'connected':
            return 0;
        self._send(stream, clazz, keys, object);

    def send(self, stream, clazz, object):
        if self.state != 'connected':
            return 0;
        self._send(stream, clazz, None, object);

    def recv(self, timeout=0):
        if self.state != 'connected':
            return None;
        [b, t] = self._bio.recv_byte_array(self.sock, timeout);
        if b == None or len(b) == 0:
            return None;

        m = json.loads(b.decode('utf-8'));

        return m;

    def recvAll(self, interval):
        if self.state != 'connected':
            return False;

        messages = [];

        try:
            tr = interval
            while (tr > 0):
                [b, t] = self._bio.recv_byte_array(self.sock, tr);
                if b == None or len(b) == 0:
                    break

                m = json.loads(b.decode('utf-8'));
                messages.append(m);

                tr = tr - t

        except socket.timeout:
            # Nothing to do here
            1

        return messages;

    def disconnect(self):
        if self.state != 'connected':
            return False;

        sock = self.sock;
        self._bio.send_byte_array(sock, bytearray(0));
        sock.close();

        return True;

# PRIVATE FUNCTIONS
    def _send(self, stream, clazz, keys, object):
        message = {
            'stream': str(stream),
            'class': str(clazz),
            'object': str(object)
        }

        if keys != None and isinstance(keys, ListType):
            message['keys'] = map(str, keys); # convert all keys to strings

        m = json.dumps(message);
        self._bio.send_byte_array(self.sock, m.encode('utf-8'));

    def _iscompatible(self, p):
      return (p['name'] == self.protocolName
             and p['versionMajor'] == self.versionMajor
             and p['versionMinor'] >= self.versionMinor)
             


