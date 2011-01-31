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

from struct import (pack, pack_into, unpack_from)
import sys
import time
from ctypes import create_string_buffer

# Helper Classes

# Byte-oriented IO
class _ByteIO:
    _debug = False;

    def recv_bytes(self, sock, n, timeout=0):
      b = bytearray()
      r = 0

      tNow = time.time()
      tEnd = tNow + timeout
      tStart = tNow

      while (r < n):
        if (timeout > 0):
          sock.settimeout(tEnd - tNow)

        p = sock.recv(n-r) # partial recv
        b.extend(p)
        r += len(p)
        tNow = time.time()

      return [b, (tNow-tStart)]

# Better: r += sock.recv_into(buffer(b, r), (n-r));
# But doesn't work on MacOS X :(

    def recv_byte_array(self, sock, timeout=0):
      [s, t0] = self.recv_bytes(sock, 4, timeout)
      sz = unpack_from('>I', buffer(s))[0]

      tr = 0;
      if (timeout > 0):
          if (t0 < timeout):
              tr = timeout - t0;
          else:
              tr = 0.001;

      [m, t1] = self.recv_bytes(sock, sz, tr)

      if self._debug:
          print >> sys.stderr, ">>[" + str(sz) + "]" + str(m)

      return [m, t0+t1]

    def send_byte_array(self, sock, b):
      sz = pack('>I', len(b));
      sock.sendall(sz);
      sock.sendall(b);

      if self._debug:
          print >> sys.stderr, "<<[" + str(len(b)) + "]" + str(b)

