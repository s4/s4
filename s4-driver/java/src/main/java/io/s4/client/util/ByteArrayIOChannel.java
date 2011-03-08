/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.client.util;

import io.s4.client.IOChannel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class ByteArrayIOChannel implements IOChannel {
    private InputStream in;
    private OutputStream out;
    private Socket socket;

    public ByteArrayIOChannel(Socket socket) throws IOException {
        in = socket.getInputStream();
        out = socket.getOutputStream();
        this.socket = socket;
    }

    private int readBytes(byte[] s, int n, int timeout) throws IOException {
        int r = 0; // bytes read so far

        long tStart = System.currentTimeMillis();
        long tEnd = tStart + timeout;
        long tRem = timeout;
        long tNow = tStart;

        do {
            socket.setSoTimeout((int) tRem);

            // keep reading bytes till the required "n" are read
            int p = in.read(s, r, (n - r));

            if (p == -1) {
                throw new IOException("reached end of stream after reading "
                        + r + " bytes. expected " + n + " bytes");
            }

            r += p;

            tNow = System.currentTimeMillis();

            tRem = tEnd - tNow;

        } while (r < n && (timeout == 0 || tRem > 0));

        return (int) (tNow - tStart);
    }

    public byte[] recv() throws IOException {
        return recv(0);
    }

    public byte[] recv(int timeout) throws IOException {
        // first read size of byte array.
        // unsigned int, big endian: 0A0B0C0D -> {0A, 0B, 0C, 0D}
        byte[] s = { 0, 0, 0, 0 };
        int tUsed = readBytes(s, 4, timeout);

        if (timeout > 0 && (timeout - tUsed <= 1)) {
            throw new SocketTimeoutException("recv timed out");
        }

        int size = (int) ( // NOTE: type cast not necessary for int
        (0xff & s[0]) << 24 | (0xff & s[1]) << 16 | (0xff & s[2]) << 8 | (0xff & s[3]) << 0);

        if (size == 0)
            return null;

        byte[] v = new byte[size];

        // read the message
        int tRem = (timeout > 0 ? timeout - tUsed : 0);
        readBytes(v, size, tRem);

        return v;
    }

    public void send(byte[] v) throws IOException {
        byte[] s = { 0, 0, 0, 0 };
        int size = v.length;

        s[3] = (byte) (size & 0xff);
        size >>= 8;
        s[2] = (byte) (size & 0xff);
        size >>= 8;
        s[1] = (byte) (size & 0xff);
        size >>= 8;
        s[0] = (byte) (size & 0xff);

        out.write(s);
        out.write(v);
        out.flush();
    }
}
