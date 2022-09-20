/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet.util;

import org.apache.parquet.io.DelegatingPositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

class SimplePositionOutputStream extends DelegatingPositionOutputStream {
    private long position = 0;

    public SimplePositionOutputStream(OutputStream stream) {
        super(stream);
    }

    @Override
    public void write(int b) throws IOException {
        super.write(b);
        position += 1;
    }

    @Override
    public void write(byte[] b) throws IOException {
        super.write(b);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        position += len;
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }
}
