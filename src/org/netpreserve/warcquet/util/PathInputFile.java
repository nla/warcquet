/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet.util;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class PathInputFile implements InputFile {
    private final Path path;

    public PathInputFile(Path path) {
        this.path = path;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(path);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        FileChannel channel = FileChannel.open(path);
        return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {
            @Override
            public long getPos() throws IOException {
                return channel.position();
            }

            @Override
            public void seek(long newPos) throws IOException {
                channel.position(newPos);
            }
        };
    }
}
