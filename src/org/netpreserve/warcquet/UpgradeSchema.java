/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.api.*;
import org.netpreserve.warcquet.util.PathInputFile;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.function.Consumer;

public class UpgradeSchema {
    public static void main(String[] args) throws IOException {
        Path inFile = Paths.get(args[0]);
        try (var reader = new WarcquetReader(inFile);
             var writer = Warcquet.newWriter(Paths.get(args[1]))) {
            for (CaptureEvent event: reader) {
                writer.write(event);
            }
        }
    }
}
