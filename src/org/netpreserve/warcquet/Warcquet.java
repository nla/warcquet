/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.netpreserve.warcquet.util.PathOutputFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class Warcquet {
    private static MessageType schema;

    public static WarcquetWriterBuilder newWriterBuilder(Path path) {
        return new WarcquetWriterBuilder(new PathOutputFile(path));
    }

    public static ParquetWriter<CaptureEvent> newWriter(Path path) throws IOException {
        return new WarcquetWriterBuilder(new PathOutputFile(path)).build();
    }

    public static MessageType schema() {
        if (schema == null) {
            try (InputStream stream = Warc2Warcquet.class.getResourceAsStream("warcquet-schema.txt")) {
                if (stream == null) throw new RuntimeException("Missing classpath resource: warcquet-schema.txt");
                schema = MessageTypeParser.parseMessageType(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new RuntimeException("Failed to load warcquet-schema.txt", e);
            }
        }
        return schema;
    }
}
