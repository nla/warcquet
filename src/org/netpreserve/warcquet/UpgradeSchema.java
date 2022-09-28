/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import com.google.common.net.InetAddresses;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.api.*;
import org.netpreserve.warcquet.util.PathInputFile;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

public class UpgradeSchema {
    private static PrimitiveConverter binaryConverter(Consumer<Binary> consumer) {
        return new PrimitiveConverter() {
            @Override
            public void addBinary(Binary value) {
                consumer.accept(value);
            }
        };
    }

    private static PrimitiveConverter longConverter(Consumer<Long> consumer) {
        return new PrimitiveConverter() {
            @Override
            public void addLong(long value) {
                consumer.accept(value);
            }
        };
    }

    private static PrimitiveConverter intConverter(Consumer<Integer> consumer) {
        return new PrimitiveConverter() {
            @Override
            public void addInt(int value) {
                consumer.accept(value);
            }
        };
    }

    private static UUID binaryToUUID(Binary value) {
        var buffer = value.toByteBuffer();
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    private static InetAddress binaryToInet6Address(Binary value) {
        try {
            return Inet6Address.getByAddress(value.getBytesUnsafe());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public static class CaptureEventMaterializer extends RecordMaterializer<CaptureEvent> {
        MutableCaptureEvent event = new MutableCaptureEvent();
        Converter[] converters = new Converter[]{
                binaryConverter(value -> event.setUrl(value.toStringUsingUTF8())),
                longConverter(value -> event.setDate(Instant.ofEpochMilli(value))),

                binaryConverter(value -> event.setFilename(value.toStringUsingUTF8())),
                longConverter(value -> event.setResponsePosition(value)),
                longConverter(value -> event.setResponseLength(value)),
                binaryConverter(value -> event.setResponseRecordType(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setResponseUUID(binaryToUUID(value))),
                binaryConverter(value -> event.setResponsePayloadType(value.toStringUsingUTF8())),
                longConverter(value -> event.setResponsePayloadLength(value)),
                binaryConverter(value -> event.setResponsePayloadSha1(value.getBytes())),

                longConverter(value -> event.setRequestPosition(value)),
                longConverter(value -> event.setRequestLength(value)),
                binaryConverter(value -> event.setRequestUUID(binaryToUUID(value))),
                binaryConverter(value -> event.setRequestPayloadType(value.toStringUsingUTF8())),
                longConverter(value -> event.setRequestPayloadLength(value)),
                binaryConverter(value -> event.setRequestPayloadSha1(value.getBytes())),

                binaryConverter(value -> event.setRefersToUrl(value.toStringUsingUTF8())),
                longConverter(value -> event.setRefersToDate(Instant.ofEpochMilli(value))),
                binaryConverter(value -> event.setRefersToUUID(binaryToUUID(value))),

                intConverter(value -> event.setHttpStatus(value)),
                binaryConverter(value -> event.setHttpMethod(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setHopsFromSeed(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setVia(value.toStringUsingUTF8())),
                intConverter(value -> event.setIpAddress(InetAddresses.fromInteger(value))),
                binaryConverter(value -> event.setIpAddress(binaryToInet6Address(value))),
                binaryConverter(value -> event.setRedirect(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setSoftware(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setSoftwareVersion(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setServer(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setServerVersion(value.toStringUsingUTF8())),

                binaryConverter(value -> event.setSurtKey(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setSurtDomain(value.toStringUsingUTF8())),
                binaryConverter(value -> event.setSurtRegistry(value.toStringUsingUTF8())),
        };

        @Override
        public CaptureEvent getCurrentRecord() {
            return event;
        }

        @Override
        public GroupConverter getRootConverter() {
            return new GroupConverter() {
                @Override
                public Converter getConverter(int fieldIndex) {
                    return converters[fieldIndex];
                }

                @Override
                public void start() {
                    event = new MutableCaptureEvent();
                }

                @Override
                public void end() {
                }
            };
        }
    }

    public static void main(String[] args) throws IOException {
        try (var reader = ParquetFileReader.open(new PathInputFile(Paths.get(args[0])))) {
            var schema = reader.getFileMetaData().getSchema();
            var columnIO = new ColumnIOFactory().getColumnIO(schema);
            var materializer = new CaptureEventMaterializer();

            var rowGroup = reader.readNextRowGroup();

            var recordReader = columnIO.getRecordReader(rowGroup, materializer);
            var record = recordReader.read();
            System.out.println(record.getUrl());

        }
    }
}
