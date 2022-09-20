/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.UUID;

class WarcquetWriteSupport extends WriteSupport<CaptureEvent> {
    private RecordConsumer recordConsumer;
    private MessageType schema;

    @Override
    public WriteContext init(Configuration configuration) {
        this.schema = Warcquet.schema();
        return new WriteContext(schema, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    private void add(int index, String field, String value) {
        checkFieldIndex(index, field);
        if (value == null) return;
        recordConsumer.startField(field, index);
        recordConsumer.addBinary(Binary.fromString(value));
        recordConsumer.endField(field, index);
    }

    private void add(int index, String field, byte[] value) {
        checkFieldIndex(index, field);
        if (value == null) return;
        recordConsumer.startField(field, index);
        recordConsumer.addBinary(Binary.fromConstantByteArray(value));
        recordConsumer.endField(field, index);
    }

    private void add(int index, String field, long value) {
        checkFieldIndex(index, field);
        recordConsumer.startField(field, index);
        recordConsumer.addLong(value);
        recordConsumer.endField(field, index);
    }

    private void add(int index, String field, Long value) {
        checkFieldIndex(index, field);
        if (value == null) return;
        recordConsumer.startField(field, index);
        recordConsumer.addLong(value);
        recordConsumer.endField(field, index);
    }

    private void add(int index, String field, Instant value) {
        add(index, field, value == null ? null : value.toEpochMilli());
    }

    private void add(int index, String field, Integer value) {
        checkFieldIndex(index, field);
        if (value == null) return;
        recordConsumer.startField(field, index);
        recordConsumer.addInteger(value);
        recordConsumer.endField(field, index);
    }

    private void checkFieldIndex(int index, String field) {
        if (!schema.getFieldName(index).equals(field)) {
            throw new IllegalArgumentException("wrong field index: " + field);
        }
    }

    private void add(int index, String field, UUID value) {
        if (value == null) return;
        recordConsumer.startField(field, index);
        var buffer = ByteBuffer.allocate(16);
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
        recordConsumer.addBinary(Binary.fromConstantByteArray(buffer.array()));
        recordConsumer.endField(field, index);
    }


    @Override
    public void write(CaptureEvent event) {
        recordConsumer.startMessage();

        int i = 0;
        add(i++, "url", event.getUrl());
        add(i++, "date", event.getDate());

        add(i++, "filename", event.getFilename());
        add(i++, "response_position", event.getResponsePosition());
        add(i++, "response_length", event.getResponsePosition());
        add(i++, "response_record_type", event.getResponseRecordType());
        add(i++, "response_uuid", event.getResponseUUID());
        add(i++, "response_payload_type", event.getResponsePayloadType());
        add(i++, "response_payload_length", event.getResponsePayloadLength());
        add(i++, "response_payload_sha1", event.getResponsePayloadSha1());

        add(i++, "request_position", event.getRequestPosition());
        add(i++, "request_length", event.getRequestLength());
        add(i++, "request_uuid", event.getRequestUUID());
        add(i++, "request_payload_type", event.getRequestPayloadType());
        add(i++, "request_payload_length", event.getRequestPayloadLength());
        add(i++, "request_payload_sha1", event.getRequestPayloadSha1());

        add(i++, "refers_to_url", event.getRefersToUrl());
        add(i++, "refers_to_date", event.getRefersToDate());
        add(i++, "refers_to_uuid", event.getRefersToUUID());

        add(i++, "http_status", event.getHttpStatus());
        add(i++, "http_method", event.getHttpMethod());
        add(i++, "hops_from_seed", event.getHopsFromSeed());
        add(i++, "via", event.getVia());
        add(i++, "ipv4", event.getIpv4());
        add(i++, "ipv6", event.getIpv6());
        add(i++, "redirect", event.getRedirect());
        add(i++, "software", event.getSoftware());
        add(i++, "software_version", event.getSoftwareVersion());
        add(i++, "server", event.getServer());
        add(i++, "server_version", event.getServerVersion());
        
        add(i++, "surt_key", event.getSurtKey());
        add(i++, "surt_domain", event.getSurtDomain());
        add(i++, "surt_registry", event.getSurtRegistry());

        recordConsumer.endMessage();
    }
}
