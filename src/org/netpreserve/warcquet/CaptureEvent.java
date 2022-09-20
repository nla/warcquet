/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

public interface CaptureEvent {
    String getUrl();
    Instant getDate();

    String getFilename();
    long getResponsePosition();
    String getResponsePayloadType();
    Long getResponsePayloadLength();
    byte[] getResponsePayloadSha1();

    Long getRequestPosition();
    Long getRequestLength();
    String getRequestPayloadType();
    Long getRequestPayloadLength();
    byte[] getRequestPayloadSha1();

    Integer getHttpStatus();

    String getHttpMethod();

    String getVia();

    Integer getIpv4();

    byte[] getIpv6();

    UUID getResponseUUID();

    UUID getRequestUUID();

    String getRedirect();

    String getSurtKey();

    String getSurtDomain();

    String getSurtRegistry();

    String getHopsFromSeed();

    String getResponseRecordType();

    String getRefersToUrl();

    Instant getRefersToDate();

    UUID getRefersToUUID();

    String getSoftware();

    String getSoftwareVersion();

    String getServer();

    String getServerVersion();
}
