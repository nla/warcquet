/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import com.google.common.net.InternetDomainName;
import org.netpreserve.jwarc.URIs;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;

public class MutableCaptureEvent implements CaptureEvent {
    private String url;
    private Instant date;
    private String filename;
    private long responsePosition;
    private long responseLength;
    private String responseRecordType;
    private UUID responseUUID;
    private String responsePayloadType;
    private Long responsePayloadLength;
    private byte[] responsePayloadSha1;
    private Long requestPosition;
    private Long requestLength;
    private UUID requestUUID;
    private String requestPayloadType;
    private Long requestPayloadLength;
    private byte[] requestPayloadSha1;

    private String refersToUrl;
    private Instant refersToDate;
    private UUID refersToUUID;
    private Integer httpStatus;
    private String httpMethod;
    private String via;
    private Integer ipv4;
    private byte[] ipv6;
    private String redirect;

    private String software;
    private String softwareVersion;
    private String server;
    private String serverVersion;

    private String surtKey;
    private String surtDomain;
    private String surtRegistry;
    private String hopsFromSeed;

    private static String surt(InternetDomainName name) {
        return String.join(",", name.parts().reverse());
    }
    public void setUrl(String url) {
        this.url = url;
        surtKey = URIs.toNormalizedSurt(url);
        URI uri = URIs.parseLeniently(url);
        String host = uri.getHost();
        try {
            InternetDomainName domainName = InternetDomainName.from(host);
            setSurtDomain(surt(domainName.topPrivateDomain()));
            setSurtRegistry(surt(domainName.registrySuffix()));
        } catch (IllegalArgumentException e) {
            // just ignore invalid domains
        }
    }

    public void setDate(Instant date) {
        this.date = date;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public void setResponsePosition(long responsePosition) {
        this.responsePosition = responsePosition;
    }

    public void setRequestPosition(Long requestPosition) {
        this.requestPosition = requestPosition;
    }

    public void setRequestLength(Long requestLength) {
        this.requestLength = requestLength;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public Instant getDate() {
        return date;
    }

    @Override
    public String getFilename() {
        return filename;
    }

    @Override
    public long getResponsePosition() {
        return responsePosition;
    }

    @Override
    public Long getRequestPosition() {
        return requestPosition;
    }

    @Override
    public Long getRequestLength() {
        return requestLength;
    }

    public long getResponseLength() {
        return responseLength;
    }

    public void setResponseLength(long responseLength) {
        this.responseLength = responseLength;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(Integer httpStatus) {
        this.httpStatus = httpStatus;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public String getVia() {
        return via;
    }

    public void setVia(String via) {
        this.via = via;
    }

    public void setIpAddress(InetAddress inetAddress) {
        if (inetAddress instanceof Inet4Address) {
            byte[] addr = inetAddress.getAddress();
            ipv4 = (addr[0] & 0xFF) << 24 +
                    (addr[1] & 0xFF) << 16 +
                    (addr[2] & 0xFF) << 8 +
                    (addr[3] & 0xFF);
        } else {
            ipv6 = inetAddress.getAddress();
        }
    }

    @Override
    public Integer getIpv4() {
        return ipv4;
    }

    @Override
    public byte[] getIpv6() {
        return ipv6;
    }

    @Override
    public String getResponsePayloadType() {
        return responsePayloadType;
    }

    public void setResponsePayloadType(String responsePayloadType) {
        this.responsePayloadType = responsePayloadType;
    }

    @Override
    public Long getResponsePayloadLength() {
        return responsePayloadLength;
    }

    public void setResponsePayloadLength(Long payloadLength) {
        this.responsePayloadLength = payloadLength;
    }

    public byte[] getResponsePayloadSha1() {
        return responsePayloadSha1;
    }

    public void setResponsePayloadSha1(byte[] payloadSha1) {
        this.responsePayloadSha1 = payloadSha1;
    }

    public void setResponseUUID(UUID responseUUID) {
        this.responseUUID = responseUUID;
    }

    public UUID getResponseUUID() {
        return responseUUID;
    }

    public void setRequestUUID(UUID requestUUID) {
        this.requestUUID = requestUUID;
    }

    public UUID getRequestUUID() {
        return requestUUID;
    }

    public String getRedirect() {
        return redirect;
    }

    public void setRedirect(String redirect) {
        this.redirect = redirect;
    }

    public String getSurtKey() {
        return surtKey;
    }

    public void setSurtKey(String surtKey) {
        this.surtKey = surtKey;
    }

    public String getSurtDomain() {
        return surtDomain;
    }

    public void setSurtDomain(String surtDomain) {
        this.surtDomain = surtDomain;
    }

    public String getSurtRegistry() {
        return surtRegistry;
    }

    public void setSurtRegistry(String surtRegistry) {
        this.surtRegistry = surtRegistry;
    }

    @Override
    public String getRequestPayloadType() {
        return requestPayloadType;
    }

    public void setRequestPayloadType(String requestPayloadType) {
        this.requestPayloadType = requestPayloadType;
    }

    @Override
    public Long getRequestPayloadLength() {
        return requestPayloadLength;
    }

    public void setRequestPayloadLength(Long requestPayloadLength) {
        this.requestPayloadLength = requestPayloadLength;
    }

    @Override
    public byte[] getRequestPayloadSha1() {
        return requestPayloadSha1;
    }

    public void setRequestPayloadSha1(byte[] requestPayloadSha1) {
        this.requestPayloadSha1 = requestPayloadSha1;
    }

    public void setHopsFromSeed(String hopsFromSeed) {
        this.hopsFromSeed = hopsFromSeed;
    }

    public String getHopsFromSeed() {
        return hopsFromSeed;
    }

    public String getRefersToUrl() {
        return refersToUrl;
    }

    public void setRefersToUrl(String refersToUrl) {
        this.refersToUrl = refersToUrl;
    }

    public Instant getRefersToDate() {
        return refersToDate;
    }

    public void setRefersToDate(Instant refersToDate) {
        this.refersToDate = refersToDate;
    }

    public UUID getRefersToUUID() {
        return refersToUUID;
    }

    public void setRefersToUUID(UUID refersToUUID) {
        this.refersToUUID = refersToUUID;
    }

    public String getResponseRecordType() {
        return responseRecordType;
    }

    public void setResponseRecordType(String responseWarcType) {
        this.responseRecordType = responseWarcType;
    }

    public String getSoftware() {
        return software;
    }

    public void setSoftware(String software) {
        this.software = software;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public void setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }
}
