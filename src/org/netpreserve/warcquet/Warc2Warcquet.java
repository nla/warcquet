/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2022 National Library of Australia
 */

package org.netpreserve.warcquet;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.netpreserve.jwarc.*;
import org.netpreserve.warcquet.util.PathOutputFile;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Warc2Warcquet {
    private String filename;
    private final Set<URI> concurrentIdSet = new HashSet<>();
    private final ParquetWriter<CaptureEvent> writer;
    private MutableCaptureEvent event;
    boolean inCaptureEvent = false;
    private String referrer;
    private boolean verbose;
    private String software;
    private String softwareVersion;

    private Warc2Warcquet(ParquetWriter<CaptureEvent> writer, boolean verbose) {
        this.writer = writer;
        this.verbose = verbose;
    }

    private void startFile(String filename) {
        softwareVersion = null;
        software = null;
        this.filename = filename;
    }

    private void startWarcinfo(Warcinfo warcinfo, long position) throws IOException {
        if (warcinfo.contentType().equals(MediaType.WARC_FIELDS)) {
            String softwareField = warcinfo.fields().first("Software").orElse(null);
            if (softwareField != null) {
                int slash = softwareField.indexOf('/');
                if (slash >= 0) {
                    software = softwareField.substring(0, slash);
                    int space = softwareField.indexOf(' ', slash);
                    softwareVersion = softwareField.substring(slash + 1, space >= 0 ? space : softwareField.length());
                } else {
                    software = softwareField;
                    softwareVersion = null;
                }
            }
        }
    }

    public void startResponse(WarcResponse response, long position) throws IOException {
        startResponseOrResource(response, position);
        if (response.contentType().equals(MediaType.HTTP_RESPONSE)) {
            handleHttpResponse(response.http());
        }
    }

    private void handleHttpResponse(HttpResponse httpResponse) throws IOException {
        event.setHttpStatus(httpResponse.status());
        httpResponse.headers().first("Location").ifPresent(event::setRedirect);
        String serverField = httpResponse.headers().first("Server").orElse(null);
        if (serverField != null) {
            int slash = serverField.indexOf('/');
            if (slash >= 0) {
                event.setServer(serverField.substring(0, slash));
                int space = serverField.indexOf(' ', slash);
                event.setServerVersion(serverField.substring(slash + 1, space >= 0 ? space : serverField.length()));
            } else {
                event.setServer(serverField);
            }
        }
    }

    public void startResource(WarcResource resource, long position) throws IOException {
        startResponseOrResource(resource, position);
    }

    public UUID getRecordUUID(WarcRecord record) {
        URI id = record.id();
        return idToUUID(id);
    }

    private static UUID idToUUID(URI id) {
        if (!id.getScheme().equals("urn")) return null;
        var part = id.getSchemeSpecificPart();
        if (!part.startsWith("uuid:")) return null;
        return UUID.fromString(part.substring(5));
    }

    public void startResponseOrResource(WarcCaptureRecord record, long position) throws IOException {
        String url = record.target();
        event.setUrl(url);
        event.setDate(record.date());
        event.setResponsePosition(position);
        event.setResponseRecordType(record.type());
        event.setResponseUUID(getRecordUUID(record));
        record.ipAddress().ifPresent(event::setIpAddress);
        var payload = record.payload().orElse(null);
        if (payload != null) {
            try {
                event.setResponsePayloadType(payload.type().base().toString());
            } catch (IllegalArgumentException e) {
                // ignore bad media-type
            }
            byte[] sha1 = consumeAndSha1Payload(payload);
            event.setResponsePayloadLength(payload.body().position());
        }
    }

    private void startRevisit(WarcRevisit revisit, long position) throws IOException {
        startResponseOrResource(revisit, position);
        revisit.refersTo().map(Warc2Warcquet::idToUUID).ifPresent(event::setRefersToUUID);
        revisit.refersToDate().ifPresent(event::setRefersToDate);
        revisit.headers().first("WARC-Refers-To-Target-URI").ifPresent(event::setRefersToUrl);
        if (revisit.contentType().equals(MediaType.HTTP_RESPONSE)) {
            handleHttpResponse(revisit.http());
        }
    }

    public byte[] consumeAndSha1Payload(WarcPayload payload) throws IOException {
        var digest = payload.digest().orElse(null);
        if (digest != null && digest.algorithm().equals("sha1")) {
            try {
                payload.body().consume();
            } catch (EOFException e) {
                // truncated
            }
            return digest.bytes();
        } else {
            Hasher hasher = Hashing.sha1().newHasher();
            try {
                ByteStreams.copy(payload.body().stream(), Funnels.asOutputStream(hasher));
            } catch (EOFException e) {
                // truncated
            }
            return hasher.hash().asBytes();
        }
    }

    private void endResponseOrResource(WarcCaptureRecord record, long position, long length) {
        event.setResponsePosition(position);
        event.setResponseLength(length);
    }

    public void startRequest(WarcRequest request, long position) throws IOException {
        if (request.contentType().equals(MediaType.HTTP_REQUEST)) {
            event.setHttpMethod(request.http().method());
            referrer = request.http().headers().first("Referer").orElse(null);
        }
        request.ipAddress().ifPresent(event::setIpAddress);
        event.setRequestUUID(getRecordUUID(request));
        var payload = request.payload().orElse(null);
        if (payload != null) {
            try {
                event.setRequestPayloadType(payload.type().base().toString());
            } catch (IllegalArgumentException e) {
                // ignore bad media-type
            }
            byte[] sha1 = consumeAndSha1Payload(payload);
            event.setRequestPayloadLength(payload.body().position());
            if (event.getRequestPayloadLength() > 0) {
                event.setRequestPayloadSha1(sha1);
            }
        }
    }

    public void endRequest(WarcRequest request, long position, long length) {
        event.setRequestPosition(position);
        event.setRequestLength(length);
    }

    public void startMetadata(WarcMetadata metadata, long position) throws IOException {
        if (metadata.contentType().equals(MediaType.WARC_FIELDS)) {
            event.setVia(metadata.fields().first("via").orElse(null));
            event.setHopsFromSeed(metadata.fields().first("hopsFromSeed").orElse(null));
        }
    }

    private void startCaptureEvent() {
        if (verbose) System.out.println("-- capture --");
        event = new MutableCaptureEvent();
        event.setFilename(filename);
        event.setSoftware(software);
        event.setSoftwareVersion(softwareVersion);
    }

    private void endCaptureEvent() throws IOException {
        if (event.getVia() == null && referrer != null) {
            event.setVia(referrer);
        }
        writer.write(event);
        event = null;
        referrer = null;
    }

    private boolean isConcurrentToCurrentEvent(WarcCaptureRecord record) {
        if (concurrentIdSet.contains(record.id())) {
            return true;
        }
        for (URI id : record.concurrentTo()) {
            if (concurrentIdSet.contains(id)) {
                return true;
            }
        }
        return false;
    }

    public void startRecord(WarcRecord record, long position) throws IOException {
        if (record instanceof WarcCaptureRecord) {
            WarcCaptureRecord captureRecord = (WarcCaptureRecord) record;
            if (!isConcurrentToCurrentEvent(captureRecord)) {
                // record is not concurrent so start a new capture event
                if (inCaptureEvent) {
                    endCaptureEvent();
                    concurrentIdSet.clear();
                    inCaptureEvent = false;
                }
                startCaptureEvent();
                inCaptureEvent = true;
            }
            concurrentIdSet.add(captureRecord.id());
            concurrentIdSet.addAll(captureRecord.concurrentTo());
        } else {
            // we encountered a non-capture record so end any active capture event
            if (inCaptureEvent) {
                endCaptureEvent();
                concurrentIdSet.clear();
                inCaptureEvent = false;
            }
        }

        if (verbose) System.out.println(record);
        if (record instanceof WarcRequest) {
            startRequest((WarcRequest) record, position);
        } else if (record instanceof WarcResponse) {
            startResponse((WarcResponse) record, position);
        } else if (record instanceof WarcResource) {
            startResource((WarcResource) record, position);
        } else if (record instanceof WarcRevisit) {
            startRevisit((WarcRevisit) record, position);
        } else if (record instanceof WarcMetadata) {
            startMetadata((WarcMetadata) record, position);
        } else if (record instanceof Warcinfo) {
            startWarcinfo((Warcinfo) record, position);
        }
    }

    public void endRecord(WarcRecord record, long position, long length) {
        if (record instanceof WarcRequest) {
            endRequest((WarcRequest) record, position, length);
        } else if (record instanceof WarcResponse) {
            endResponseOrResource((WarcResponse) record, position, length);
        } else if (record instanceof WarcResource) {
            endResponseOrResource((WarcResource) record, position, length);
        } else if (record instanceof WarcRevisit) {
            endResponseOrResource((WarcRevisit)record, position, length);
        }
    }

    public void scan(WarcReader reader, String filename) throws IOException {
        startFile(filename);
        WarcRecord record = reader.next().orElse(null);
        while (record != null) {
            long position = reader.position();
            startRecord(record, position);
            WarcRecord next = reader.next().orElse(null);
            long length = reader.position() - position;
            endRecord(record, position, length);
            record = next;
        }
        if (!concurrentIdSet.isEmpty()) {
            endCaptureEvent();
        }
    }

    public static void main(String args[]) throws IOException {
        Path outFile = null;
        var warcFiles = new ArrayList<String>();
        CompressionCodecName compression = CompressionCodecName.UNCOMPRESSED;
        ParquetProperties.WriterVersion parquetVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
        boolean verbose = false;

        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                switch (args[i]) {
                    case "-o":
                    case "--output-file":
                        outFile = Paths.get(args[++i]);
                        break;
                    case "-c":
                    case "--compression":
                        compression = CompressionCodecName.fromConf(args[++i]);
                        break;
                    case "--parquet-version":
                        parquetVersion = ParquetProperties.WriterVersion.fromString(args[++i]);
                        break;
                    case "--verbose":
                    case "-v":
                        verbose = true;
                        break;
                    case "-h":
                    case "--help":
                        System.out.print("Usage: Warc2Parquet [OPTIONS] -o outfile.parquet warc-files...\n" +
                                "\n" +
                                "Options:\n" +
                                "  -c, --compression CODEC     Output compression codec " + Arrays.asList(CompressionCodecName.values()) + "\n" +
                                "  -o, --output-file FILE      Output parquet file (mandatory)\n" +
                                "      --parquet-version VERS  Output parquet version (v1, v2)\n" +
                                "  -v, --verbose               Increase logging detail\n");
                        System.exit(0);
                        break;
                    default:
                        System.err.println("Warc2Parquet: unrecognized option: '" + args[i] + "'");
                        System.exit(1);
                }
            } else {
                warcFiles.add(args[i]);
            }
        }

        if (outFile == null) {
            System.err.println("Warc2Parquet: an output file (-o) must be specified. See --help for usage information");
            System.exit(1);
        }

        if (warcFiles.isEmpty()) {
            System.err.println("Warc2Parquet: at least one input file must be specified. See --help for usage information");
            System.exit(1);
        }

        try (var writer = new WarcquetWriterBuilder(new PathOutputFile(outFile))
                .withCompressionCodec(compression)
                .withWriterVersion(parquetVersion)
                .build()) {
            Warc2Warcquet converter = new Warc2Warcquet(writer, verbose);
            for (String warcFile : warcFiles) {
                try (var reader = openWarcReader(warcFile)) {
                    String filename = warcFile.replaceAll(".*[/\\\\]", "");
                    converter.scan(reader, filename);
                }
            }
        }
    }

    private static WarcReader openWarcReader(String pathOrUrl) throws IOException {
        if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) {
            return new WarcReader(new URL(pathOrUrl).openStream());
        } else {
            return new WarcReader(Paths.get(pathOrUrl));
        }
    }
}
