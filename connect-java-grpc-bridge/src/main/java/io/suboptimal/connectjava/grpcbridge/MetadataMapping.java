package io.suboptimal.connectjava.grpcbridge;

import io.grpc.Metadata;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import org.jspecify.annotations.Nullable;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bidirectional mapping between gRPC {@link Metadata} and Connect request/response metadata.
 *
 * <p>Both protocols use the same wire convention for binary metadata: a key suffixed with
 * {@code -bin} carries base64-encoded bytes, because HTTP headers cannot hold raw bytes.
 * gRPC omits the base64 padding when encoding, so this class does the same; decoding accepts
 * padded and unpadded input alike.
 *
 * <p>Response headers and trailers are handled by separate methods because
 * {@link ConnectResponseHeadersBuilder} and {@link ConnectResponseTrailersBuilder} share no
 * common supertype. The loop is duplicated rather than abstracted behind a callback so that
 * translating metadata allocates nothing beyond the encoded values themselves.
 */
final class MetadataMapping {
    /**
     * Header names owned by the HTTP, Connect or gRPC protocol itself, which must never be
     * forwarded as user metadata on a response. A service that sets one of these would otherwise
     * corrupt the framing a Connect client relies on, or duplicate protocol state: notably
     * {@code grpc-status-details-bin}, whose payload the bridge already surfaces in the
     * {@code details} field of the Connect error.
     *
     * <p>Mirrors {@code protocolHeaders} in connect-go, which filters the same set when merging
     * user metadata into a response.
     */
    private static final Set<String> RESERVED_HEADERS = Set.of(
        // HTTP
        "content-type", "content-length", "content-encoding", "host", "user-agent",
        "trailer", "date",
        // Connect
        "accept-encoding", "connect-content-encoding", "connect-accept-encoding",
        "connect-timeout-ms", "connect-protocol-version",
        // gRPC
        "grpc-encoding", "grpc-accept-encoding", "grpc-timeout",
        "grpc-status", "grpc-message", "grpc-status-details-bin");

    /** Prefix Connect uses to convey trailers as headers on unary responses. */
    private static final String CONNECT_TRAILER_PREFIX = "trailer-";

    private static final Base64.Encoder BINARY_ENCODER = Base64.getEncoder().withoutPadding();
    private static final Base64.Decoder BINARY_DECODER = Base64.getDecoder();

    /**
     * Builds gRPC request metadata from Connect request headers.
     *
     * @throws IllegalArgumentException if a {@code -bin} header does not hold valid base64
     */
    static Metadata extractMetadataFromConnectRequestHeaders(ConnectRequestMeta requestMeta) {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, List<String>> entry : requestMeta.headers().entrySet()) {
            String name = entry.getKey();
            if (isBinary(name)) {
                Metadata.Key<byte[]> key = Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER);
                for (String value : entry.getValue()) {
                    metadata.put(key, BINARY_DECODER.decode(value));
                }
            } else {
                Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
                for (String value : entry.getValue()) {
                    metadata.put(key, value);
                }
            }
        }
        return metadata;
    }

    /**
     * Copies gRPC response headers into the Connect response headers builder, skipping
     * {@linkplain #isReserved reserved protocol headers}.
     */
    static void putMetadataToConnectResponseHeaders(
        Metadata metadata, ConnectResponseHeadersBuilder builder)
    {
        for (String name : metadata.keys()) {
            if (isReserved(name)) {
                continue;
            }
            if (isBinary(name)) {
                Metadata.Key<byte[]> key = Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER);
                Iterable<byte[]> values = metadata.getAll(key);
                if (values != null) {
                    for (byte[] value : values) {
                        builder.add(name, BINARY_ENCODER.encodeToString(value));
                    }
                }
            } else {
                Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
                Iterable<String> values = metadata.getAll(key);
                if (values != null) {
                    for (String value : values) {
                        builder.add(name, value);
                    }
                }
            }
        }
    }

    /**
     * Copies gRPC trailers into the Connect response trailers builder, skipping
     * {@linkplain #isReserved reserved protocol headers}.
     */
    static void putMetadataToConnectResponseTrailers(
        Metadata metadata, ConnectResponseTrailersBuilder builder)
    {
        for (String name : metadata.keys()) {
            if (isReserved(name)) {
                continue;
            }
            if (isBinary(name)) {
                Metadata.Key<byte[]> key = Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER);
                Iterable<byte[]> values = metadata.getAll(key);
                if (values != null) {
                    for (byte[] value : values) {
                        builder.add(name, BINARY_ENCODER.encodeToString(value));
                    }
                }
            } else {
                Metadata.Key<String> key = Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
                Iterable<String> values = metadata.getAll(key);
                if (values != null) {
                    for (String value : values) {
                        builder.add(name, value);
                    }
                }
            }
        }
    }

    private static boolean isBinary(String name) {
        return name.endsWith(Metadata.BINARY_HEADER_SUFFIX);
    }

    /**
     * Whether the protocol owns this name, so a service must not set it on a response.
     *
     * <p>Applies to responses only: inbound request headers are passed through untouched, so a
     * service can still inspect what the client actually sent. connect-go draws the same line.
     */
    private static boolean isReserved(String name) {
        return RESERVED_HEADERS.contains(name) || name.startsWith(CONNECT_TRAILER_PREFIX);
    }

    private MetadataMapping() {}
}
