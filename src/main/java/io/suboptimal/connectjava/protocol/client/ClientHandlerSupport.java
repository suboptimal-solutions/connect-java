package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectProtocolHttpHeaders;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Shared helpers for the client-side Connect handlers. The unary (POST/GET) and streaming
 * handlers all derive the request codec and encoding from the caller's headers, copy
 * user headers while skipping protocol-managed ones, and convert Netty headers to a map.
 */
class ClientHandlerSupport {
    /** Protocol-managed headers for unary requests; user values for these are ignored. */
    private static final Set<String> UNARY_RESERVED_HEADERS = Set.of(
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString(),
            HttpHeaderNames.ACCEPT_ENCODING.toString());

    /** Protocol-managed headers for streaming requests; user values for these are ignored. */
    private static final Set<String> STREAMING_RESERVED_HEADERS = Set.of(
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString(),
            HttpHeaderNames.TRANSFER_ENCODING.toString(),
            ConnectProtocolHttpHeaders.CONNECT_CONTENT_ENCODING.toString(),
            ConnectProtocolHttpHeaders.CONNECT_ACCEPT_ENCODING.toString());

    private ClientHandlerSupport() {}

    /** Maps a {@code Content-Type} value to a codec name, or {@code null} if unrecognized. */
    @Nullable
    static String codecNameForContentType(@Nullable String contentType) {
        if (contentType == null) {
            return null;
        }
        int semicolonIdx = contentType.indexOf(';');
        String mimeType = semicolonIdx >= 0
            ? contentType.substring(0, semicolonIdx).trim()
            : contentType.trim();
        return switch (mimeType.toLowerCase(Locale.ROOT)) {
            case "application/proto", "application/connect+proto" -> "proto";
            case "application/json", "application/connect+json" -> "json";
            default -> null;
        };
    }

    /**
     * Selects the request codec by explicit name, falling back to the registry's preferred codec
     * when {@code codecName} is {@code null} or not registered.
     */
    static ConnectCodec selectRequestCodec(ConnectClientProtocolConfig config,
                                           @Nullable String codecName)
    {
        if (codecName != null) {
            ConnectCodec codec = config.codecRegistry().byName(codecName);
            if (codec != null) {
                return codec;
            }
        }

        return config.codecRegistry().preferred().getFirst();
    }

    /**
     * Resolves the request compression from the caller's {@code content-encoding} header,
     * defaulting to identity when absent or unsupported.
     */
    static ConnectCompression selectRequestEncoding(ConnectClientProtocolConfig config,
                                                    Map<String, List<String>> requestHeaders)
    {
        List<String> values = requestHeaders.get(HttpHeaderNames.CONTENT_ENCODING.toString());
        if (values == null || values.isEmpty()) {
            return ConnectIdentityCompression.INSTANCE;
        }

        String name = ConnectCompressionNegotiation.compressionNameFor(values.getFirst());
        if (name == null) {
            return ConnectIdentityCompression.INSTANCE;
        }

        ConnectCompression compression = config.compressionRegistry().resolve(name);
        return compression != null ? compression : ConnectIdentityCompression.INSTANCE;
    }

    static void copyUserHeadersForUnaryCall(Map<String, List<String>> source, HttpHeaders target) {
        fillHttpHeaders(source, target, UNARY_RESERVED_HEADERS);
    }

    static void copyUserHeadersForStreamCall(Map<String, List<String>> source, HttpHeaders target) {
        fillHttpHeaders(source, target, STREAMING_RESERVED_HEADERS);
    }

    private static void fillHttpHeaders(Map<String, List<String>> source,
                                        HttpHeaders target,
                                        Set<String> reserved)
    {
        source
                .entrySet()
                .stream()
                .filter(e -> !reserved.contains(e.getKey().toLowerCase(Locale.ROOT)))
                .forEach(e -> target.add(e.getKey(), e.getValue()));
    }

    /** Converts Netty headers to a lower-cased name-to-values map. */
    static Map<String, List<String>> toHeaderMap(HttpHeaders headers) {
        return headers
                .entries()
                .stream()
                .collect(Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(Locale.ROOT),
                        e -> List.of(e.getValue()),
                        (l1,l2) -> Stream.concat(l1.stream(), l2.stream()).toList()));
    }

    /** Maps a non-200 HTTP status to the closest Connect error code (client-side table). */
    static ConnectErrorCode httpStatusToErrorCode(int status) {
        return switch (status) {
            case 400 -> ConnectErrorCode.INTERNAL;
            case 401 -> ConnectErrorCode.UNAUTHENTICATED;
            case 403 -> ConnectErrorCode.PERMISSION_DENIED;
            case 404 -> ConnectErrorCode.UNIMPLEMENTED;
            case 429, 502, 503, 504 -> ConnectErrorCode.UNAVAILABLE;
            default -> ConnectErrorCode.UNKNOWN;
        };
    }

    /** Returns the {@link ConnectErrorCode} whose wire name equals {@code wireName}, or {@code null}. */
    static @Nullable ConnectErrorCode findErrorCodeByWireName(String wireName) {
        for (ConnectErrorCode code : ConnectErrorCode.values()) {
            if (code.wireName().equals(wireName)) {
                return code;
            }
        }
        return null;
    }

    /** Reads all readable bytes of {@code buf} into a new array without advancing its reader index. */
    static byte[] toByteArray(ByteBuf buf) {
        int length = buf.readableBytes();
        if (length == 0) {
            return new byte[0];
        } else {
            byte[] bytes = new byte[length];
            buf.getBytes(buf.readerIndex(), bytes);
            return bytes;
        }
    }
}
