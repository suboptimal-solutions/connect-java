package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.suboptimal.connectjava.api.ConnectClientCallStart;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectProtocolHttpHeaders;
import org.jspecify.annotations.Nullable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
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
            HttpHeaderNames.HOST.toString(),
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString(),
            HttpHeaderNames.ACCEPT_ENCODING.toString());

    /** Protocol-managed headers for streaming requests; user values for these are ignored. */
    private static final Set<String> STREAMING_RESERVED_HEADERS = Set.of(
            HttpHeaderNames.HOST.toString(),
            HttpHeaderNames.CONTENT_TYPE.toString(),
            HttpHeaderNames.CONTENT_LENGTH.toString(),
            ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION.toString(),
            HttpHeaderNames.CONTENT_ENCODING.toString(),
            HttpHeaderNames.TRANSFER_ENCODING.toString(),
            ConnectProtocolHttpHeaders.CONNECT_CONTENT_ENCODING.toString(),
            ConnectProtocolHttpHeaders.CONNECT_ACCEPT_ENCODING.toString());

    private ClientHandlerSupport() {}

    /**
     * Resolves the HTTP/1.1 {@code Host} authority for an outgoing request. Prefers the logical
     * {@code authority} carried on the call (e.g. propagated by a higher layer such as the gRPC
     * bridge); otherwise derives {@code host} or {@code host:port} from the channel's remote
     * address. The default HTTP port (80) is omitted. Returns {@code null} when no authority can be
     * determined (e.g. a channel with no {@link InetSocketAddress} remote), in which case the caller
     * should not set the header.
     */
    static @Nullable String resolveAuthority(ConnectClientCallStart callStart, ChannelHandlerContext ctx) {
        return resolveAuthority(callStart.authority(), ctx.channel().remoteAddress());
    }

    static @Nullable String resolveAuthority(@Nullable String authority, @Nullable SocketAddress remote) {
        if (authority != null && !authority.isBlank()) {
            return authority;
        }
        if (remote instanceof InetSocketAddress addr) {
            String host = addr.getHostString();
            int port = addr.getPort();
            return (port > 0 && port != 80) ? host + ":" + port : host;
        }
        return null;
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
        String headerValue = (values == null || values.isEmpty()) ? null : values.getFirst();
        String name = ConnectCompressionNegotiation.compressionNameFor(headerValue);
        return ConnectCompressionNegotiation.resolveOrIdentity(config.compressionRegistry(), name);
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

    /** Hop-by-hop response headers (RFC 7230 §6.1) that must not surface as application metadata. */
    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            HttpHeaderNames.CONNECTION.toString(),
            HttpHeaderNames.KEEP_ALIVE.toString(),
            HttpHeaderNames.TRANSFER_ENCODING.toString(),
            HttpHeaderNames.TE.toString(),
            HttpHeaderNames.TRAILER.toString(),
            HttpHeaderNames.UPGRADE.toString(),
            HttpHeaderNames.PROXY_AUTHENTICATE.toString(),
            HttpHeaderNames.PROXY_AUTHORIZATION.toString());

    /**
     * Converts response headers to a lower-cased name-to-values map for surfacing as application
     * metadata, dropping HTTP/1 hop-by-hop headers (RFC 7230 §6.1). The excluded set is the standard
     * hop-by-hop headers plus any names listed in the {@code Connection} header's own value.
     */
    static Map<String, List<String>> toApplicationHeaderMap(HttpHeaders headers) {
        Set<String> excluded = hopByHopHeaders(headers);
        return headers
                .entries()
                .stream()
                .filter(e -> !excluded.contains(e.getKey().toLowerCase(Locale.ROOT)))
                .collect(Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(Locale.ROOT),
                        e -> List.of(e.getValue()),
                        (l1,l2) -> Stream.concat(l1.stream(), l2.stream()).toList()));
    }

    /** Standard hop-by-hop set, augmented with any header names listed in the {@code Connection} value. */
    private static Set<String> hopByHopHeaders(HttpHeaders headers) {
        List<String> connectionValues = headers.getAll(HttpHeaderNames.CONNECTION);
        if (connectionValues.isEmpty()) {
            return HOP_BY_HOP_HEADERS;
        }
        Set<String> excluded = new HashSet<>(HOP_BY_HOP_HEADERS);
        for (String value : connectionValues) {
            for (String token : value.split(",")) {
                String name = token.trim().toLowerCase(Locale.ROOT);
                if (!name.isEmpty()) {
                    excluded.add(name);
                }
            }
        }
        return excluded;
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
