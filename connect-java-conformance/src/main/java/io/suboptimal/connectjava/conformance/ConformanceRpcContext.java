package io.suboptimal.connectjava.conformance;

import io.grpc.Context;
import io.grpc.Metadata;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Per-call {@link Context} keys populated by {@link ConformanceMetadataInterceptor}.
 *
 * <p>Standard gRPC {@code *ImplBase} stubs only ever see a {@link io.grpc.stub.StreamObserver}, so
 * reading the request's raw metadata or contributing response headers/trailers from inside a
 * service method is only possible via an interceptor that stashes mutable {@link Metadata} in the
 * {@link Context}. This is the canonical gRPC pattern for that, not something specific to Connect.
 */
final class ConformanceRpcContext {
    static final Context.Key<Metadata> REQUEST_HEADERS = Context.key("conformance-request-headers");
    static final Context.Key<Metadata> RESPONSE_HEADERS = Context.key("conformance-response-headers");
    static final Context.Key<Metadata> RESPONSE_TRAILERS = Context.key("conformance-response-trailers");

    private static final Base64.Encoder BINARY_ENCODER = Base64.getEncoder().withoutPadding();
    private static final Base64.Decoder BINARY_DECODER = Base64.getDecoder();

    static Metadata requestHeaders() {
        return REQUEST_HEADERS.get();
    }

    static Metadata responseHeaders() {
        return RESPONSE_HEADERS.get();
    }

    static Metadata responseTrailers() {
        return RESPONSE_TRAILERS.get();
    }

    /** Adds a value to {@code metadata}, decoding it first if {@code name} is a binary (-bin) key. */
    static void put(Metadata metadata, String name, String value) {
        if (isBinary(name)) {
            metadata.put(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER), BINARY_DECODER.decode(value));
        } else {
            metadata.put(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER), value);
        }
    }

    /** Returns all values of {@code name} in {@code metadata}, base64-encoding binary (-bin) values. */
    static List<String> getAll(Metadata metadata, String name) {
        if (isBinary(name)) {
            Iterable<byte[]> values = metadata.getAll(Metadata.Key.of(name, Metadata.BINARY_BYTE_MARSHALLER));
            if (values == null) {
                return List.of();
            }
            List<String> encoded = new ArrayList<>();
            for (byte[] value : values) {
                encoded.add(BINARY_ENCODER.encodeToString(value));
            }
            return encoded;
        }
        Iterable<String> values = metadata.getAll(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER));
        if (values == null) {
            return List.of();
        }
        List<String> copy = new ArrayList<>();
        values.forEach(copy::add);
        return copy;
    }

    private static boolean isBinary(String name) {
        return name.endsWith(Metadata.BINARY_HEADER_SUFFIX);
    }

    private ConformanceRpcContext() {}
}
