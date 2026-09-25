package io.suboptimal.connectjava.grpcbridge;

import io.grpc.Metadata;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataMappingTest {

    static final Metadata.Key<String> ASCII_KEY =
        Metadata.Key.of("x-ascii", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<byte[]> BINARY_KEY =
        Metadata.Key.of("x-binary-bin", Metadata.BINARY_BYTE_MARSHALLER);

    // ------------------------------------------------------------- request headers -> Metadata

    @Test
    void extractsAsciiRequestHeaders() {
        Metadata metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(Map.of("x-ascii", List.of("value"))));

        assertThat(metadata.get(ASCII_KEY)).isEqualTo("value");
    }

    @Test
    void extractsRepeatedRequestHeaderValues() {
        Metadata metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(Map.of("x-ascii", List.of("one", "two"))));

        assertThat(metadata.getAll(ASCII_KEY)).containsExactly("one", "two");
    }

    @Test
    void extractsBinaryRequestHeaderWithoutPadding() {
        byte[] raw = {1, 2, 3, (byte) 0xFF};
        String encoded = Base64.getEncoder().withoutPadding().encodeToString(raw);

        Metadata metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(Map.of("x-binary-bin", List.of(encoded))));

        assertThat(metadata.get(BINARY_KEY)).isEqualTo(raw);
    }

    @Test
    void extractsBinaryRequestHeaderWithPadding() {
        byte[] raw = {1, 2, 3, (byte) 0xFF};
        String encoded = Base64.getEncoder().encodeToString(raw);
        assertThat(encoded).endsWith("=");

        Metadata metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(Map.of("x-binary-bin", List.of(encoded))));

        assertThat(metadata.get(BINARY_KEY)).isEqualTo(raw);
    }

    @Test
    void rejectsMalformedBinaryRequestHeader() {
        ConnectRequestMeta meta =
            new ConnectRequestMeta(Map.of("x-binary-bin", List.of("not base64 !!")));

        assertThatThrownBy(() -> MetadataMapping.extractMetadataFromConnectRequestHeaders(meta))
            .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------- Metadata -> response headers

    @Test
    void putsAsciiAndBinaryResponseHeaders() {
        byte[] raw = {10, 20, 30};
        Metadata metadata = new Metadata();
        metadata.put(ASCII_KEY, "value");
        metadata.put(BINARY_KEY, raw);

        var builder = new RecordingHeadersBuilder();
        MetadataMapping.putMetadataToConnectResponseHeaders(metadata, builder);

        assertThat(builder.valuesOf("x-ascii")).containsExactly("value");
        assertThat(builder.valuesOf("x-binary-bin"))
            .containsExactly(Base64.getEncoder().withoutPadding().encodeToString(raw));
    }

    @Test
    void encodesBinaryResponseHeaderWithoutPadding() {
        Metadata metadata = new Metadata();
        metadata.put(BINARY_KEY, new byte[]{1, 2, 3, (byte) 0xFF});

        var builder = new RecordingHeadersBuilder();
        MetadataMapping.putMetadataToConnectResponseHeaders(metadata, builder);

        assertThat(builder.valuesOf("x-binary-bin")).singleElement()
            .asString().doesNotContain("=");
    }

    @Test
    void putsRepeatedResponseHeaderValues() {
        Metadata metadata = new Metadata();
        metadata.put(ASCII_KEY, "one");
        metadata.put(ASCII_KEY, "two");

        var builder = new RecordingHeadersBuilder();
        MetadataMapping.putMetadataToConnectResponseHeaders(metadata, builder);

        assertThat(builder.valuesOf("x-ascii")).containsExactly("one", "two");
    }

    // ------------------------------------------------------------- Metadata -> response trailers

    @Test
    void putsAsciiAndBinaryResponseTrailers() {
        byte[] raw = {42, 43};
        Metadata metadata = new Metadata();
        metadata.put(ASCII_KEY, "value");
        metadata.put(BINARY_KEY, raw);

        var builder = new RecordingTrailersBuilder();
        MetadataMapping.putMetadataToConnectResponseTrailers(metadata, builder);

        assertThat(builder.valuesOf("x-ascii")).containsExactly("value");
        assertThat(builder.valuesOf("x-binary-bin"))
            .containsExactly(Base64.getEncoder().withoutPadding().encodeToString(raw));
    }

    @Test
    void doesNotForwardStatusDetailsTrailer() {
        Metadata.Key<byte[]> statusDetails =
            Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER);
        Metadata metadata = new Metadata();
        metadata.put(statusDetails, new byte[]{1, 2, 3});
        metadata.put(ASCII_KEY, "kept");

        var builder = new RecordingTrailersBuilder();
        MetadataMapping.putMetadataToConnectResponseTrailers(metadata, builder);

        assertThat(builder.valuesOf("grpc-status-details-bin")).isEmpty();
        assertThat(builder.valuesOf("x-ascii")).containsExactly("kept");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "content-type", "content-length", "content-encoding", "host", "user-agent",
        "trailer", "date",
        "accept-encoding", "connect-content-encoding", "connect-accept-encoding",
        "connect-timeout-ms", "connect-protocol-version",
        "grpc-encoding", "grpc-accept-encoding", "grpc-timeout",
        "grpc-status", "grpc-message",
        "trailer-anything"})
    void doesNotForwardReservedProtocolHeaders(String reservedName) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of(reservedName, Metadata.ASCII_STRING_MARSHALLER), "leaked");
        metadata.put(ASCII_KEY, "kept");

        var headers = new RecordingHeadersBuilder();
        MetadataMapping.putMetadataToConnectResponseHeaders(metadata, headers);
        var trailers = new RecordingTrailersBuilder();
        MetadataMapping.putMetadataToConnectResponseTrailers(metadata, trailers);

        assertThat(headers.valuesOf(reservedName)).isEmpty();
        assertThat(trailers.valuesOf(reservedName)).isEmpty();
        assertThat(headers.valuesOf("x-ascii")).containsExactly("kept");
        assertThat(trailers.valuesOf("x-ascii")).containsExactly("kept");
    }

    @Test
    void reservedNamesAreStillAcceptedOnInboundRequestHeaders() {
        // Responses are filtered, requests are not: a service may inspect what the client sent.
        Metadata metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(Map.of("content-type", List.of("application/proto"))));

        assertThat(metadata.get(Metadata.Key.of("content-type", Metadata.ASCII_STRING_MARSHALLER)))
            .isEqualTo("application/proto");
    }

    // ------------------------------------------------------------- round trip

    @Test
    void binaryValueSurvivesResponseThenRequestRoundTrip() {
        byte[] raw = {0, 1, 2, (byte) 0x80, (byte) 0xFF};
        Metadata outbound = new Metadata();
        outbound.put(BINARY_KEY, raw);

        var builder = new RecordingHeadersBuilder();
        MetadataMapping.putMetadataToConnectResponseHeaders(outbound, builder);

        Metadata inbound = MetadataMapping.extractMetadataFromConnectRequestHeaders(
            new ConnectRequestMeta(builder.recorded));

        assertThat(inbound.get(BINARY_KEY)).isEqualTo(raw);
    }

    // ------------------------------------------------------------- recording builders

    static class RecordingHeadersBuilder implements ConnectResponseHeadersBuilder {
        final Map<String, List<String>> recorded = new LinkedHashMap<>();

        @Override
        public ConnectResponseHeadersBuilder add(CharSequence name, CharSequence value) {
            recorded.computeIfAbsent(name.toString(), k -> new ArrayList<>())
                .add(value.toString());
            return this;
        }

        @Override
        public ConnectResponseHeadersBuilder set(CharSequence name, CharSequence value) {
            var values = new ArrayList<String>();
            values.add(value.toString());
            recorded.put(name.toString(), values);
            return this;
        }

        List<String> valuesOf(String name) {
            return recorded.getOrDefault(name, List.of());
        }
    }

    static class RecordingTrailersBuilder implements ConnectResponseTrailersBuilder {
        final Map<String, List<String>> recorded = new LinkedHashMap<>();

        @Override
        public ConnectResponseTrailersBuilder add(CharSequence name, CharSequence value) {
            recorded.computeIfAbsent(name.toString(), k -> new ArrayList<>())
                .add(value.toString());
            return this;
        }

        @Override
        public ConnectResponseTrailersBuilder set(CharSequence name, CharSequence value) {
            var values = new ArrayList<String>();
            values.add(value.toString());
            recorded.put(name.toString(), values);
            return this;
        }

        List<String> valuesOf(String name) {
            return recorded.getOrDefault(name, List.of());
        }
    }
}
