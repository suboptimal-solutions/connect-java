package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.compression.ConnectGzipCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectCompressionNegotiationTest {
    private static final ConnectCompression GZIP = ConnectGzipCompression.INSTANCE;
    private static final ConnectCompressionRegistry REGISTRY = ConnectCompressionRegistry.builder().register(GZIP).build();

    @Test
    void absentAcceptHeaderUsesIdentity() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, null, REGISTRY);

        assertThat(selected.isIdentity()).isTrue();
    }

    @Test
    void explicitIdentityUsesIdentity() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, "identity", REGISTRY);

        assertThat(selected.isIdentity()).isTrue();
    }

    @Test
    void supportedEncodingUsesServerPreference() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, "gzip", REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }

    @Test
    void wildcardCanSelectSupportedEncoding() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, "*", REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }

    @Test
    void qZeroRejectsThatEncodingAndFallsBackToIdentity() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, "gzip;q=0", REGISTRY);

        assertThat(selected.isIdentity()).isTrue();
    }

    @Test
    void acceptEncodingNamesAreCaseInsensitive() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            ConnectIdentityCompression.INSTANCE, "GZip", REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }

    @Test
    void requestCompressionGzipWinsWhenAcceptAbsent() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            GZIP, null, REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }

    @Test
    void requestCompressionGzipWinsWhenAcceptIdentity() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            GZIP, "identity", REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }

    @Test
    void requestCompressionGzipWinsWhenAcceptRejectsGzip() {
        ConnectCompression selected = ConnectCompressionNegotiation.selectResponseEncoding(
            GZIP, "gzip;q=0", REGISTRY);

        assertThat(selected.name()).isEqualTo("gzip");
    }
}
