package io.suboptimal.connectjava.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectCompressionRegistryTest {

    @Test
    void standardGzipRoundTrips() throws IOException {
        ConnectCompression gzip = requireNonNull(ConnectCompressionRegistry.standard().resolve("gzip"));
        ByteBuf input = Unpooled.copiedBuffer("hello gzip", StandardCharsets.UTF_8);
        ByteBuf compressed = null;
        ByteBuf decompressed = null;
        try {
            compressed = gzip.compress(input, ByteBufAllocator.DEFAULT);
            decompressed = gzip.decompress(compressed, ByteBufAllocator.DEFAULT);

            assertThat(decompressed.toString(StandardCharsets.UTF_8)).isEqualTo("hello gzip");
        } finally {
            input.release();
            if (compressed != null) {
                compressed.release();
            }
            if (decompressed != null) {
                decompressed.release();
            }
        }
    }

    @Test
    void identityRoundTripsWithoutNullSentinel() throws IOException {
        ConnectCompression identity = ConnectIdentityCompression.INSTANCE;
        ByteBuf input = Unpooled.copiedBuffer("plain", StandardCharsets.UTF_8);
        ByteBuf compressed = null;
        ByteBuf decompressed = null;
        try {
            compressed = identity.compress(input, ByteBufAllocator.DEFAULT);
            decompressed = identity.decompress(compressed, ByteBufAllocator.DEFAULT);

            assertThat(identity.isIdentity()).isTrue();
            assertThat(decompressed.toString(StandardCharsets.UTF_8)).isEqualTo("plain");
        } finally {
            input.release();
            if (compressed != null) {
                compressed.release();
            }
            if (decompressed != null) {
                decompressed.release();
            }
        }
    }

    @Test
    void rejectsReservedIdentityName() {
        assertThatThrownBy(() -> ConnectCompressionRegistry.builder()
                .register(new NoopCompression("identity"))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("identity");
    }

    @Test
    void rejectsDuplicateNames() {
        assertThatThrownBy(() -> ConnectCompressionRegistry.builder()
                .register(new NoopCompression("custom"))
                .register(new NoopCompression("custom"))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate compression name");
    }

    @Test
    void rejectsNonCanonicalNames() {
        assertThatThrownBy(() -> ConnectCompressionRegistry.builder()
                .register(new NoopCompression("GZip"))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("canonical lower-case");
    }

    @Test
    void separatesAdvertisedAndSupportedNames() {
        ConnectCompressionRegistry registry = ConnectCompressionRegistry.standard();

        assertThat(registry.advertisedNames()).containsExactly("gzip");
        assertThat(registry.supportedNames()).containsExactly("identity", "gzip");
        assertThat(registry.resolve("identity")).isSameAs(ConnectIdentityCompression.INSTANCE);
    }

    @Test
    void resolvesNamesCaseInsensitively() {
        ConnectCompressionRegistry registry = ConnectCompressionRegistry.standard();

        assertThat(registry.resolve("GZip")).isSameAs(registry.resolve("gzip"));
        assertThat(requireNonNull(registry.resolve("GZip")).name()).isEqualTo("gzip");
    }

    @Test
    void resolvesUnsupportedNamesAsNull() {
        ConnectCompressionRegistry registry = ConnectCompressionRegistry.standard();

        assertThat(registry.resolve("br")).isNull();
    }

    record NoopCompression(String name) implements ConnectCompression {
        @Override
        public ByteBuf compress(ByteBuf input, ByteBufAllocator alloc) {
            return input.retainedDuplicate();
        }

        @Override
        public ByteBuf decompress(ByteBuf input, ByteBufAllocator alloc) {
            return input.retainedDuplicate();
        }
    }
}
