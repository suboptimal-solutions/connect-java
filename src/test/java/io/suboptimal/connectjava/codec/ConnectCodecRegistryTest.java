package io.suboptimal.connectjava.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectCodecRegistryTest {

    @Test
    void registersCodecsInServerPreferenceOrder() {
        ConnectCodec first = new NoopCodec("proto");
        ConnectCodec second = new NoopCodec("json");

        ConnectCodecRegistry registry = ConnectCodecRegistry.builder()
            .register(first)
            .register(second)
            .build();

        assertThat(registry.preferred())
            .extracting(ConnectCodec::name)
            .containsExactly("proto", "json");
        assertThat(registry.byName("PROTO")).isSameAs(first);
        assertThat(registry.byName("json")).isSameAs(second);
    }

    @Test
    void rejectsDuplicateNames() {
        assertThatThrownBy(() -> ConnectCodecRegistry.builder()
                .register(new NoopCodec("proto"))
                .register(new NoopCodec("proto"))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Duplicate codec name");
    }

    @Test
    void rejectsNonCanonicalNames() {
        assertThatThrownBy(() -> ConnectCodecRegistry.builder()
                .register(new NoopCodec("Proto"))
                .build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("canonical lower-case");
    }

    record NoopCodec(String name) implements ConnectCodec {
        @Override
        public ByteBuf encode(Object value, ByteBufAllocator alloc) throws IOException {
            throw new IOException("not implemented");
        }

        @Override
        public <T> T decode(ByteBuf bytes, Class<T> type) throws IOException {
            throw new IOException("not implemented");
        }
    }
}
