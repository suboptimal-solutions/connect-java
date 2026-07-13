package io.suboptimal.connectjava.protocol.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectEndStreamMetaBuilderTest {
    @Test
    void buildReturnsEmptySingletonWhenNoMetadataWasAdded() {
        assertThat(ConnectEndStreamMeta.builder().build()).isSameAs(ConnectEndStreamMeta.EMPTY);
    }

    @Test
    void normalizesNamesToLowercase() {
        ConnectEndStreamMeta meta = ConnectEndStreamMeta.builder()
            .add("X-Trace-ID", "abc")
            .build();

        assertThat(meta.names()).containsExactly("x-trace-id");
        assertThat(meta.getAll("x-trace-id")).containsExactly("abc");
    }

    @Test
    void coalescesDifferentlyCasedNames() {
        ConnectEndStreamMeta meta = ConnectEndStreamMeta.builder()
            .add("Operation-Cost", "7")
            .add("operation-cost", "11")
            .set("OPERATION-COST", "13")
            .add("operation-cost", "17")
            .build();

        assertThat(meta.names()).containsExactly("operation-cost");
        assertThat(meta.getAll("Operation-Cost")).containsExactly("13", "17");
    }
}
