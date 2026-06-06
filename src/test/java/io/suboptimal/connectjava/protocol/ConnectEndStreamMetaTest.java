package io.suboptimal.connectjava.protocol;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectEndStreamMetaTest {
    @Test
    void emptySingletonHasNoMetadata() {
        assertThat(ConnectEndStreamMeta.EMPTY.isEmpty()).isTrue();
        assertThat(ConnectEndStreamMeta.EMPTY.names()).isEmpty();
        assertThat(ConnectEndStreamMeta.EMPTY.getAll("missing")).isEmpty();
    }

    @Test
    void lookupsAreCaseInsensitive() {
        ConnectEndStreamMeta meta = ConnectEndStreamMeta.builder()
            .add("X-Test", "value")
            .build();

        assertThat(meta.getAll("x-test")).containsExactly("value");
        assertThat(meta.getAll("X-TEST")).containsExactly("value");
    }

    @Test
    void namesPreserveInsertionOrder() {
        ConnectEndStreamMeta meta = ConnectEndStreamMeta.builder()
            .add("beta", "2")
            .add("alpha", "1")
            .build();

        assertThat(meta.names()).containsExactly("beta", "alpha");
    }
}
