package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectAttributeKey;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ConnectRequestMetaTest {
    @Test
    void headerValuesIsCaseInsensitive() {
        ConnectRequestMeta meta = new ConnectRequestMeta(
            Map.of("content-type", List.of("application/proto")));

        assertThat(meta.headerValues("Content-Type")).containsExactly("application/proto");
    }

    @Test
    void headerValuesReturnsEmptyListForAbsentName() {
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        assertThat(meta.headerValues("x-missing")).isEmpty();
    }

    @Test
    void firstHeaderReturnsFirstValue() {
        ConnectRequestMeta meta = new ConnectRequestMeta(
            Map.of("x-tag", List.of("first", "second")));

        assertThat(meta.firstHeader("x-tag")).isEqualTo("first");
    }

    @Test
    void firstHeaderReturnsNullForAbsentName() {
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        assertThat(meta.firstHeader("x-missing")).isNull();
    }

    @Test
    void customAttributesRoundTrip() {
        ConnectAttributeKey<String> key = ConnectAttributeKey.valueOf("test.meta.round-trip");
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        meta.put(key, "value");

        assertThat(meta.get(key)).isEqualTo("value");
    }

    @Test
    void containsReturnsTrueAfterPut() {
        ConnectAttributeKey<String> key = ConnectAttributeKey.valueOf("test.meta.contains-present");
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        meta.put(key, "x");

        assertThat(meta.contains(key)).isTrue();
    }

    @Test
    void containsReturnsFalseWhenAbsent() {
        ConnectAttributeKey<String> key = ConnectAttributeKey.valueOf("test.meta.contains-absent");
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        assertThat(meta.contains(key)).isFalse();
    }

    @Test
    void getReturnsNullWhenAbsent() {
        ConnectAttributeKey<String> key = ConnectAttributeKey.valueOf("test.meta.get-absent");
        ConnectRequestMeta meta = new ConnectRequestMeta(Map.of());

        assertThat(meta.get(key)).isNull();
    }

    @Test
    void headersAreImmutableAfterConstruction() {
        List<String> values = new ArrayList<>(List.of("bar"));
        Map<String, List<String>> headers = Map.of("x-foo", values);
        ConnectRequestMeta meta = new ConnectRequestMeta(headers);

        values.add("baz");

        assertThat(meta.headerValues("x-foo")).containsExactly("bar");
        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> meta.headers().put("x-bar", List.of("qux")));
    }
}
