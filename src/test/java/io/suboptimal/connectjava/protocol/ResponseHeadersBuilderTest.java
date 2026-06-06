package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResponseHeadersBuilderTest {
    @Test
    void appliesSetAndAddOperationsInOrder() {
        HttpHeaders target = new DefaultHttpHeaders();
        new ResponseHeadersBuilder()
            .add("x-order", "old")
            .set("x-order", "new")
            .add("x-order", "last")
            .applyTo(target);

        assertThat(target.getAll("x-order")).containsExactly("new", "last");
    }

    @Test
    void preservesHeaderNameCaseForHttpHeadersSink() {
        HttpHeaders target = new DefaultHttpHeaders();
        new ResponseHeadersBuilder()
            .set("X-Trace-Id", "abc")
            .applyTo(target);

        assertThat(target.names()).contains("X-Trace-Id");
        assertThat(target.get("x-trace-id")).isEqualTo("abc");
    }
}
