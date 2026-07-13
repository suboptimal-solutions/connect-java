package io.suboptimal.connectjava.protocol.server;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResponseTrailersBuilderTest {
    @Test
    void appliesUnaryTrailersWithTrailerPrefixInOrder() {
        HttpHeaders target = new DefaultHttpHeaders();
        new ResponseTrailersBuilder()
            .add("Operation-Cost", "old")
            .set("Operation-Cost", "7")
            .add("Operation-Cost", "13")
            .applyTo(target);

        assertThat(target.getAll("Trailer-Operation-Cost")).containsExactly("7", "13");
    }

    @Test
    void preservesTrailerNameCaseForHttpHeadersSink() {
        HttpHeaders target = new DefaultHttpHeaders();
        new ResponseTrailersBuilder()
            .set("Trace-Id", "abc")
            .applyTo(target);

        assertThat(target.names()).contains("Trailer-Trace-Id");
        assertThat(target.get("trailer-trace-id")).isEqualTo("abc");
    }

    @Test
    void appliesStreamingTrailersToEndStreamMetadata() {
        ConnectEndStreamMeta.Builder meta = ConnectEndStreamMeta.builder();
        new ResponseTrailersBuilder()
            .add("Operation-Cost", "7")
            .add("operation-cost", "13")
            .set("Trace-Id", "abc")
            .applyTo(meta);

        ConnectEndStreamMeta snapshot = meta.build();
        assertThat(snapshot.names()).containsExactly("operation-cost", "trace-id");
        assertThat(snapshot.getAll("OPERATION-COST")).containsExactly("7", "13");
        assertThat(snapshot.getAll("trace-id")).containsExactly("abc");
    }
}
