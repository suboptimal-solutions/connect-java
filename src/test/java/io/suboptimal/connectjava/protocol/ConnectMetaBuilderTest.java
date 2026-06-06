package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectMetaBuilderTest {

    private static ConnectRequestMeta requestMeta(HttpHeaders headers) {
        return ConnectMetaBuilder.fromHeaders(headers);
    }

    @Test
    void lowercasesHeaderNames() {
        HttpHeaders headers = new DefaultHttpHeaders()
            .set("X-Tenant-Id", "acme")
            .set("Connect-Protocol-Version", "1");

        ConnectRequestMeta meta = requestMeta(headers);

        assertThat(meta.headers())
            .containsKey("x-tenant-id")
            .containsKey("connect-protocol-version")
            .doesNotContainKey("X-Tenant-Id")
            .doesNotContainKey("Connect-Protocol-Version");
    }

    @Test
    void preservesSingleHeaderValue() {
        HttpHeaders headers = new DefaultHttpHeaders().set("X-Value", "MiXeD  Value");

        ConnectRequestMeta meta = requestMeta(headers);

        assertThat(meta.headerValues("x-value")).containsExactly("MiXeD  Value");
    }

    @Test
    void collectsRepeatedHeaderValues() {
        HttpHeaders headers = new DefaultHttpHeaders()
            .add("X-Tag", "first")
            .add("X-Tag", "second")
            .add("X-Tag", "third");

        ConnectRequestMeta meta = requestMeta(headers);

        assertThat(meta.headerValues("x-tag")).containsExactly("first", "second", "third");
    }

    @Test
    void firstHeaderReturnsFirstValueOfRepeatedHeader() {
        HttpHeaders headers = new DefaultHttpHeaders()
            .add("X-Tag", "first")
            .add("X-Tag", "second");

        ConnectRequestMeta meta = requestMeta(headers);

        assertThat(meta.firstHeader("x-tag")).isEqualTo("first");
    }

    @Test
    void handlesEmptyHeaders() {
        ConnectRequestMeta meta = requestMeta(new DefaultHttpHeaders());

        assertThat(meta.headers()).isEmpty();
    }
}
