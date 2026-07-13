package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectServerProtocolVersionTest {

    @Test
    void acceptsVersion1() {
        HttpHeaders headers = new DefaultHttpHeaders()
            .set("connect-protocol-version", "1");

        assertThat(ConnectProtocolVersion.validate(headers)).isNull();
    }

    @Test
    void acceptsMissingHeader() {
        assertThat(ConnectProtocolVersion.validate(new DefaultHttpHeaders())).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2", "0", "", "1.0"})
    void rejectsOtherVersions(String version) {
        HttpHeaders headers = new DefaultHttpHeaders()
            .set("connect-protocol-version", version);

        assertThat(ConnectProtocolVersion.validate(headers))
            .isEqualTo("Unsupported Connect protocol version: " + version);
    }
}
