package io.suboptimal.connectjava.api;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectErrorTest {
    @Test
    void defaultsToRpcOrigin() {
        assertThat(new ConnectError(ConnectErrorCode.INTERNAL, "x").origin())
            .isEqualTo(ConnectErrorOrigin.RPC);
        assertThat(new ConnectError(ConnectErrorCode.INTERNAL, "x", List.of()).origin())
            .isEqualTo(ConnectErrorOrigin.RPC);
        assertThat(ConnectError.notFound("x").origin()).isEqualTo(ConnectErrorOrigin.RPC);
    }

    @Test
    void withOriginReplacesOnlyOrigin() {
        ConnectError base = new ConnectError(ConnectErrorCode.NOT_FOUND, "gone");
        ConnectError transport = base.withOrigin(ConnectErrorOrigin.TRANSPORT);

        assertThat(transport.origin()).isEqualTo(ConnectErrorOrigin.TRANSPORT);
        assertThat(transport.code()).isEqualTo(base.code());
        assertThat(transport.message()).isEqualTo(base.message());
        assertThat(transport.details()).isEqualTo(base.details());
    }
}
