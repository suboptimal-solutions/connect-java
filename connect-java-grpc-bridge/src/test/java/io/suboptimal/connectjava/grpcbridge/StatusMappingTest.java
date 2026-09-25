package io.suboptimal.connectjava.grpcbridge;

import io.grpc.Status;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class StatusMappingTest {

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = "OK", mode = EnumSource.Mode.EXCLUDE)
    void mapsAllGrpcCodesToConnect(Status.Code grpcCode) {
        ConnectErrorCode connectCode = StatusMapping.toConnect(grpcCode);
        assertThat(connectCode).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(ConnectErrorCode.class)
    void mapsAllConnectCodesToGrpc(ConnectErrorCode connectCode) {
        Status.Code grpcCode = StatusMapping.toGrpc(connectCode);
        assertThat(grpcCode).isNotNull();
    }

    @ParameterizedTest
    @EnumSource(ConnectErrorCode.class)
    void roundTripsConnectToGrpcAndBack(ConnectErrorCode connectCode) {
        Status.Code grpcCode = StatusMapping.toGrpc(connectCode);
        ConnectErrorCode roundTripped = StatusMapping.toConnect(grpcCode);
        assertThat(roundTripped).isEqualTo(connectCode);
    }

    @Test
    void mapsCancelledToCanceled() {
        assertThat(StatusMapping.toConnect(Status.Code.CANCELLED))
            .isEqualTo(ConnectErrorCode.CANCELED);
    }

    @Test
    void fallsBackToUnknownForOk() {
        assertThat(StatusMapping.toConnect(Status.Code.OK))
            .isEqualTo(ConnectErrorCode.UNKNOWN);
    }
}
