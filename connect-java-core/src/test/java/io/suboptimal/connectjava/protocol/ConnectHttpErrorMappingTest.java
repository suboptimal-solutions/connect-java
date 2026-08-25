package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectErrorCode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectHttpErrorMappingTest {
    @ParameterizedTest
    @MethodSource("serverMappings")
    void mapsServerErrorCodeToStatus(ConnectErrorCode code, int status) {
        assertThat(ConnectHttpErrorMapping.statusFor(code)).isEqualTo(status);
    }

    @ParameterizedTest
    @MethodSource("clientMappings")
    void mapsHttpStatusToClientFallbackCode(int status, ConnectErrorCode code) {
        assertThat(ConnectHttpErrorMapping.codeForStatus(status)).isEqualTo(code);
    }

    private static Stream<Arguments> serverMappings() {
        return Stream.of(
            Arguments.of(ConnectErrorCode.CANCELED, 499),
            Arguments.of(ConnectErrorCode.UNKNOWN, 500),
            Arguments.of(ConnectErrorCode.INVALID_ARGUMENT, 400),
            Arguments.of(ConnectErrorCode.DEADLINE_EXCEEDED, 504),
            Arguments.of(ConnectErrorCode.NOT_FOUND, 404),
            Arguments.of(ConnectErrorCode.ALREADY_EXISTS, 409),
            Arguments.of(ConnectErrorCode.PERMISSION_DENIED, 403),
            Arguments.of(ConnectErrorCode.RESOURCE_EXHAUSTED, 429),
            Arguments.of(ConnectErrorCode.FAILED_PRECONDITION, 400),
            Arguments.of(ConnectErrorCode.ABORTED, 409),
            Arguments.of(ConnectErrorCode.OUT_OF_RANGE, 400),
            Arguments.of(ConnectErrorCode.UNIMPLEMENTED, 501),
            Arguments.of(ConnectErrorCode.INTERNAL, 500),
            Arguments.of(ConnectErrorCode.UNAVAILABLE, 503),
            Arguments.of(ConnectErrorCode.DATA_LOSS, 500),
            Arguments.of(ConnectErrorCode.UNAUTHENTICATED, 401));
    }

    private static Stream<Arguments> clientMappings() {
        return Stream.of(
            Arguments.of(400, ConnectErrorCode.INVALID_ARGUMENT),
            Arguments.of(401, ConnectErrorCode.UNAUTHENTICATED),
            Arguments.of(403, ConnectErrorCode.PERMISSION_DENIED),
            Arguments.of(404, ConnectErrorCode.UNIMPLEMENTED),
            Arguments.of(408, ConnectErrorCode.DEADLINE_EXCEEDED),
            Arguments.of(412, ConnectErrorCode.FAILED_PRECONDITION),
            Arguments.of(413, ConnectErrorCode.RESOURCE_EXHAUSTED),
            Arguments.of(431, ConnectErrorCode.RESOURCE_EXHAUSTED),
            Arguments.of(429, ConnectErrorCode.UNAVAILABLE),
            Arguments.of(502, ConnectErrorCode.UNAVAILABLE),
            Arguments.of(503, ConnectErrorCode.UNAVAILABLE),
            Arguments.of(504, ConnectErrorCode.UNAVAILABLE),
            Arguments.of(200, ConnectErrorCode.UNKNOWN),
            Arguments.of(499, ConnectErrorCode.UNKNOWN),
            Arguments.of(500, ConnectErrorCode.UNKNOWN));
    }
}
