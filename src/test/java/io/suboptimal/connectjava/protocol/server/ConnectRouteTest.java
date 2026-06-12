package io.suboptimal.connectjava.protocol.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectRouteTest {

    @ParameterizedTest
    @MethodSource("validPaths")
    void parsesValidPaths(String uri, String service, String method) {
        ConnectRoute route = ConnectRoute.parse(uri);

        assertThat(route).isNotNull();
        assertThat(route.service()).isEqualTo(service);
        assertThat(route.method()).isEqualTo(method);
    }

    @Test
    void stripsQueryString() {
        ConnectRoute route = ConnectRoute.parse("/pkg.Service/Method?x=1");

        assertThat(route).isNotNull();
        assertThat(route.service()).isEqualTo("pkg.Service");
        assertThat(route.method()).isEqualTo("Method");
    }

    @ParameterizedTest
    @MethodSource("malformedPaths")
    void rejectsMalformedPaths(String uri) {
        assertThat(ConnectRoute.parse(uri)).isNull();
    }

    private static Stream<Arguments> validPaths() {
        return Stream.of(
            Arguments.of("/pkg.Service/Method", "pkg.Service", "Method"),
            Arguments.of("/pkg.service/method", "pkg.service", "method")
        );
    }

    private static Stream<String> malformedPaths() {
        return Stream.of(
            "",
            "pkg.Service/Method",
            "/",
            "/pkg.Service",
            "/pkg.Service/",
            "//Method",
            "/pkg.Service//Method",
            "/pkg.Service/Method/"
        );
    }
}
