package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.suboptimal.connectjava.model.ConnectMethodType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcServiceAdapterTest {

    @Test
    void extractsServiceName() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().serviceName())
            .isEqualTo("connectjava.grpcbridge.test.v1.TestService");
    }

    @Test
    void extractsAllMethods() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods()).hasSize(5);
        assertThat(adapted.definition().methods()).containsKeys(
            "SafeUnary", "Unary", "ServerStream", "ClientStream", "BidiStream");
    }

    @Test
    void mapsUnaryMethodType() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods().get("Unary").type())
            .isEqualTo(ConnectMethodType.UNARY);
    }

    @Test
    void mapsServerStreamingMethodType() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods().get("ServerStream").type())
            .isEqualTo(ConnectMethodType.SERVER_STREAMING);
    }

    @Test
    void mapsClientStreamingMethodType() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods().get("ClientStream").type())
            .isEqualTo(ConnectMethodType.CLIENT_STREAMING);
    }

    @Test
    void mapsBidiStreamingMethodType() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods().get("BidiStream").type())
            .isEqualTo(ConnectMethodType.BIDI_STREAMING);
    }

    @Test
    void extractsRequestAndResponseTypes() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        var unary = adapted.definition().methods().get("Unary");
        assertThat(unary.requestType()).isEqualTo(TestRequest.class);
        assertThat(unary.responseType()).isEqualTo(TestResponse.class);
    }

    @Test
    void mapsSafeUnaryMethodAsIdempotent() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods().get("SafeUnary").idempotent())
            .isTrue();
    }

    @Test
    void mapsNonSafeMethodsAsNonIdempotent() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.definition().methods())
            .containsKeys("Unary", "ServerStream", "ClientStream", "BidiStream");
        assertThat(adapted.definition().methods().get("Unary").idempotent()).isFalse();
        assertThat(adapted.definition().methods().get("ServerStream").idempotent()).isFalse();
        assertThat(adapted.definition().methods().get("ClientStream").idempotent()).isFalse();
        assertThat(adapted.definition().methods().get("BidiStream").idempotent()).isFalse();
    }

    @Test
    void createsMethodDefinitionForEachMethod() {
        var adapted = GrpcServiceAdapter.adapt(new NoOpTestService());
        assertThat(adapted.methods()).hasSize(5);
        assertThat(adapted.methods()).containsKeys(
            "SafeUnary", "Unary", "ServerStream", "ClientStream", "BidiStream");
    }

    static class NoOpTestService extends TestServiceGrpc.TestServiceImplBase {}
}
