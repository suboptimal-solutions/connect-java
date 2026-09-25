package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.protocol.server.ConnectProtocol;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolConfig;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolParameters;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcBridgeServerIntegrationTest {
    @Test
    void servesUnaryGrpcServiceThroughConnectProtocol() throws IOException {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.newBuilder().setText("echo: " + request.getText()).build());
                observer.onCompleted();
            }
        };
        ConnectGrpcBridge bridge = ConnectGrpcBridge.builder()
            .addService(service)
            .withDirectExecutor()
            .build();
        ConnectProtocolConfig config = ConnectProtocolConfig.builder(
                bridge.serviceDefinitions(), bridge,
                new ConnectProtocolParameters(1024 * 1024, 1024 * 1024),
                ConnectProtobufCodecs.defaults())
            .build();
        EmbeddedChannel channel = new EmbeddedChannel();
        new ConnectProtocol(config).http1().configure(channel);

        byte[] body = TestRequest.newBuilder().setText("hello").build().toByteArray();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST,
            "/connectjava.grpcbridge.test.v1.TestService/Unary", Unpooled.wrappedBuffer(body));
        request.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/proto")
            .set(HttpHeaderNames.CONTENT_LENGTH, body.length)
            .set("connect-protocol-version", "1");

        try {
            channel.writeInbound(request);
            FullHttpResponse response = channel.readOutbound();
            assertThat(response).isNotNull();
            try {
                assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
                assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE))
                    .isEqualTo("application/proto");
                byte[] responseBody = new byte[response.content().readableBytes()];
                response.content().readBytes(responseBody);
                assertThat(TestResponse.parseFrom(responseBody).getText()).isEqualTo("echo: hello");
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
