package io.suboptimal.connectjava.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufJsonCodec;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class UnaryPostRequestServerHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.UnaryPostFixtureService";

    private static final ConnectMethodDefinition UNARY_METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);

    private static final ConnectServiceDefinition SERVICE_DEF = new ConnectServiceDefinition(
        SERVICE_NAME, List.of(UNARY_METHOD), null);

    private static final UnaryPostRequest REQUEST =
        UnaryPostRequest.newBuilder().setText("hello").build();

    private EmbeddedChannel channel;
    private @Nullable ConnectCallExchange exchange;

    @BeforeEach
    void setUpChannel() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDownChannel() {
        channel.finishAndReleaseAll();
    }

    void setUpHandler() {
        setUpHandler(ConnectServerInterceptorPipeline.EMPTY);
    }

    void setUpHandler(ConnectServerInterceptorPipeline interceptorPipeline) {
        exchange = new ConnectCallExchange(SERVICE_DEF, UNARY_METHOD,
            new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());

        channel.pipeline().addLast(ConnectServerPipeline.UNARY_POST_REQUEST_HANDLER,
            new UnaryPostRequestServerHandler(
                exchange,
                ConnectProtobufCodecs.defaults(),
                ConnectCompressionRegistry.standard(),
                ConnectStringBuilderJsonSerializer.INSTANCE,
                interceptorPipeline));
    }

    @Nested
    class RequestProcessing {
        @BeforeEach
        void setUp() {
            setUpHandler();
        }

        @Test
        void acceptsMinimalValidJsonRequest() throws IOException {
            DefaultFullHttpRequest request = unaryPostRequest("application/json",
                encoded(ConnectProtobufJsonCodec.INSTANCE, REQUEST));

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsMinimalValidProtobufRequest() throws IOException {
            DefaultFullHttpRequest request = unaryPostRequest("application/proto",
                encoded(ConnectProtobufCodec.INSTANCE, REQUEST));

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsContentTypeWithParameters() throws IOException {
            DefaultFullHttpRequest request = unaryPostRequest("application/proto; charset=utf-8",
                encoded(ConnectProtobufCodec.INSTANCE, REQUEST));

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsMalformedCompressedRequestAsInvalidArgument() {
            DefaultFullHttpRequest request = unaryPostRequest("application/proto",
                "not gzip".getBytes(StandardCharsets.UTF_8));
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
                assertThat(json).contains("\"message\":\"Decompression failed:");
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }

        @Test
        void acceptsZeroLengthRequestWithSupportedContentEncodingWithoutDecompression() {
            DefaultFullHttpRequest request = unaryPostRequest("application/proto", new byte[0]);
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(UnaryPostRequest.getDefaultInstance());
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsUnsupportedContentEncodingEvenForZeroLengthRequest() {
            DefaultFullHttpRequest request = unaryPostRequest("application/proto", new byte[0]);
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "br");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertConnectErrorResponse(response, HttpResponseStatus.NOT_IMPLEMENTED,
                    "{\"code\":\"unimplemented\",\"message\":\"Unsupported content-encoding: br; supported: identity,gzip\"}");
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }

        @ParameterizedTest
        @MethodSource("io.suboptimal.connectjava.protocol.server.UnaryPostRequestServerHandlerTest#malformedBodies")
        void rejectsMalformedRequestBodyAsInvalidArgument(String contentType, byte[] body) {
            DefaultFullHttpRequest request = unaryPostRequest(contentType, body);

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
                assertThat(json).contains("\"message\":\"Deserialization failed:");
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Nested
    class InterceptorCallbacks {
        @Test
        void observerCallbacksAreInterleavedCorrectlyWithConnectMessages() throws IOException {
            List<String> events = new ArrayList<>();
            ConnectServerCallObserver observer = new ConnectServerCallObserver() {
                @Override
                public void onRequestPayload(Object payload) {
                    events.add("onRequestPayload");
                }

                @Override
                public void onRequestFinished() {
                    events.add("onRequestFinished");
                }
            };

            setUpHandler(
                new ConnectServerInterceptorPipeline(List.of(ctx -> ConnectServerInterceptor.continueWith(observer))));
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    switch (msg) {
                        case ConnectCallExchange ignore -> events.add("ConnectServerCallExchange");
                        case ConnectPayload ignore -> events.add("ConnectPayload");
                        case ConnectEndOfStream ignore -> events.add("ConnectEndOfStream");
                        case null, default -> {
                        }
                    }
                }
            });

            DefaultFullHttpRequest request = unaryPostRequest( "application/proto",
                encoded(ConnectProtobufCodec.INSTANCE, REQUEST));
            channel.writeInbound(request);

            assertThat(events).containsExactly(
                "ConnectServerCallExchange",
                "onRequestPayload",
                "ConnectPayload",
                "onRequestFinished",
                "ConnectEndOfStream");
        }
    }

    private void assertResponseHandlerInstalled() {
        assertThat(channel.pipeline().get(ConnectServerPipeline.UNARY_RESPONSE_HANDLER))
            .isInstanceOf(UnaryResponseProcessingServerHandler.class);
    }

    private void assertCallExchangeFired() {
        assertThat((ConnectCallExchange) channel.readInbound()).isSameAs(exchange);
    }

    private void assertInboundDataDecodesTo(UnaryPostRequest expected) {
        Object inbound = channel.readInbound();
        assertThat(inbound)
            .isInstanceOfSatisfying(ConnectPayload.class, data ->
                assertThat(data.data()).isEqualTo(expected));
    }

    private static DefaultFullHttpRequest unaryPostRequest(String contentType, byte[] body) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/" + SERVICE_NAME + "/Unary",
            Unpooled.wrappedBuffer(body));
        request.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, contentType)
            .set(HttpHeaderNames.CONTENT_LENGTH, body.length)
            .set("connect-protocol-version", "1");
        return request;
    }

    private static byte[] encoded(ConnectCodec codec, Object value) throws IOException {
        ByteBuf encoded = codec.encode(value, ByteBufAllocator.DEFAULT);
        try {
            byte[] bytes = new byte[encoded.readableBytes()];
            encoded.readBytes(bytes);
            return bytes;
        } finally {
            encoded.release();
        }
    }

    private static void assertConnectErrorResponse(
        FullHttpResponse response, HttpResponseStatus status, String expectedJson) {
        String actualJson = assertConnectErrorResponse(response, status);
        assertThat(actualJson).isEqualTo(expectedJson);
    }

    private static String assertConnectErrorResponse(FullHttpResponse response, HttpResponseStatus status) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE))
            .isEqualTo("application/json");
        String actualJson = response.content().toString(StandardCharsets.UTF_8);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH))
            .isEqualTo(String.valueOf(actualJson.getBytes(StandardCharsets.UTF_8).length));
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        return actualJson;
    }

    static Stream<Arguments> malformedBodies() {
        return Stream.of(
            Arguments.of("application/proto", "not proto".getBytes(StandardCharsets.UTF_8)),
            Arguments.of("application/json", "{".getBytes(StandardCharsets.UTF_8))
        );
    }
}
