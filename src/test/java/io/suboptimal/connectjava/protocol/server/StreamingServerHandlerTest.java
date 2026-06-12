package io.suboptimal.connectjava.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ConnectEnvelope;
import io.suboptimal.connectjava.protocol.client.ConnectCallTerminatedException;
import io.suboptimal.connectjava.testfixtures.StreamingRequest;
import io.suboptimal.connectjava.testfixtures.StreamingResponse;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamingServerHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.StreamingFixtureService";

    private static final ConnectMethodDefinition SERVER_STREAMING_METHOD = new ConnectMethodDefinition(
        "ServerStreaming",
        ConnectMethodType.SERVER_STREAMING,
        StreamingRequest.class,
        StreamingResponse.class,
        false);

    private static final ConnectMethodDefinition CLIENT_STREAMING_METHOD = new ConnectMethodDefinition(
        "ClientStreaming",
        ConnectMethodType.CLIENT_STREAMING,
        StreamingRequest.class,
        StreamingResponse.class,
        false);

    private static final ConnectServiceDefinition SERVICE_DEF = new ConnectServiceDefinition(
        SERVICE_NAME,
        List.of(SERVER_STREAMING_METHOD, CLIENT_STREAMING_METHOD),
        null);

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

    @Test
    void endOfStreamDoesNotSetConnectionClose() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        HttpResponse response = channel.readOutbound();
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void acceptsContentTypeWithParameters() {
        setUpHandler(SERVER_STREAMING_METHOD);

        DefaultHttpRequest request = request("/ServerStreaming");
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/connect+proto; charset=utf-8");

        channel.writeInbound(request);

        assertCallExchangeFired();
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void lateWriteAfterEndOfStreamIsRejected() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        // drain the response so subsequent assertions are not polluted
        while (channel.readOutbound() != null) {
            // discard
        }

        assertThatThrownBy(() -> channel.writeOutbound(ConnectEndOfStream.INSTANCE))
            .isInstanceOf(ConnectCallTerminatedException.class);
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void lateWriteAfterStreamingErrorIsRejected() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectError.invalidArgument("bad input"));
        while (channel.readOutbound() != null) {
            // discard
        }

        assertThatThrownBy(() -> channel.writeOutbound(ConnectEndOfStream.INSTANCE))
            .isInstanceOf(ConnectCallTerminatedException.class);
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void streamingErrorDoesNotCloseChannel() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectError.invalidArgument("bad input"));

        HttpResponse response = channel.readOutbound();
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void serializesUnsupportedMethodAfterAcceptedRpcAsEndStreamError() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectError.unimplemented("Missing"));

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"unimplemented\"");
        assertThat(json).contains("\"message\":\"Missing\"");
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void interceptorsAttachStreamingHeadersAndSuccessMetadata() {
        ConnectServerCallObserver observer = new ConnectServerCallObserver() {
            @Override
            public void onResponseHeaders(ConnectResponseHeadersBuilder headers) {
                headers.set("x-stream", "started");
            }

            @Override
            public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, @Nullable ConnectError error) {
                assertThat(error).isNull();
                trailers.add("operation-cost", "11");
                trailers.add("operation-cost", "13");
            }
        };
        setUpHandler(SERVER_STREAMING_METHOD, List.of(observer));

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        HttpResponse response = channel.readOutbound();
        assertThat(response.headers().get("x-stream")).isEqualTo("started");
        String json = readEndStreamBody();
        assertThat(json).isEqualTo("{\"metadata\":{\"operation-cost\":[\"11\",\"13\"]}}");
    }

    @Test
    void interceptorsAttachStreamingMetadataOnError() {
        ConnectServerCallObserver observer = new ConnectServerCallObserver() {
            @Override
            public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, @Nullable ConnectError error) {
                assertThat(error.code()).isEqualTo(ConnectErrorCode.INVALID_ARGUMENT);
                trailers.set("error-trailer", "yes");
            }
        };
        setUpHandler(SERVER_STREAMING_METHOD, List.of(observer));

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectError.invalidArgument("bad input"));

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"invalid_argument\"");
        assertThat(json).contains("\"metadata\":{\"error-trailer\":[\"yes\"]}");
    }

    @Test
    void interceptorCanRejectStreamingCallBeforeRpcRequest() {
        ConnectServerInterceptor rejecting = ctx -> ConnectServerInterceptor.reject(ConnectError.invalidArgument("nope"));
        setUpHandler(SERVER_STREAMING_METHOD,
            new ConnectServerInterceptorPipeline(List.of(rejecting)));

        channel.writeInbound(request("/ServerStreaming"));

        assertThat((Object) channel.readInbound()).isNull();
        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"code\":\"invalid_argument\"");
        assertThat(json).contains("\"message\":\"nope\"");
    }

    @Test
    void streamResponseFiresEndOfStreamAfterLastHttpContent() throws IOException {
        setUpHandler(CLIENT_STREAMING_METHOD);

        channel.writeInbound(request("/ClientStreaming"));
        assertCallExchangeFired();

        StreamingRequest firstRequest = StreamingRequest.newBuilder().setText("one").build();
        ByteBuf firstPayload = ConnectProtobufCodec.INSTANCE.encode(firstRequest, channel.alloc());
        ByteBuf firstFrame;
        try {
            firstFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(firstPayload));
        } finally {
            firstPayload.release();
        }
        channel.writeInbound(new DefaultHttpContent(firstFrame));
        assertThat((Object) channel.readInbound()).isInstanceOfSatisfying(ConnectPayload.class,
            data -> assertThat(data.data()).isEqualTo(firstRequest));

        StreamingRequest secondRequest = StreamingRequest.newBuilder().setText("two").build();
        ByteBuf secondPayload = ConnectProtobufCodec.INSTANCE.encode(secondRequest, channel.alloc());
        ByteBuf secondFrame;
        try {
            secondFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(secondPayload));
        } finally {
            secondPayload.release();
        }
        channel.writeInbound(new DefaultHttpContent(secondFrame));
        assertThat((Object) channel.readInbound()).isInstanceOfSatisfying(ConnectPayload.class,
            data -> assertThat(data.data()).isEqualTo(secondRequest));

        StreamingRequest finalRequest = StreamingRequest.newBuilder().setText("three").build();
        ByteBuf finalPayload = ConnectProtobufCodec.INSTANCE.encode(finalRequest, channel.alloc());
        ByteBuf finalFrame;
        try {
            finalFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(finalPayload));
        } finally {
            finalPayload.release();
        }
        channel.writeInbound(new DefaultLastHttpContent(finalFrame));
        assertThat((Object) channel.readInbound()).isInstanceOfSatisfying(ConnectPayload.class,
            data -> assertThat(data.data()).isEqualTo(finalRequest));
        assertThat((Object) channel.readInbound()).isSameAs(ConnectEndOfStream.INSTANCE);
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void ignoresLastHttpContentAfterContentProcessingError() throws IOException {
        setUpHandler(CLIENT_STREAMING_METHOD);

        channel.writeInbound(request("/ClientStreaming"));
        assertCallExchangeFired();

        ByteBuf invalidPayloadFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, new byte[] {0});
        channel.writeInbound(new DefaultHttpContent(invalidPayloadFrame));

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"invalid_argument\"");
        assertThat((Object) channel.readInbound()).isNull();

        StreamingRequest ignoredRequest = StreamingRequest.newBuilder().setText("ignored").build();
        ByteBuf ignoredPayload = ConnectProtobufCodec.INSTANCE.encode(ignoredRequest, channel.alloc());
        ByteBuf ignoredFrame;
        try {
            ignoredFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(ignoredPayload));
        } finally {
            ignoredPayload.release();
        }
        channel.writeInbound(new DefaultLastHttpContent(ignoredFrame));

        assertThat((Object) channel.readInbound()).isNull();
        assertThat((Object) channel.readOutbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void cancelPathNotifiesObserverOnceWhenChannelClosedMidStream() {
        AtomicInteger callCount = new AtomicInteger();
        List<ConnectError> errors = new ArrayList<>();
        ConnectServerCallObserver observer = new ConnectServerCallObserver() {
            @Override
            public void onCallComplete(@Nullable ConnectError error) {
                callCount.incrementAndGet();
                errors.add(error);
            }
        };

        setUpHandler(SERVER_STREAMING_METHOD, List.of(observer));

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.close();

        assertThat(callCount.get()).isEqualTo(1);
        assertThat(errors.getFirst()).isNotNull();
        assertThat(errors.getFirst().code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    @Test
    void cancelPathDoesNotFireAfterNormalCompletion() {
        AtomicInteger callCount = new AtomicInteger();
        ConnectServerCallObserver observer = new ConnectServerCallObserver() {
            @Override
            public void onCallComplete(@Nullable ConnectError error) {
                callCount.incrementAndGet();
            }
        };

        setUpHandler(SERVER_STREAMING_METHOD, List.of(observer));

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        Object msg;
        while ((msg = channel.readOutbound()) != null) {
            io.netty.util.ReferenceCountUtil.release(msg);
        }

        channel.close();

        assertThat(callCount.get()).isEqualTo(1);
    }

    @Test
    void versionCheckFailureBeforeInterceptorDoesNotInvokeInterceptor() {
        boolean[] interceptorCalled = {false};
        ConnectServerInterceptor interceptor = exchange -> {
            interceptorCalled[0] = true;
            return ConnectServerInterceptor.continueCall();
        };

        setUpHandler(SERVER_STREAMING_METHOD,
            new ConnectServerInterceptorPipeline(List.of(interceptor)));

        DefaultHttpRequest badRequest = request("/ServerStreaming");
        badRequest.headers().set("connect-protocol-version", "999");

        channel.writeInbound(badRequest);

        assertThat((Object) channel.readInbound()).isNull();
        assertThat(interceptorCalled[0]).isFalse();
        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"invalid_argument\"");
    }

    @Test
    void streamingRejectsCompressedFrameWithoutNegotiatedEncoding() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        ByteBuf frame = ConnectEnvelope.encode(channel.alloc(),
            ConnectEnvelope.FLAG_COMPRESSED, new byte[] {0x01, 0x02, 0x03});
        channel.writeInbound(new DefaultHttpContent(frame));

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"internal\"");
        assertThat((Object) channel.readInbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void serverStreamingRejectsZeroRequestPayloads() {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"unimplemented\"");

        assertThat((Object) channel.readInbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void serverStreamingRejectsTwoRequestPayloads() throws IOException {
        setUpHandler(SERVER_STREAMING_METHOD);

        channel.writeInbound(request("/ServerStreaming"));
        assertCallExchangeFired();

        StreamingRequest firstRequest = StreamingRequest.newBuilder().setText("one").build();
        ByteBuf firstPayload = ConnectProtobufCodec.INSTANCE.encode(firstRequest, channel.alloc());
        ByteBuf firstFrame;
        try {
            firstFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(firstPayload));
        } finally {
            firstPayload.release();
        }
        channel.writeInbound(new DefaultHttpContent(firstFrame));
        assertThat((Object) channel.readInbound()).isInstanceOfSatisfying(ConnectPayload.class,
            data -> assertThat(data.data()).isEqualTo(firstRequest));

        StreamingRequest secondRequest = StreamingRequest.newBuilder().setText("two").build();
        ByteBuf secondPayload = ConnectProtobufCodec.INSTANCE.encode(secondRequest, channel.alloc());
        ByteBuf secondFrame;
        try {
            secondFrame = ConnectEnvelope.encode(channel.alloc(), (byte) 0, ByteBufUtil.getBytes(secondPayload));
        } finally {
            secondPayload.release();
        }
        channel.writeInbound(new DefaultHttpContent(secondFrame));

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"unimplemented\"");

        assertThat((Object) channel.readInbound()).isNull();
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void clientStreamingRejectsZeroResponsePayloads() {
        setUpHandler(CLIENT_STREAMING_METHOD);

        channel.writeInbound(request("/ClientStreaming"));
        assertCallExchangeFired();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        String json = assertStreamingErrorResponse();
        assertThat(json).contains("\"error\":{\"code\":\"internal\"");
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void clientStreamingRejectsTwoResponsePayloads() {
        setUpHandler(CLIENT_STREAMING_METHOD);

        channel.writeInbound(request("/ClientStreaming"));
        assertCallExchangeFired();

        StreamingResponse first = StreamingResponse.newBuilder().setText("one").build();
        StreamingResponse second = StreamingResponse.newBuilder().setText("two").build();

        channel.writeOutbound(new ConnectPayload(first));
        channel.writeOutbound(new ConnectPayload(second));

        HttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);

        HttpContent firstChunk = channel.readOutbound();
        firstChunk.release();

        String json = readEndStreamBody();
        assertThat(json).contains("\"error\":{\"code\":\"internal\"");
        assertThat(channel.isOpen()).isTrue();
    }

    private void assertCallExchangeFired() {
        assertThat((ConnectCallExchange) channel.readInbound()).isSameAs(exchange);
    }

    void setUpHandler(ConnectMethodDefinition method) {
        setUpHandler(method, ConnectServerInterceptorPipeline.EMPTY);
    }

    void setUpHandler(ConnectMethodDefinition method, List<ConnectServerCallObserver> observers) {
        List<ConnectServerInterceptor> interceptors = observers.stream()
            .<ConnectServerInterceptor>map(observer -> ctx -> ConnectServerInterceptor.continueWith(observer))
            .toList();
        setUpHandler(method, new ConnectServerInterceptorPipeline(interceptors));
    }

    void setUpHandler(ConnectMethodDefinition method, ConnectServerInterceptorPipeline interceptorPipeline) {
        exchange = new ConnectCallExchange(SERVICE_DEF, method,
            new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());

        channel.pipeline().addLast(new StreamingServerHandler(
            exchange,
            1024, ConnectProtobufCodecs.defaults(), ConnectCompressionRegistry.standard(),
            ConnectStringBuilderJsonSerializer.INSTANCE, interceptorPipeline));
    }

    private static DefaultHttpRequest request(String methodUri) {
        DefaultHttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/" + SERVICE_NAME + methodUri);
        request.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/connect+proto")
            .set("connect-protocol-version", "1");
        return request;
    }

    private String assertStreamingErrorResponse() {
        HttpResponse response = channel.readOutbound();
        assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();

        return readEndStreamBody();
    }

    private String readEndStreamBody() {
        HttpContent content = channel.readOutbound();
        String json;
        try {
            ByteBuf envelope = content.content();
            assertThat(envelope.readByte()).isEqualTo((byte) 0x02);
            int length = envelope.readInt();
            assertThat(envelope.readableBytes()).isEqualTo(length);
            json = envelope.toString(envelope.readerIndex(), length, StandardCharsets.UTF_8);
        } finally {
            content.release();
        }

        LastHttpContent last = channel.readOutbound();
        try {
            assertThat(last).isSameAs(LastHttpContent.EMPTY_LAST_CONTENT);
        } finally {
            last.release();
        }
        return json;
    }
}
