package io.suboptimal.connectjava.protocol;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufJsonCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectGzipCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnaryResponseProcessingHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.UnaryResponseFixtureService";

    private static final ConnectMethodDefinition UNARY_METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);

    private static final ConnectServiceDefinition SERVICE_DEF = new ConnectServiceDefinition(
        SERVICE_NAME, List.of(UNARY_METHOD), null);

    private static final UnaryPostResponse RESPONSE = UnaryPostResponse.newBuilder().setText("ok").build();
    private static final ConnectCompression GZIP = ConnectGzipCompression.INSTANCE;

    private EmbeddedChannel channel;

    @BeforeEach
    void setUpChannel() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDownChannel() {
        channel.finishAndReleaseAll();
    }

    void setUpHandler(ConnectCodec codec, ConnectCompression compression, boolean varyAcceptEncoding) {
        setUpHandler(codec, compression, varyAcceptEncoding, ConnectCallObserver.NOOP);
    }

    void setUpHandler(ConnectCodec codec, ConnectCompression compression, boolean varyAcceptEncoding,
        ConnectCallObserver observer)
    {
        channel.pipeline().addLast(ConnectPipeline.UNARY_RESPONSE_HANDLER,
            new UnaryResponseProcessingHandler(
                createExchange(), codec, compression, varyAcceptEncoding,
                observer, ConnectStringBuilderJsonSerializer.INSTANCE));
    }

    void setUpHandler(ConnectCodec codec, ConnectCompression compression, boolean varyAcceptEncoding,
        List<ConnectCallObserver> observers)
    {
        setUpHandler(codec, compression, varyAcceptEncoding, composite(observers));
    }

    private static ConnectCallExchange createExchange() {
        return new ConnectCallExchange(SERVICE_DEF, UNARY_METHOD,
            new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());
    }

    private static ConnectCallObserver composite(List<ConnectCallObserver> observers) {
        return new ConnectInterceptorPipeline(observers.stream()
            .<ConnectInterceptor>map(observer -> ctx -> ConnectInterceptor.continueWith(observer))
            .toList())
            .interceptCall(createExchange())
            .observer();
    }

    @Test
    void buffersThenWritesOnEndOfStream() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        assertThat((Object) channel.readOutbound()).isNull();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
        } finally {
            response.release();
        }
    }

    @Test
    void noSecondResponseAfterEndOfStream() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        channel.readOutbound(); // consume the response

        // After termination, subsequent writes are rejected with ConnectCallTerminatedException
        assertThatThrownBy(() -> channel.writeOutbound(ConnectEndOfStream.INSTANCE)).isInstanceOf(ConnectCallTerminatedException.class);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void setsProtoContentType() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/proto");
        } finally {
            response.release();
        }
    }

    @Test
    void setsJsonContentType() {
        setUpHandler(ConnectProtobufJsonCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/json");
        } finally {
            response.release();
        }
    }

    @Test
    void setsContentLengthMatchingBody() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            int bodyLength = response.content().readableBytes();
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo(String.valueOf(bodyLength));
        } finally {
            response.release();
        }
    }

    @Test
    void omitsContentEncodingWhenIdentity() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().contains(HttpHeaderNames.CONTENT_ENCODING)).isFalse();
        } finally {
            response.release();
        }
    }

    @Test
    void setsContentEncodingWhenGzip() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, GZIP, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().get(HttpHeaderNames.CONTENT_ENCODING)).isEqualTo("gzip");
        } finally {
            response.release();
        }
    }

    @Test
    void addsVaryAcceptEncodingWhenFlagIsTrue() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, true);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().get("Vary")).contains("Accept-Encoding");
        } finally {
            response.release();
        }
    }

    @Test
    void omitsVaryAcceptEncodingWhenFlagIsFalse() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().contains("Vary")).isFalse();
        } finally {
            response.release();
        }
    }

    @Test
    void errorResponseOmitsVaryAcceptEncoding() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, true);

        channel.writeOutbound(ConnectError.invalidArgument("bad input"));
        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().contains("Vary")).isFalse();
        } finally {
            response.release();
        }
    }

    @Test
    void interceptorsMutateUnaryHeadersAndTrailersInExpectedOrder() {
        List<String> events = new ArrayList<>();
        ConnectCallObserver first = new ConnectCallObserver() {
            @Override
            public void onResponseHeaders(ConnectResponseHeadersBuilder headers) {
                events.add("first:headers");
                headers.add("x-order", "first");
            }

            @Override
            public void onResponsePayload(Object payload) {
                events.add("first:payload");
            }

            @Override
            public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, ConnectError error) {
                events.add("first:trailers");
                trailers.add("operation-cost", "7");
            }

            @Override
            public void onCallComplete(ConnectError error) {
                events.add("first:complete");
            }
        };
        ConnectCallObserver second = new ConnectCallObserver() {
            @Override
            public void onResponseHeaders(ConnectResponseHeadersBuilder headers) {
                events.add("second:headers");
                headers.add("x-order", "second");
            }

            @Override
            public void onResponsePayload(Object payload) {
                events.add("second:payload");
            }

            @Override
            public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, ConnectError error) {
                events.add("second:trailers");
                trailers.add("trace-id", "abc");
            }

            @Override
            public void onCallComplete(ConnectError error) {
                events.add("second:complete");
            }
        };

        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false, List.of(first, second));

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpResponse response = channel.readOutbound();
        try {
            assertThat(response.headers().getAll("x-order")).containsExactly("second", "first");
            assertThat(response.headers().get("Trailer-Operation-Cost")).isEqualTo("7");
            assertThat(response.headers().get("Trailer-Trace-Id")).isEqualTo("abc");
        } finally {
            response.release();
        }
        assertThat(events).containsExactly(
            "second:headers",
            "first:headers",
            "first:payload",
            "second:payload",
            "second:trailers",
            "first:trailers",
            "second:complete",
            "first:complete");
    }

    @Test
    void interceptorsCanAttachUnaryHeadersAndTrailersOnRpcError() {
        ConnectCallObserver observer = new ConnectCallObserver() {
            @Override
            public void onResponseHeaders(ConnectResponseHeadersBuilder headers) {
                headers.set("error-header", "from-interceptor");
            }

            @Override
            public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, ConnectError error) {
                assertThat(error.code()).isEqualTo(ConnectErrorCode.INVALID_ARGUMENT);
                trailers.set("error-detail", "from-interceptor");
            }
        };

        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false, List.of(observer));

        channel.writeOutbound(ConnectError.invalidArgument("bad"));
        FullHttpResponse response = channel.readOutbound();
        try {
            assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
            assertThat(response.headers().get("Error-Header")).isEqualTo("from-interceptor");
            assertThat(response.headers().get("Trailer-Error-Detail")).isEqualTo("from-interceptor");
        } finally {
            response.release();
        }
    }

    @Test
    void serializesDeserializationErrorAsInvalidArgument() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(ConnectError.invalidArgument("bad input"));
        FullHttpResponse response = channel.readOutbound();
        try {
            assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST, "{\"code\":\"invalid_argument\",\"message\":\"bad input\"}");
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("rpcErrorMappings")
    void serializesRpcErrors(ConnectError error, HttpResponseStatus expectedStatus, String expectedJson) {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(error);
        FullHttpResponse response = channel.readOutbound();
        try {
            assertConnectErrorResponse(response, expectedStatus, expectedJson);
        } finally {
            response.release();
        }
    }

    @Test
    void preservesErrorMessageExactly() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        String message = "Detailed error: something went wrong";
        channel.writeOutbound(ConnectError.invalidArgument(message));
        FullHttpResponse response = channel.readOutbound();
        try {
            String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
            assertThat(json).isEqualTo("{\"code\":\"invalid_argument\",\"message\":\"Detailed error: something went wrong\"}");
        } finally {
            response.release();
        }
    }

    @Test
    void escapesSpecialCharsInErrorMessage() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        String message = "err \"quoted\" \\ back\nnew\rret\ttab";
        channel.writeOutbound(ConnectError.invalidArgument(message));
        FullHttpResponse response = channel.readOutbound();
        try {
            String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
            assertThat(json).contains("\"code\":\"invalid_argument\"");
            assertThat(json).contains("err \\\"quoted\\\" \\\\ back\\nnew\\rret\\ttab");
        } finally {
            response.release();
        }
    }

    @Test
    void invalidOutboundPayloadWritesInternalError() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload("not a response"));
        FullHttpResponse response = channel.readOutbound();
        try {
            String json = assertConnectErrorResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            assertThat(json).contains("\"code\":\"internal\"");
            assertThat(json).contains("Invalid service response");
        } finally {
            response.release();
        }
    }

    @Test
    void endOfStreamAfterInvalidPayloadProducesNoSecondResponse() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload("not a response"));
        channel.readOutbound(); // consume the error response

        assertThatThrownBy(() -> channel.writeOutbound(ConnectEndOfStream.INSTANCE)).isInstanceOf(ConnectCallTerminatedException.class);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void endOfStreamWithoutDataWritesInternalError() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpResponse response = channel.readOutbound();
        try {
            String json = assertConnectErrorResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            assertThat(json).contains("\"code\":\"internal\"");
            assertThat(json).contains("Unary RPC completed without a response");
        } finally {
            response.release();
        }
    }

    @Test
    void secondRpcDataAfterFirstWritesInternalError() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        assertThat((Object) channel.readOutbound()).isNull();

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        FullHttpResponse response = channel.readOutbound();
        try {
            String json = assertConnectErrorResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            assertThat(json).contains("\"code\":\"internal\"");
            assertThat(json).contains("more than one response payload");
        } finally {
            response.release();
        }
    }

    @Test
    void endOfStreamAfterSecondPayloadErrorProducesNoResponse() {
        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.readOutbound(); // consume the error

        assertThatThrownBy(() -> channel.writeOutbound(ConnectEndOfStream.INSTANCE)).isInstanceOf(ConnectCallTerminatedException.class);
        assertThat((Object) channel.readOutbound()).isNull();
    }

    @Test
    void cancelPathNotifiesObserverOnceWhenChannelClosedBeforeResponse() {
        List<ConnectError> completions = new ArrayList<>();
        ConnectCallObserver observer = new ConnectCallObserver() {
            @Override
            public void onCallComplete(ConnectError error) {
                completions.add(error);
            }
        };

        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false, observer);
        channel.finishAndReleaseAll();

        assertThat(completions).hasSize(1);
        assertThat(completions.getFirst()).isNotNull();
        assertThat(completions.getFirst().code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    @Test
    void onCallCompleteFiresExactlyOnceAfterSuccessfulResponse() {
        int[] callCount = {0};
        ConnectCallObserver observer = new ConnectCallObserver() {
            @Override
            public void onCallComplete(ConnectError error) {
                callCount[0]++;
            }
        };

        setUpHandler(ConnectProtobufCodec.INSTANCE, ConnectIdentityCompression.INSTANCE, false, observer);

        channel.writeOutbound(new ConnectPayload(RESPONSE));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        channel.readOutbound();
        channel.finishAndReleaseAll();

        assertThat(callCount[0]).isEqualTo(1);
    }

    private static void assertConnectErrorResponse(FullHttpResponse response, HttpResponseStatus status,
        String expectedJson)
    {
        String actualJson = assertConnectErrorResponse(response, status);
        assertThat(actualJson).isEqualTo(expectedJson);
    }

    private static String assertConnectErrorResponse(FullHttpResponse response, HttpResponseStatus status) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/json");
        String actualJson = response.content().toString(StandardCharsets.UTF_8);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo(String.valueOf(actualJson.getBytes(StandardCharsets.UTF_8).length));
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        return actualJson;
    }

    static Stream<Arguments> rpcErrorMappings() {
        return Stream.of(
            Arguments.of(ConnectError.unimplemented(""), HttpResponseStatus.NOT_IMPLEMENTED, "{\"code\":\"unimplemented\"}"),
            Arguments.of(ConnectError.unimplemented("Unknown"), HttpResponseStatus.NOT_IMPLEMENTED, "{\"code\":\"unimplemented\",\"message\":\"Unknown\"}"),
            Arguments.of(ConnectError.invalidArgument("bad input"), HttpResponseStatus.BAD_REQUEST, "{\"code\":\"invalid_argument\",\"message\":\"bad input\"}"),
            Arguments.of(ConnectError.invalidArgument("invalid"), HttpResponseStatus.BAD_REQUEST, "{\"code\":\"invalid_argument\",\"message\":\"invalid\"}"),
            Arguments.of(ConnectError.resourceExhausted("too big"), HttpResponseStatus.TOO_MANY_REQUESTS, "{\"code\":\"resource_exhausted\",\"message\":\"too big\"}"),
            Arguments.of(ConnectError.permissionDenied("denied"), HttpResponseStatus.FORBIDDEN, "{\"code\":\"permission_denied\",\"message\":\"denied\"}"),
            Arguments.of(ConnectError.unknown("oops"), HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"code\":\"unknown\",\"message\":\"oops\"}"),
            Arguments.of(ConnectError.internal("bug"), HttpResponseStatus.INTERNAL_SERVER_ERROR, "{\"code\":\"internal\",\"message\":\"bug\"}"));
    }
}
