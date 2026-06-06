package io.suboptimal.connectjava.protocol;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingHandlerTest {
    private static final ConnectProtocolParameters PARAMETERS =
        new ConnectProtocolParameters(1024, 64);

    private EmbeddedChannel channel;

    @BeforeEach
    void setUpChannel() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDownChannel() {
        channel.finishAndReleaseAll();
    }

    void setUpHandler() {
        setUpHandler(ConnectTransport.HTTP_1_1);
    }

    void setUpHandler(ConnectTransport transport) {
        channel.pipeline().addLast(ConnectPipeline.ROUTING_HANDLER, newRoutingHandler(transport));
    }

    @Test
    void unsupportedContentTypeReturns415AndReleasesRequest() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, methodUri("Unary"));
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();

        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).unsupportedMediaTypeError();
        } finally {
            response.release();
        }

        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void missingContentTypeReturns415() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, methodUri("Unary"));
        // Intentionally no Content-Type header.

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).unsupportedMediaTypeError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unknownServiceReturnsPlain404BeforeContentTypeSelection() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, "/pkg.Missing/Unary");
        // Intentionally no Content-Type header; route miss wins over protocol selection.

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unknownMethodReturnsPlain404BeforeStreamingSelection() {
        setUpHandler();
        DefaultHttpRequest request = request("application/connect+proto", "/pkg.Service/Missing");

        channel.writeInbound(request);

        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unknownMethodReturnsPlain404BeforeUnarySelection() {
        setUpHandler();
        DefaultFullHttpRequest request = fullRequest("application/proto", "/pkg.Service/Missing");

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unknownGetRouteReturnsPlain404BeforeQueryValidation() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/pkg.Missing/Unary?encoding=bogus");

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void routeResolutionIsCaseSensitive() {
        setUpHandler();
        DefaultFullHttpRequest request = fullRequest("application/proto", "/pkg.service/unary");

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("malformedProcedurePaths")
    void malformedProcedurePathReturnsPlain404(String uri) {
        setUpHandler();
        DefaultFullHttpRequest request = fullRequest("application/proto", uri);

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).notFoundError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void knownRouteWithUnsupportedMethodReturns405BeforeContentTypeSelection() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, methodUri("Unary"));
        // Intentionally no Content-Type header; method gate wins for known procedures.

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).methodNotAllowedAllowing("GET, POST");
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unaryContentTypeWithParametersStillInstallsUnaryHandler() {
        setUpHandler();

        // Content-Type with parameters must still resolve to the unary path
        // via Netty's parsed request MIME type.
        channel.writeInbound(fullRequest("application/proto; charset=utf-8"));

        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_POST_REQUEST_HANDLER))
            .isInstanceOf(UnaryPostRequestHandler.class);
    }

    @Test
    void unsupportedApplicationContentTypeIsHandledByUnaryHandler() {
        setUpHandler();
        DefaultHttpRequest request = request("application/foo");

        channel.writeInbound(request);

        assertThat(channel.pipeline().get(ConnectPipeline.ROUTING_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.AGGREGATOR_HANDLER))
            .isInstanceOf(HttpObjectAggregator.class);
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_POST_REQUEST_HANDLER))
            .isInstanceOf(UnaryPostRequestHandler.class);

        channel.writeInbound(new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER));

        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).unsupportedMediaTypeError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void streamingContentTypeInstallsNamedStreamingHandler() {
        setUpHandler();

        channel.writeInbound(request("application/connect+proto", methodUri("ServerStream")));

        assertThat(channel.pipeline().get(ConnectPipeline.ROUTING_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.STREAMING_HANDLER))
            .isInstanceOf(StreamingHandler.class);

        Object inbound = channel.readInbound();
        assertThat(inbound)
            .isInstanceOfSatisfying(ConnectCallExchange.class, exchange -> {
                assertThat(exchange.serviceDefinition().serviceName()).isEqualTo("pkg.Service");
                assertThat(exchange.methodDefinition().methodName()).isEqualTo("ServerStream");
            });
    }

    @Test
    void unaryContentTypeInstallsNamedAggregatorAndUnaryHandlerInOrder() {
        setUpHandler();

        channel.writeInbound(fullRequest("application/proto"));

        assertThat(channel.pipeline().get(ConnectPipeline.ROUTING_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.AGGREGATOR_HANDLER))
            .isInstanceOf(HttpObjectAggregator.class);
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_POST_REQUEST_HANDLER))
            .isInstanceOf(UnaryPostRequestHandler.class);
        assertThat(channel.pipeline().names())
            .containsSequence(ConnectPipeline.AGGREGATOR_HANDLER, ConnectPipeline.UNARY_POST_REQUEST_HANDLER);
    }

    @Test
    void getInstallsNamedAggregatorAndUnaryGetHandler() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, methodUri("Unary") + "?encoding=json&message=%7B%7D");

        channel.writeInbound(request);

        assertThat(channel.pipeline().get(ConnectPipeline.ROUTING_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.AGGREGATOR_HANDLER))
            .isInstanceOf(HttpObjectAggregator.class);
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_GET_REQUEST_HANDLER))
            .isInstanceOf(UnaryGetRequestHandler.class);

        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).methodNotAllowedAllowing("POST");
        } finally {
            response.release();
        }
    }

    @Test
    void bidiOverHttp1ReturnsHttp505WithConnectionCloseAndEmptyBody() {
        setUpHandler();
        DefaultFullHttpRequest request = fullRequest("application/connect+proto", methodUri("BidiStream"));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).httpVersionNotSupportedWithConnectionClose();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void bidiOverHttp2InstallsStreamingHandler() {
        setUpHandler(ConnectTransport.HTTP_2);
        DefaultHttpRequest request = request("application/connect+proto", methodUri("BidiStream"));

        channel.writeInbound(request);

        assertThat(channel.pipeline().get(ConnectPipeline.ROUTING_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.STREAMING_HANDLER))
            .isInstanceOf(StreamingHandler.class);
        Object outbound = channel.readOutbound();
        assertThat(outbound).isNull();

        Object inbound = channel.readInbound();
        assertThat(inbound)
            .isInstanceOfSatisfying(ConnectCallExchange.class, exchange -> {
                assertThat(exchange.serviceDefinition().serviceName()).isEqualTo("pkg.Service");
                assertThat(exchange.methodDefinition().methodName()).isEqualTo("BidiStream");
            });
    }

    @ParameterizedTest
    @MethodSource("streamingMethodsWithUnaryContentTypes")
    void streamingMethodWithUnaryContentTypeReturns415(ContentTypeMismatch mismatch) {
        setUpHandler(mismatch.transport());
        DefaultFullHttpRequest request = fullRequest(mismatch.contentType(), methodUri(mismatch.methodName()));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).unsupportedMediaTypeError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("streamingContentTypes")
    void unaryMethodWithStreamingContentTypeReturns415(String contentType) {
        setUpHandler();
        DefaultFullHttpRequest request = fullRequest(contentType, methodUri("Unary"));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertThat(channel.pipeline().get(ConnectPipeline.STREAMING_HANDLER)).isNull();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).unsupportedMediaTypeError();
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("streamingMethodsForGet")
    void getOnStreamingMethodReturns405WithAllowPost(MethodGate gate) {
        setUpHandler(gate.transport());
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, methodUri(gate.methodName()));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        assertNoConnectHandlerInstalled();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).methodNotAllowedAllowing("POST");
        } finally {
            response.release();
        }
        assertThat(channel.isOpen()).isTrue();
    }

    @Test
    void unsupportedMethodOnStreamingProcedureReturns405WithAllowPost() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, methodUri("ServerStream"));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).methodNotAllowedAllowing("POST");
        } finally {
            response.release();
        }
    }

    @Test
    void unsupportedMethodOnUnaryProcedureReturns405WithAllowPostGet() {
        setUpHandler();
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.PUT, methodUri("Unary"));

        channel.writeInbound(request);

        assertThat(request.refCnt()).isZero();
        FullHttpResponse response = channel.readOutbound();
        try {
            HttpAssertions.assertThat(response).methodNotAllowedAllowing("GET, POST");
        } finally {
            response.release();
        }
    }

    private void assertNoConnectHandlerInstalled() {
        assertThat(channel.pipeline().get(ConnectPipeline.AGGREGATOR_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_GET_REQUEST_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_POST_REQUEST_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_RESPONSE_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectPipeline.STREAMING_HANDLER)).isNull();
    }

    private static RoutingHandler newRoutingHandler(ConnectTransport transport) {
        return new RoutingHandler(transport, ConnectProtocolConfig.builder(
                Map.of("pkg.Service", SERVICE_DEFINITION),
                ChannelInboundHandlerAdapter::new,
                PARAMETERS,
                ConnectProtobufCodecs.defaults())
            .build());
    }

    private static DefaultHttpRequest request(String contentType) {
        return request(contentType, methodUri("Unary"));
    }

    private static DefaultHttpRequest request(String contentType, String uri) {
        DefaultHttpRequest request = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        setConnectHeaders(request, contentType);
        return request;
    }

    private static DefaultFullHttpRequest fullRequest(String contentType) {
        return fullRequest(contentType, methodUri("Unary"));
    }

    private static DefaultFullHttpRequest fullRequest(String contentType, String uri) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        setConnectHeaders(request, contentType);
        return request;
    }

    private static void setConnectHeaders(DefaultHttpRequest request, String contentType) {
        request.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        // Required by ConnectStreamingHandler / ConnectUnaryPostHandler — without it the
        // downstream handler would short-circuit with a Connect validation error
        // before producing the inbound ConnectCallExchange these tests assert on.
        request.headers().set("connect-protocol-version", "1");
    }

    private static String methodUri(String methodName) {
        return "/pkg.Service/" + methodName;
    }

    static Stream<String> malformedProcedurePaths() {
        return Stream.of(
            "",
            "/",
            "/OnlyService",
            "/pkg.Service/",
            "//Method",
            "no-leading-slash/pkg.Service/Method"
        );
    }

    static Stream<ContentTypeMismatch> streamingMethodsWithUnaryContentTypes() {
        return Stream.of(
            new ContentTypeMismatch("ServerStream", ConnectTransport.HTTP_1_1, "application/proto"),
            new ContentTypeMismatch("ServerStream", ConnectTransport.HTTP_1_1, "application/json"),
            new ContentTypeMismatch("ClientStream", ConnectTransport.HTTP_1_1, "application/proto"),
            new ContentTypeMismatch("ClientStream", ConnectTransport.HTTP_1_1, "application/json"),
            new ContentTypeMismatch("BidiStream", ConnectTransport.HTTP_2, "application/proto"),
            new ContentTypeMismatch("BidiStream", ConnectTransport.HTTP_2, "application/json")
        );
    }

    static Stream<String> streamingContentTypes() {
        return Stream.of("application/connect+proto", "application/connect+json");
    }

    static Stream<MethodGate> streamingMethodsForGet() {
        return Stream.of(
            new MethodGate("ServerStream", ConnectTransport.HTTP_1_1),
            new MethodGate("ClientStream", ConnectTransport.HTTP_1_1),
            new MethodGate("BidiStream", ConnectTransport.HTTP_2)
        );
    }

    private record ContentTypeMismatch(String methodName, ConnectTransport transport, String contentType) {}

    private record MethodGate(String methodName, ConnectTransport transport) {}

    private static final ConnectMethodDefinition UNARY_METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostRequest.class, false);

    private static final ConnectMethodDefinition SERVER_STREAM_METHOD = new ConnectMethodDefinition(
        "ServerStream", ConnectMethodType.SERVER_STREAMING,
        UnaryPostRequest.class, UnaryPostRequest.class, false);

    private static final ConnectMethodDefinition CLIENT_STREAM_METHOD = new ConnectMethodDefinition(
        "ClientStream", ConnectMethodType.CLIENT_STREAMING,
        UnaryPostRequest.class, UnaryPostRequest.class, false);

    private static final ConnectMethodDefinition BIDI_STREAM_METHOD = new ConnectMethodDefinition(
        "BidiStream", ConnectMethodType.BIDI_STREAMING,
        UnaryPostRequest.class, UnaryPostRequest.class, false);

    private static final ConnectServiceDefinition SERVICE_DEFINITION = new ConnectServiceDefinition(
        "pkg.Service",
        List.of(UNARY_METHOD, SERVER_STREAM_METHOD, CLIENT_STREAM_METHOD, BIDI_STREAM_METHOD),
        null);
}
