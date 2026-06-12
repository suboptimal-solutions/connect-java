package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnaryPostRequestClientHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.UnaryPostFixtureService";
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        SERVICE_NAME, List.of(METHOD), null);
    private static final UnaryPostRequest REQUEST =
        UnaryPostRequest.newBuilder().setText("hello").build();

    private final ConnectClientProtocolConfig config = ClientTestSupport.config();
    private final ConnectCodec proto = ClientTestSupport.protoCodec();
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private ConnectClientCallStart callStart(Map<String, List<String>> headers) {
        return new ConnectClientCallStart(SERVICE, METHOD, headers, false, "proto");
    }

    private void install(ClientTestSupport.RecordingObserver observer, ConnectClientCallStart callStart) {
        channel.pipeline().addLast(ConnectClientPipeline.UNARY_POST_HANDLER,
            new UnaryPostRequestClientHandler(callStart, config, observer));
    }

    @Test
    void sendsPostRequestAndInstallsResponseHandler() throws IOException {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.method()).isEqualTo(HttpMethod.POST);
        assertThat(request.uri()).isEqualTo("/" + SERVICE_NAME + "/Unary");
        assertThat(request.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/proto");
        assertThat(request.headers().get("connect-protocol-version")).isEqualTo("1");
        assertThat(request.headers().get(HttpHeaderNames.CONTENT_LENGTH))
            .isEqualTo(String.valueOf(request.content().readableBytes()));
        assertThat(request.headers().get(HttpHeaderNames.ACCEPT_ENCODING)).contains("gzip");

        UnaryPostRequest decoded = proto.decode(request.content(), UnaryPostRequest.class);
        assertThat(decoded).isEqualTo(REQUEST);
        request.release();

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_RESPONSE_HANDLER)).isNotNull();
        assertThat(observer.events).containsExactly("onRequestPayload", "onRequestFinished");
    }

    @Test
    void compressesRequestWhenContentEncodingGzip() throws IOException {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart =
            callStart(Map.of("content-encoding", List.of("gzip")));
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.headers().get(HttpHeaderNames.CONTENT_ENCODING)).isEqualTo("gzip");

        byte[] body = new byte[request.content().readableBytes()];
        request.content().getBytes(request.content().readerIndex(), body);
        byte[] decompressed = ClientTestSupport.gzipDecompress(body);
        assertThat(decompressed).isEqualTo(ClientTestSupport.encode(proto, REQUEST));
        request.release();
    }

    @Test
    void failsLateWriteBeforeCallStart() {
        var observer = new ClientTestSupport.RecordingObserver();
        install(observer, callStart(Map.of()));

        assertThatThrownBy(() -> channel.writeOutbound(new ConnectPayload(REQUEST)))
            .isInstanceOf(ConnectCallTerminatedException.class);
    }

    @Test
    void handlerRemovedBeforeRequestSentCompletesCallAsCanceled() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);
        channel.writeOutbound(callStart);

        channel.pipeline().remove(ConnectClientPipeline.UNARY_POST_HANDLER);

        assertThat(observer.completeCount).isEqualTo(1);
        assertThat(observer.completeError).isNotNull();
        assertThat(observer.completeError.code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    @Test
    void handlerRemovedAfterRequestSentDoesNotComplete() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpRequest request = channel.readOutbound();
        request.release();

        channel.pipeline().remove(ConnectClientPipeline.UNARY_POST_HANDLER);

        assertThat(observer.completeCount).isZero();
    }

    @Test
    void handlerRemovedWithBufferedPayloadCompletesCallAsCanceled() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));

        channel.pipeline().remove(ConnectClientPipeline.UNARY_POST_HANDLER);

        assertThat(observer.completeCount).isEqualTo(1);
        assertThat(observer.completeError.code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    @Test
    void sendsConnectTimeoutHeaderWhenTimeoutSet() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStartWithTimeout =
            new ConnectClientCallStart(SERVICE, METHOD, Map.of(), false, "proto", 2500L);
        install(observer, callStartWithTimeout);

        channel.writeOutbound(callStartWithTimeout);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.headers().get("connect-timeout-ms")).isEqualTo("2500");
        request.release();
    }

    @Test
    void copiesUserHeadersToOutboundRequest() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart cs = callStart(Map.of("x-custom", List.of("v1", "v2")));
        install(observer, cs);

        channel.writeOutbound(cs);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.headers().getAll("x-custom")).containsExactly("v1", "v2");
        request.release();
    }

    @Test
    void ignoresUserSuppliedReservedHeaders() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart cs = callStart(Map.of("content-type", List.of("text/bogus")));
        install(observer, cs);

        channel.writeOutbound(cs);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/proto");
        request.release();
    }
}
