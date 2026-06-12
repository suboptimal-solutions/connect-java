package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.testfixtures.UnaryGetRequest;
import io.suboptimal.connectjava.testfixtures.UnaryGetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnaryGetRequestClientHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.UnaryGetFixtureService";
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "SafeUnary", ConnectMethodType.UNARY, UnaryGetRequest.class, UnaryGetResponse.class, true);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        SERVICE_NAME, List.of(METHOD), null);
    private static final UnaryGetRequest REQUEST =
        UnaryGetRequest.newBuilder().setText("ping").build();

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
        return new ConnectClientCallStart(SERVICE, METHOD, headers, true, "proto");
    }

    private void install(ClientTestSupport.RecordingObserver observer, ConnectClientCallStart callStart) {
        channel.pipeline().addLast(ConnectClientPipeline.UNARY_GET_HANDLER,
            new UnaryGetRequestClientHandler(callStart, config, observer));
    }

    @Test
    void sendsGetRequestWithBase64QueryAndInstallsResponseHandler() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.method()).isEqualTo(HttpMethod.GET);
        assertThat(request.content().readableBytes()).isZero();

        QueryStringDecoder query = new QueryStringDecoder(request.uri());
        assertThat(query.path()).isEqualTo("/" + SERVICE_NAME + "/SafeUnary");
        assertThat(first(query, "encoding")).isEqualTo("proto");
        assertThat(first(query, "base64")).isEqualTo("1");
        assertThat(first(query, "connect")).isEqualTo("v1");

        byte[] decodedMessage = Base64.getUrlDecoder().decode(first(query, "message"));
        assertThat(decodedMessage).isEqualTo(ClientTestSupport.encode(proto, REQUEST));
        request.release();

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_RESPONSE_HANDLER)).isNotNull();
        assertThat(observer.events).containsExactly("onRequestPayload", "onRequestFinished");
    }

    @Test
    void compressesGetPayloadWhenContentEncodingGzip() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of("content-encoding", List.of("gzip")));
        install(observer, callStart);

        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        QueryStringDecoder query = new QueryStringDecoder(request.uri());
        assertThat(first(query, "compression")).isEqualTo("gzip");

        byte[] decodedMessage = Base64.getUrlDecoder().decode(first(query, "message"));
        byte[] decompressed = ClientTestSupport.gzipDecompress(decodedMessage);
        assertThat(decompressed).isEqualTo(ClientTestSupport.encode(proto, REQUEST));
        request.release();
    }

    @Test
    void handlerRemovedBeforeRequestSentCompletesCallAsCanceled() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart callStart = callStart(Map.of());
        install(observer, callStart);
        channel.writeOutbound(callStart);

        channel.pipeline().remove(ConnectClientPipeline.UNARY_GET_HANDLER);

        assertThat(observer.completeCount).isEqualTo(1);
        assertThat(observer.completeError.code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    // TDD: описывает корректное поведение; может падать на текущей реализации (см. ТЗ §6.3)
    @Test
    void sendsConnectTimeoutHeaderWhenTimeoutSet() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart cs =
            new ConnectClientCallStart(SERVICE, METHOD, Map.of(), true, "proto", 3000L);
        install(observer, cs);

        channel.writeOutbound(cs);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        FullHttpRequest request = channel.readOutbound();
        assertThat(request.headers().get("connect-timeout-ms")).isEqualTo("3000");
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
    void handlerRemovedAfterRequestSentDoesNotComplete() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart cs = callStart(Map.of());
        install(observer, cs);

        channel.writeOutbound(cs);
        channel.writeOutbound(new ConnectPayload(REQUEST));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        FullHttpRequest request = channel.readOutbound();
        request.release();

        channel.pipeline().remove(ConnectClientPipeline.UNARY_GET_HANDLER);

        assertThat(observer.completeCount).isZero();
    }

    @Test
    void handlerRemovedWithBufferedPayloadCompletesCallAsCanceled() {
        var observer = new ClientTestSupport.RecordingObserver();
        ConnectClientCallStart cs = callStart(Map.of());
        install(observer, cs);

        channel.writeOutbound(cs);
        channel.writeOutbound(new ConnectPayload(REQUEST));

        channel.pipeline().remove(ConnectClientPipeline.UNARY_GET_HANDLER);

        assertThat(observer.completeCount).isEqualTo(1);
        assertThat(observer.completeError.code()).isEqualTo(ConnectErrorCode.CANCELED);
    }

    private static String first(QueryStringDecoder query, String name) {
        List<String> values = query.parameters().get(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }
}
