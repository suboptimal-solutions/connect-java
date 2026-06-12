package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.embedded.EmbeddedChannel;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.netty.handler.codec.http.FullHttpRequest;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.testfixtures.StreamingRequest;
import io.suboptimal.connectjava.testfixtures.StreamingResponse;
import io.suboptimal.connectjava.testfixtures.UnaryGetRequest;
import io.suboptimal.connectjava.testfixtures.UnaryGetResponse;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientCallDispatcherTest {
    private static final ConnectMethodDefinition UNARY_POST = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectMethodDefinition UNARY_IDEMPOTENT = new ConnectMethodDefinition(
        "SafeUnary", ConnectMethodType.UNARY, UnaryGetRequest.class, UnaryGetResponse.class, true);
    private static final ConnectMethodDefinition SERVER_STREAMING = new ConnectMethodDefinition(
        "ServerStreaming", ConnectMethodType.SERVER_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "svc.Service", List.of(UNARY_POST, UNARY_IDEMPOTENT, SERVER_STREAMING), null);

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private void install(ConnectClientProtocolConfig config) {
        channel.pipeline().addLast(ConnectClientPipeline.CALL_DISPATCHER,
            new ConnectClientCallDispatcher(config));
    }

    private ConnectClientCallStart callStart(ConnectMethodDefinition method, boolean preferGet, String codecName) {
        return new ConnectClientCallStart(SERVICE, method, Map.of(), preferGet, codecName);
    }

    @Test
    void installsPostHandlerForUnary() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.AGGREGATOR_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_GET_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.STREAMING_HANDLER)).isNull();
    }

    @Test
    void installsGetHandlerForIdempotentPreferGet() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_IDEMPOTENT, true, "proto"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_GET_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
    }

    @Test
    void usesPostWhenPreferGetButNotIdempotent() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_POST, true, "proto"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_GET_HANDLER)).isNull();
    }

    @Test
    void installsStreamingHandlerForStreaming() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(SERVER_STREAMING, false, "proto"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.STREAMING_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
    }

    @Test
    void nullCodecNameProceeds() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_POST, false, null));

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNotNull();
        Object inbound = channel.readInbound();
        assertThat(inbound).isNull();
    }

    @Test
    void registeredCodecNameProceeds() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_POST, false, "json"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNotNull();
        Object inbound = channel.readInbound();
        assertThat(inbound).isNull();
    }

    @Test
    void unknownCodecNameFailsCall() {
        install(ClientTestSupport.config());
        channel.writeOutbound(callStart(UNARY_POST, false, "bogus"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(((ConnectError) inbound).message()).contains("Unknown codec");
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
    }

    @Test
    void interceptorRejectDeliversErrorAndInstallsNoHandler() {
        ConnectError rejection = ConnectError.permissionDenied("denied");
        install(ClientTestSupport.config(List.of(ClientTestSupport.rejectingInterceptor(rejection))));

        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.PERMISSION_DENIED);
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
    }

    @Test
    void interceptorObserverReceivesCallbacks() {
        var observer = new ClientTestSupport.RecordingObserver();
        install(ClientTestSupport.config(List.of(ClientTestSupport.continuingInterceptor(observer))));

        ConnectClientCallStart callStart = callStart(UNARY_POST, false, "proto");
        channel.writeOutbound(callStart);
        channel.writeOutbound(new ConnectPayload(UnaryPostRequest.newBuilder().setText("x").build()));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        Object request = channel.readOutbound();
        if (request instanceof io.netty.util.ReferenceCounted rc) {
            rc.release();
        }
        assertThat(observer.events).containsExactly("onRequestPayload", "onRequestFinished");
    }

    @Test
    void reuseRemovesPreviousHandlersWithoutSpuriousComplete() {
        var observer = new ClientTestSupport.RecordingObserver();
        install(ClientTestSupport.config(List.of(ClientTestSupport.continuingInterceptor(observer))));

        // First call: drive the request fully so the POST handler reaches TERMINATED and the
        // response handler is installed.
        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));
        channel.writeOutbound(new ConnectPayload(UnaryPostRequest.newBuilder().setText("x").build()));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        Object firstRequest = channel.readOutbound();
        if (firstRequest instanceof io.netty.util.ReferenceCounted rc) {
            rc.release();
        }

        // Second call reuses the channel.
        channel.writeOutbound(callStart(SERVER_STREAMING, false, "proto"));

        assertThat(channel.pipeline().get(ConnectClientPipeline.STREAMING_HANDLER)).isNotNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_RESPONSE_HANDLER)).isNull();
        assertThat(channel.pipeline().get(ConnectClientPipeline.AGGREGATOR_HANDLER)).isNull();
        assertThat(observer.completeCount).isZero();
    }

    @Test
    void interceptorCanRewriteOutgoingHeaders() {
        ConnectClientInterceptor adder = cs ->
            ConnectClientInterceptor.continueWith(cs.withHeader("x-test", "v1"));
        install(ClientTestSupport.config(List.of(adder)));

        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));
        channel.writeOutbound(new ConnectPayload(UnaryPostRequest.newBuilder().setText("x").build()));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        Object out = channel.readOutbound();
        assertThat(out).isInstanceOf(FullHttpRequest.class);
        FullHttpRequest req = (FullHttpRequest) out;
        assertThat(req.headers().get("x-test")).isEqualTo("v1");
        req.release();
    }

    @Test
    void rewriteIsThreadedToNextInterceptor() {
        AtomicReference<ConnectClientCallStart> seenBySecond = new AtomicReference<>();
        ConnectClientInterceptor first = cs ->
            ConnectClientInterceptor.continueWith(cs.withHeader("x-a", "1"));
        ConnectClientInterceptor second = cs -> {
            seenBySecond.set(cs);
            return ConnectClientInterceptor.continueWith(cs.withHeader("x-b", "2"));
        };
        install(ClientTestSupport.config(List.of(first, second)));

        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));
        channel.writeOutbound(new ConnectPayload(UnaryPostRequest.newBuilder().setText("x").build()));
        channel.writeOutbound(ConnectEndOfStream.INSTANCE);

        // the second interceptor observed the first interceptor's rewrite
        assertThat(seenBySecond.get().requestHeaders()).containsKey("x-a");

        Object out = channel.readOutbound();
        assertThat(out).isInstanceOf(FullHttpRequest.class);
        FullHttpRequest req = (FullHttpRequest) out;
        assertThat(req.headers().get("x-a")).isEqualTo("1");
        assertThat(req.headers().get("x-b")).isEqualTo("2");
        req.release();
    }

    @Test
    void codecValidationAppliesToRewrittenCodec() {
        // original codec is valid; an interceptor rewrites it to an unregistered one
        ConnectClientInterceptor breaker = cs ->
            ConnectClientInterceptor.continueWith(cs.withCodecName("bogus"));
        install(ClientTestSupport.config(List.of(breaker)));

        channel.writeOutbound(callStart(UNARY_POST, false, "proto"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).message()).contains("Unknown codec");
        assertThat(channel.pipeline().get(ConnectClientPipeline.UNARY_POST_HANDLER)).isNull();
    }

    @Test
    void passesThroughNonCallStartMessages() {
        install(ClientTestSupport.config());
        channel.writeOutbound("ping");
        Object out = channel.readOutbound();
        assertThat(out).isEqualTo("ping");
    }
}
