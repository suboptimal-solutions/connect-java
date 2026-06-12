package io.suboptimal.connectjava.protocol;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalIoHandler;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpServerCodec;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.client.ConnectClientCallStart;
import io.suboptimal.connectjava.protocol.client.ConnectClientProtocol;
import io.suboptimal.connectjava.protocol.client.ConnectClientProtocolConfig;
import io.suboptimal.connectjava.protocol.client.ConnectClientProtocolParameters;
import io.suboptimal.connectjava.protocol.server.ConnectServerProtocol;
import io.suboptimal.connectjava.protocol.server.ConnectServerProtocolConfig;
import io.suboptimal.connectjava.protocol.server.ConnectServerProtocolParameters;
import io.suboptimal.connectjava.testfixtures.StreamingRequest;
import io.suboptimal.connectjava.testfixtures.StreamingResponse;
import io.suboptimal.connectjava.testfixtures.UnaryGetRequest;
import io.suboptimal.connectjava.testfixtures.UnaryGetResponse;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientProtocolIntegrationTest {
    private static final int MAX = 4 * 1024 * 1024;
    private static final LocalAddress ADDRESS = new LocalAddress("connect-client-it");

    private static final ConnectMethodDefinition UNARY_POST = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition UNARY_POST_SERVICE = new ConnectServiceDefinition(
        "connectjava.test.v1.UnaryPostFixtureService", List.of(UNARY_POST), null);

    private static final ConnectMethodDefinition UNARY_GET = new ConnectMethodDefinition(
        "SafeUnary", ConnectMethodType.UNARY, UnaryGetRequest.class, UnaryGetResponse.class, true);
    private static final ConnectServiceDefinition UNARY_GET_SERVICE = new ConnectServiceDefinition(
        "connectjava.test.v1.UnaryGetFixtureService", List.of(UNARY_GET), null);

    private static final ConnectMethodDefinition SERVER_STREAMING = new ConnectMethodDefinition(
        "ServerStreaming", ConnectMethodType.SERVER_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectMethodDefinition CLIENT_STREAMING = new ConnectMethodDefinition(
        "ClientStreaming", ConnectMethodType.CLIENT_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectServiceDefinition STREAMING_SERVICE = new ConnectServiceDefinition(
        "connectjava.test.v1.StreamingFixtureService", List.of(SERVER_STREAMING, CLIENT_STREAMING), null);

    private static EventLoopGroup group;
    private static Channel serverChannel;

    @BeforeAll
    static void startServer() throws InterruptedException {
        group = new MultiThreadIoEventLoopGroup(LocalIoHandler.newFactory());

        Map<String, ConnectServiceDefinition> services = Map.of(
            UNARY_POST_SERVICE.serviceName(), UNARY_POST_SERVICE,
            UNARY_GET_SERVICE.serviceName(), UNARY_GET_SERVICE,
            STREAMING_SERVICE.serviceName(), STREAMING_SERVICE);

        ConnectServerProtocolConfig serverConfig = ConnectServerProtocolConfig.builder(
            services, ServerEcho::new,
            new ConnectServerProtocolParameters(MAX, MAX),
            ConnectProtobufCodecs.defaults()).build();
        ConnectServerProtocol serverProtocol = new ConnectServerProtocol(serverConfig);

        serverChannel = new ServerBootstrap()
            .group(group)
            .channel(LocalServerChannel.class)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(new HttpServerCodec());
                    serverProtocol.http1().configure(ch);
                }
            })
            .bind(ADDRESS).sync().channel();
    }

    @AfterAll
    static void stopServer() throws InterruptedException {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (group != null) {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }

    @Test
    void unaryPostRoundTrip() throws Exception {
        CallResult result = call(UNARY_POST_SERVICE, UNARY_POST, false,
            List.of(UnaryPostRequest.newBuilder().setText("hi").build()));

        assertThat(result.error).isNull();
        assertThat(result.meta.statusCode()).isEqualTo(200);
        assertThat(result.payloads).hasSize(1);
        assertThat(((UnaryPostResponse) result.payloads.getFirst()).getText()).isEqualTo("echo:hi");
    }

    @Test
    void unaryGetRoundTrip() throws Exception {
        CallResult result = call(UNARY_GET_SERVICE, UNARY_GET, true,
            List.of(UnaryGetRequest.newBuilder().setText("safe").build()));

        assertThat(result.error).isNull();
        assertThat(result.payloads).hasSize(1);
        assertThat(((UnaryGetResponse) result.payloads.getFirst()).getText()).isEqualTo("echo:safe");
    }

    @Test
    void serverStreamingRoundTrip() throws Exception {
        CallResult result = call(STREAMING_SERVICE, SERVER_STREAMING, false,
            List.of(StreamingRequest.newBuilder().setText("go").build()));

        assertThat(result.error).isNull();
        assertThat(result.payloads).hasSize(2);
        assertThat(((StreamingResponse) result.payloads.get(0)).getText()).isEqualTo("echo:go#0");
        assertThat(((StreamingResponse) result.payloads.get(1)).getText()).isEqualTo("echo:go#1");
    }

    @Test
    void clientStreamingRoundTrip() throws Exception {
        CallResult result = call(STREAMING_SERVICE, CLIENT_STREAMING, false,
            List.of(StreamingRequest.newBuilder().setText("a").build(),
                StreamingRequest.newBuilder().setText("b").build()));

        assertThat(result.error).isNull();
        assertThat(result.payloads).hasSize(1);
        assertThat(((StreamingResponse) result.payloads.getFirst()).getText()).isEqualTo("echo:a");
    }

    @Test
    void unaryErrorPropagates() throws Exception {
        CallResult result = call(UNARY_POST_SERVICE, UNARY_POST, false,
            List.of(UnaryPostRequest.newBuilder().setText("FAIL").build()));

        assertThat(result.error).isNotNull();
        assertThat(result.error.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
    }

    @Test
    void serverStreamingErrorPropagates() throws Exception {
        CallResult result = call(STREAMING_SERVICE, SERVER_STREAMING, false,
            List.of(StreamingRequest.newBuilder().setText("FAIL").build()));

        assertThat(result.error).isNotNull();
        assertThat(result.error.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
    }

    private CallResult call(ConnectServiceDefinition service, ConnectMethodDefinition method,
                            boolean preferGet, List<Object> requests) throws Exception {
        CompletableFuture<CallResult> future = new CompletableFuture<>();
        ConnectClientCallStart callStart =
            new ConnectClientCallStart(service, method, Map.of(), preferGet, "proto");

        ConnectClientProtocolConfig clientConfig = ConnectClientProtocolConfig.builder(
            () -> new DriverHandler(callStart, requests, future),
            new ConnectClientProtocolParameters(MAX, MAX),
            ConnectProtobufCodecs.defaults()).build();
        ConnectClientProtocol clientProtocol = new ConnectClientProtocol(clientConfig);

        Channel channel = new Bootstrap()
            .group(group)
            .channel(LocalChannel.class)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) {
                    ch.pipeline().addLast(new HttpClientCodec());
                    clientProtocol.http1().configure(ch);
                }
            })
            .connect(ADDRESS).sync().channel();

        try {
            return future.get(5, TimeUnit.SECONDS);
        } finally {
            channel.close().sync();
        }
    }

    private record CallResult(List<Object> payloads, ConnectError error, ConnectResponseMeta meta) {}

    /** Client terminal handler: drives one call on activation, captures the result. */
    private static final class DriverHandler extends ChannelInboundHandlerAdapter {
        private final ConnectClientCallStart callStart;
        private final List<Object> requests;
        private final CompletableFuture<CallResult> future;
        private final List<Object> payloads = new ArrayList<>();
        private ConnectResponseMeta meta;

        DriverHandler(ConnectClientCallStart callStart, List<Object> requests,
                      CompletableFuture<CallResult> future) {
            this.callStart = callStart;
            this.requests = requests;
            this.future = future;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            ctx.write(callStart);
            for (Object request : requests) {
                ctx.write(new ConnectPayload(request));
            }
            ctx.writeAndFlush(ConnectEndOfStream.INSTANCE);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            switch (msg) {
                case ConnectClientResponseStart exchange -> meta = exchange.responseMeta();
                case ConnectPayload payload -> payloads.add(payload.data());
                case ConnectEndOfStream eos -> future.complete(new CallResult(payloads, eos.error(), meta));
                case ConnectError error -> future.complete(new CallResult(payloads, error, meta));
                default -> { }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            future.completeExceptionally(cause);
        }
    }

    /** Server terminal handler: echoes requests; fails when a request carries the text "FAIL". */
    private static final class ServerEcho extends ChannelInboundHandlerAdapter {
        private ConnectCallExchange exchange;
        private final List<String> requestTexts = new ArrayList<>();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            switch (msg) {
                case ConnectCallExchange ex -> exchange = ex;
                case ConnectPayload payload -> requestTexts.add(textOf(payload.data()));
                case ConnectEndOfStream ignored -> respond(ctx);
                default -> { }
            }
        }

        private void respond(ChannelHandlerContext ctx) {
            if (requestTexts.contains("FAIL")) {
                ctx.writeAndFlush(ConnectError.notFound("requested failure"));
                return;
            }
            String base = requestTexts.isEmpty() ? "" : requestTexts.getFirst();
            ConnectMethodType type = exchange.methodDefinition().type();
            int count = type == ConnectMethodType.SERVER_STREAMING ? 2 : 1;
            for (int i = 0; i < count; i++) {
                String text = type == ConnectMethodType.SERVER_STREAMING
                    ? "echo:" + base + "#" + i
                    : "echo:" + base;
                ctx.write(new ConnectPayload(response(exchange, text)));
            }
            ctx.writeAndFlush(ConnectEndOfStream.INSTANCE);
        }

        private static String textOf(Object request) {
            return switch (request) {
                case UnaryPostRequest r -> r.getText();
                case UnaryGetRequest r -> r.getText();
                case StreamingRequest r -> r.getText();
                default -> throw new IllegalStateException("unexpected request: " + request);
            };
        }

        private static Object response(ConnectCallExchange exchange, String text) {
            Class<?> type = exchange.methodDefinition().responseType();
            if (type == UnaryPostResponse.class) {
                return UnaryPostResponse.newBuilder().setText(text).build();
            }
            if (type == UnaryGetResponse.class) {
                return UnaryGetResponse.newBuilder().setText(text).build();
            }
            if (type == StreamingResponse.class) {
                return StreamingResponse.newBuilder().setText(text).build();
            }
            throw new IllegalStateException("unexpected response type: " + type);
        }
    }
}
