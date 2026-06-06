package io.suboptimal.connectjava.protocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.jspecify.annotations.Nullable;

import java.util.Map;

/**
 * One-shot routing handler that picks the correct downstream Connect request handler from
 * the first {@link HttpRequest}'s method and {@code Content-Type}.
 *
 * <ul>
 *   <li>Malformed or unknown procedure paths &rarr; plain HTTP {@code 404 Not Found}.</li>
 *   <li>Bidirectional streaming over HTTP/1.1 &rarr; plain HTTP
 *       {@code 505 HTTP Version Not Supported} with {@code Connection: close}.</li>
 *   <li>{@code GET} on unary calls &rarr; installs an {@link HttpObjectAggregator}
 *       followed by {@link UnaryGetRequestHandler} for side-effect-free calls
 *       with query payloads.</li>
 *   <li>{@code GET} on streaming calls &rarr; plain HTTP {@code 405 Method Not Allowed}
 *       with {@code Allow: POST}.</li>
 *   <li>{@code POST} with {@code application/connect+proto}, {@code application/connect+json}, or another
 *       {@code application/connect+*} type &rarr; installs {@link StreamingHandler}
 *       for streaming procedures, or rejects unary procedures with {@code 415}.</li>
 *   <li>{@code POST} with {@code application/proto}, {@code application/json}, or another
 *       {@code application/*} type &rarr; installs an {@link HttpObjectAggregator}
 *       followed by {@link UnaryPostRequestHandler} for unary procedures, or
 *       rejects streaming procedures with {@code 415}.</li>
 *   <li>Other methods on known procedures &rarr; plain HTTP {@code 405 Method Not Allowed}.</li>
 *   <li>Other {@code POST} content types &rarr; plain HTTP {@code 415 Unsupported Media Type}.</li>
 * </ul>
 *
 * <p>The unary request handlers decode the accepted full request, emit {@link
 * ConnectCallExchange} and {@link ConnectPayload} into the terminal handler, and install
 * {@link UnaryResponseProcessingHandler} for the outbound response leg.
 *
 * <p>On a successful match the handler removes itself from the pipeline and re-fires the
 * original {@link HttpRequest} into the newly configured chain. Plain HTTP error paths release
 * the request themselves; the success paths transfer ownership to the downstream handlers.
 *
 * <p>The {@link ConnectTransport} constructor parameter makes transport-sensitive
 * gates immutable for each handler instance. Annotated
 * {@link ChannelHandler.Sharable @Sharable}: one instance is created per transport
 * configurer in {@link ConnectChannelConfigurer} and reused across every channel of
 * the same {@link ConnectProtocol}. All fields are final and immutable.
 */
@ChannelHandler.Sharable
class RoutingHandler extends SimpleChannelInboundHandler<HttpRequest> {
    private final ConnectTransport transport;
    private final Map<String, ConnectServiceDefinition> serviceDefinitions;
    private final ConnectProtocolParameters parameters;
    private final ConnectProtocolConfig config;
    private final ConnectInterceptorPipeline interceptorPipeline;

    RoutingHandler(ConnectTransport transport, ConnectProtocolConfig config) {
        super(false);
        this.transport = transport;
        this.serviceDefinitions = config.services();
        this.parameters = config.parameters();
        this.config = config;
        this.interceptorPipeline = config.interceptors().isEmpty()
            ? ConnectInterceptorPipeline.EMPTY
            : new ConnectInterceptorPipeline(config.interceptors());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest request) {
        ChannelPipeline pipeline = ctx.pipeline();

        ConnectCallExchange exchange = buildConnectCallExchange(request);
        if (exchange == null) {
            ReferenceCountUtil.release(request);
            ctx.writeAndFlush(HttpResponses.notFound());
            return;
        }

        ConnectMethodType methodType = exchange.methodDefinition().type();
        if (methodType == ConnectMethodType.BIDI_STREAMING && transport == ConnectTransport.HTTP_1_1) {
            ReferenceCountUtil.release(request);
            ctx.writeAndFlush(HttpResponses.httpVersionNotSupported());
            return;
        }

        HttpMethod method = request.method();
        if (method.equals(HttpMethod.GET)) {
            if (methodType != ConnectMethodType.UNARY) {
                ReferenceCountUtil.release(request);
                ctx.writeAndFlush(HttpResponses.methodNotAllowedPostOnly());
                return;
            }
            pipeline.addAfter(
                ConnectPipeline.ROUTING_HANDLER,
                ConnectPipeline.AGGREGATOR_HANDLER,
                new HttpObjectAggregator(parameters.maxRequestBytes()));
            pipeline.addAfter(
                ConnectPipeline.AGGREGATOR_HANDLER,
                ConnectPipeline.UNARY_GET_REQUEST_HANDLER,
                new UnaryGetRequestHandler(
                    exchange, config.codecRegistry(), config.compressionRegistry(),
                    parameters.maxRequestBytes(), config.jsonSerializer(), interceptorPipeline));
            pipeline.remove(this);
            ctx.fireChannelRead(request);
            return;
        }

        if (!method.equals(HttpMethod.POST)) {
            ReferenceCountUtil.release(request);
            ctx.writeAndFlush(methodType == ConnectMethodType.UNARY
                ? HttpResponses.methodNotAllowedGetPost()
                : HttpResponses.methodNotAllowedPostOnly());
            return;
        }

        CharSequence mimeTypeRaw = HttpUtil.getMimeType(request);
        String mimeType = mimeTypeRaw == null ? "" : mimeTypeRaw.toString();

        if (mimeType.startsWith("application/connect+")) {
            if (methodType == ConnectMethodType.UNARY) {
                ReferenceCountUtil.release(request);
                ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
                return;
            }
            pipeline.addAfter(
                ConnectPipeline.ROUTING_HANDLER,
                ConnectPipeline.STREAMING_HANDLER,
                new StreamingHandler(
                    exchange, parameters.maxFrameBytes(), config.codecRegistry(),
                    config.compressionRegistry(), config.jsonSerializer(), interceptorPipeline));
            pipeline.remove(this);
            ctx.fireChannelRead(request);
        } else if (mimeType.startsWith("application/")) {
            if (methodType != ConnectMethodType.UNARY) {
                ReferenceCountUtil.release(request);
                ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
                return;
            }
            pipeline.addAfter(
                ConnectPipeline.ROUTING_HANDLER,
                ConnectPipeline.AGGREGATOR_HANDLER,
                new HttpObjectAggregator(parameters.maxRequestBytes()));
            pipeline.addAfter(
                ConnectPipeline.AGGREGATOR_HANDLER,
                ConnectPipeline.UNARY_POST_REQUEST_HANDLER,
                new UnaryPostRequestHandler(exchange, config.codecRegistry(),
                    config.compressionRegistry(), config.jsonSerializer(), interceptorPipeline));
            pipeline.remove(this);
            ctx.fireChannelRead(request);
        } else {
            ReferenceCountUtil.release(request);
            ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
        }
    }

    private @Nullable ConnectCallExchange buildConnectCallExchange(HttpRequest request) {
        ConnectRoute route = ConnectRoute.parse(request.uri());
        if (route == null) {
            return null;
        }

        ConnectServiceDefinition serviceDefinition = serviceDefinitions.get(route.service());
        if (serviceDefinition == null) {
            return null;
        }

        ConnectMethodDefinition methodDefinition = serviceDefinition.methods().get(route.method());
        if (methodDefinition == null) {
            return null;
        }

        return new ConnectCallExchange(
            serviceDefinition,
            methodDefinition,
            ConnectMetaBuilder.fromHeaders(request.headers()),
            new ResponseHeadersBuilder(),
            new ResponseTrailersBuilder());
    }
}
