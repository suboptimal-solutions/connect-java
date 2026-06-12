package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.model.ConnectMethodType;

@ChannelHandler.Sharable
class ConnectClientCallDispatcher extends ChannelOutboundHandlerAdapter {
    private final ConnectClientProtocolConfig config;
    private final ConnectClientInterceptorPipeline interceptorPipeline;

    ConnectClientCallDispatcher(ConnectClientProtocolConfig config) {
        this.config = config;
        this.interceptorPipeline = config.interceptors().isEmpty()
            ? ConnectClientInterceptorPipeline.EMPTY
            : new ConnectClientInterceptorPipeline(config.interceptors());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (!(msg instanceof ConnectClientCallStart callStart)) {
            ctx.write(msg, promise);
            return;
        }

        ChannelPipeline pipeline = ctx.pipeline();

        if (pipeline.get(ConnectClientPipeline.UNARY_POST_HANDLER) != null) {
            pipeline.remove(ConnectClientPipeline.UNARY_POST_HANDLER);
        }
        if (pipeline.get(ConnectClientPipeline.UNARY_GET_HANDLER) != null) {
            pipeline.remove(ConnectClientPipeline.UNARY_GET_HANDLER);
        }
        if (pipeline.get(ConnectClientPipeline.UNARY_RESPONSE_HANDLER) != null) {
            pipeline.remove(ConnectClientPipeline.UNARY_RESPONSE_HANDLER);
        }
        if (pipeline.get(ConnectClientPipeline.AGGREGATOR_HANDLER) != null) {
            pipeline.remove(ConnectClientPipeline.AGGREGATOR_HANDLER);
        }
        if (pipeline.get(ConnectClientPipeline.STREAMING_HANDLER) != null) {
            pipeline.remove(ConnectClientPipeline.STREAMING_HANDLER);
        }

        ConnectClientInterceptor.Decision decision = interceptorPipeline.interceptCall(callStart);
        switch (decision) {
            case ConnectClientInterceptor.Decision.Reject(ConnectClientCallObserver observer, var error) -> {
                observer.onCallComplete(error);
                ctx.fireChannelRead(error);
                promise.setSuccess();
            }
            case ConnectClientInterceptor.Decision.Continue(var observer, var rewrittenCallStart) -> {
                // Interceptors may rewrite the outgoing request; use the effective call start
                // (falling back to the original when no interceptor rewrote it) everywhere below.
                ConnectClientCallStart effectiveCallStart = rewrittenCallStart != null
                        ? rewrittenCallStart
                        : callStart;

                // codecName is a plain String (not an enum) because codecs are extensible and are
                // identified by name on the wire and in the registry. The downside of a String is
                // that a typo would otherwise slip through and silently fall back to the default
                // codec, sending the request in an unintended format. So an explicitly requested
                // codec that is not registered fails the call here rather than being ignored;
                // a null codecName still means "use the registry's preferred codec".
                String codecName = effectiveCallStart.codecName();
                if (codecName != null && config.codecRegistry().byName(codecName) == null) {
                    ConnectError error = ConnectError.internal(
                        "Unknown codec '" + codecName + "'; registered: " + registeredCodecNames());
                    observer.onCallComplete(error);
                    ctx.fireChannelRead(error);
                    promise.setSuccess();
                    return;
                }

                ConnectMethodType type = effectiveCallStart.methodDefinition().type();
                switch (type) {
                    case UNARY -> {
                        pipeline.addBefore(ConnectClientPipeline.CALL_DISPATCHER,
                            ConnectClientPipeline.AGGREGATOR_HANDLER,
                            new HttpObjectAggregator(config.parameters().maxResponseBytes()));
                        if (effectiveCallStart.preferGet() && effectiveCallStart.methodDefinition().idempotent()) {
                            pipeline.addBefore(ConnectClientPipeline.CALL_DISPATCHER,
                                ConnectClientPipeline.UNARY_GET_HANDLER,
                                new UnaryGetRequestClientHandler(effectiveCallStart, config, observer));
                        } else {
                            pipeline.addBefore(ConnectClientPipeline.CALL_DISPATCHER,
                                ConnectClientPipeline.UNARY_POST_HANDLER,
                                new UnaryPostRequestClientHandler(effectiveCallStart, config, observer));
                        }
                    }
                    case SERVER_STREAMING, CLIENT_STREAMING, BIDI_STREAMING ->
                        pipeline.addBefore(ConnectClientPipeline.CALL_DISPATCHER,
                            ConnectClientPipeline.STREAMING_HANDLER,
                            new StreamingClientHandler(effectiveCallStart, config, observer));
                }
                ctx.write(effectiveCallStart, promise);
            }
        }
    }

    private String registeredCodecNames() {
        StringBuilder sb = new StringBuilder();
        for (ConnectCodec codec : config.codecRegistry().preferred()) {
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append(codec.name());
        }
        return sb.toString();
    }
}
