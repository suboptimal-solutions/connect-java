package io.suboptimal.connectjava.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectMediaType;
import io.suboptimal.connectjava.protocol.ConnectProtocolVersion;
import org.jspecify.annotations.Nullable;

import java.io.IOException;

/**
 * Handles the inbound half of a Connect unary POST request.
 *
 * <p>The handler expects an aggregated {@link FullHttpRequest}, selects the payload
 * codec from {@code Content-Type}, validates Connect protocol/version and request
 * compression headers, decodes one request body, and fires one {@link ConnectCallExchange}
 * followed by one {@link ConnectPayload}. After successful decoding it installs
 * {@link UnaryResponseProcessingServerHandler} to own the outbound response state
 * machine.
 */
class UnaryPostRequestServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final ConnectCallExchange exchange;
    private final ConnectCodecRegistry codecRegistry;
    private final ConnectCompressionRegistry compressionRegistry;
    private final ConnectJsonSerializer jsonSerializer;
    private final ConnectServerInterceptorPipeline interceptorPipeline;

    UnaryPostRequestServerHandler(ConnectCallExchange exchange, ConnectCodecRegistry codecRegistry,
                                  ConnectCompressionRegistry compressionRegistry, ConnectJsonSerializer jsonSerializer,
                                  ConnectServerInterceptorPipeline interceptorPipeline)
    {
        this.exchange = exchange;
        this.codecRegistry = codecRegistry;
        this.compressionRegistry = compressionRegistry;
        this.jsonSerializer = jsonSerializer;
        this.interceptorPipeline = interceptorPipeline;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        ConnectMethodDefinition methodDefinition = exchange.methodDefinition();

        String codecName = ConnectMediaType.codecNameFor(request);
        ConnectCodec selectedCodec = codecName != null ? codecRegistry.byName(codecName) : null;
        if (selectedCodec == null) {
            ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
            return;
        }

        String versionError = ConnectProtocolVersion.validate(request.headers());
        if (versionError != null) {
            var error = ConnectError.invalidArgument(versionError);
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        String requestCompressionName = ConnectCompressionNegotiation.compressionNameFor(
            request.headers().get(HttpHeaderNames.CONTENT_ENCODING));
        ConnectCompression requestCompression = requestEncoding(requestCompressionName);
        if (requestCompression == null) {
            var error = ConnectError.unimplemented(
                "Unsupported content-encoding: " + requestCompressionName
                    + "; supported: " + ConnectCompressionNegotiation.formatSupportedEncodings(compressionRegistry));
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        ByteBuf decompressed;
        try {
            decompressed = ConnectCompressionNegotiation.decompressMessage(ctx.alloc(), request.content(), requestCompression);
        } catch (IOException e) {
            var error = ConnectError.invalidArgument("Decompression failed: " + e.getMessage());
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        Object decoded;
        try {
            decoded = selectedCodec.decode(decompressed, methodDefinition.requestType());
        } catch (IOException e) {
            var error = ConnectError.invalidArgument("Deserialization failed: " + e.getMessage());
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        } finally {
            decompressed.release();
        }

        ConnectServerInterceptor.Decision decision = interceptorPipeline.interceptCall(exchange);
        ConnectServerCallObserver observer;
        switch (decision) {
            case ConnectServerInterceptor.Decision.Continue(ConnectServerCallObserver o) -> observer = o;
            case ConnectServerInterceptor.Decision.Reject(ConnectServerCallObserver o, ConnectError error) -> {
                observer = o;
                writeRejectedCall(ctx, observer, error);
                return;
            }
        }

        ConnectCompression responseEncoding = ConnectCompressionNegotiation.selectResponseEncoding(
            requestCompression, request.headers().get(HttpHeaderNames.ACCEPT_ENCODING), compressionRegistry);

        ctx.pipeline().addAfter(ConnectServerPipeline.UNARY_POST_REQUEST_HANDLER, ConnectServerPipeline.UNARY_RESPONSE_HANDLER,
            new UnaryResponseProcessingServerHandler(exchange, selectedCodec,
                responseEncoding, false, observer, jsonSerializer));

        ctx.fireChannelRead(exchange);
        observer.onRequestPayload(decoded);
        ctx.fireChannelRead(new ConnectPayload(decoded));
        observer.onRequestFinished();
        ctx.fireChannelRead(ConnectEndOfStream.INSTANCE);
    }

    private void writeRejectedCall(
        ChannelHandlerContext ctx,
        ConnectServerCallObserver observer,
        ConnectError error)
    {
        var response = HttpResponses.protocolError(error, jsonSerializer);
        var trailers = new ResponseTrailersBuilder();
        observer.onResponseTrailers(trailers, error);
        trailers.applyTo(response.headers());
        ctx.writeAndFlush(response)
            .addListener(future -> {
                if (future.isSuccess()) {
                    observer.onCallComplete(error);
                }
            });
    }

    private @Nullable ConnectCompression requestEncoding(@Nullable String encodingName) {
        return encodingName == null ? ConnectIdentityCompression.INSTANCE : compressionRegistry.resolve(encodingName);
    }

}
