package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/**
 * Handles the inbound half of a Connect Unary-Get request.
 *
 * <p>The handler expects an aggregated {@link FullHttpRequest}, validates that the
 * target method is a side-effect-free unary method, selects the payload codec from the
 * {@code encoding} query parameter, decodes the query {@code message}, and fires one
 * {@link ConnectCallExchange} followed by one {@link ConnectPayload}. After successful decoding it
 * installs {@link UnaryResponseProcessingHandler} to own the outbound response
 * state machine.
 */
class UnaryGetRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final ConnectCallExchange exchange;
    private final ConnectCodecRegistry codecRegistry;
    private final ConnectCompressionRegistry compressionRegistry;
    private final int maxRequestBytes;
    private final ConnectJsonSerializer jsonSerializer;
    private final ConnectInterceptorPipeline interceptorPipeline;

    UnaryGetRequestHandler(
        ConnectCallExchange exchange,
        ConnectCodecRegistry codecRegistry,
        ConnectCompressionRegistry compressionRegistry,
        int maxRequestBytes,
        ConnectJsonSerializer jsonSerializer,
        ConnectInterceptorPipeline interceptorPipeline)
    {
        this.exchange = exchange;
        this.codecRegistry = codecRegistry;
        this.compressionRegistry = compressionRegistry;
        this.maxRequestBytes = maxRequestBytes;
        this.jsonSerializer = jsonSerializer;
        this.interceptorPipeline = interceptorPipeline;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (request.content().readableBytes() > 0) {
            ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
            return;
        }

        if (!exchange.methodDefinition().idempotent()) {
            HttpResponse response = HttpResponses.methodNotAllowedPostOnly();
            ctx.writeAndFlush(response);
            return;
        }

        QueryStringDecoder query = new QueryStringDecoder(request.uri(), StandardCharsets.UTF_8);
        String encoding = firstQueryValue(query, "encoding");
        ConnectCodec selectedCodec = codecForEncoding(encoding);
        if (selectedCodec == null) {
            ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
            return;
        }

        String versionError = ConnectProtocolVersion.validate(query.parameters().get("connect"));
        if (versionError != null) {
            var error = ConnectError.invalidArgument(versionError);
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        if (!query.parameters().containsKey("message")) {
            var error = ConnectError.invalidArgument("Missing message query parameter");
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        String compressionName = firstQueryValue(query, "compression");
        ConnectCompression requestEncoding = requestEncoding(compressionName);
        if (requestEncoding == null) {
            var error = ConnectError.unimplemented(
                "Unsupported compression: " + compressionName
                    + "; supported: " + ConnectCompressionNegotiation.formatSupportedEncodings(compressionRegistry));
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        ByteBuf requestBytes;
        try {
            requestBytes = messageBytes(query);
        } catch (IllegalArgumentException e) {
            var error = ConnectError.invalidArgument("Invalid base64 message query parameter");
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        }

        ByteBuf decompressed;
        try {
            decompressed = ConnectCompressionNegotiation.decompressMessage(ctx.alloc(), requestBytes, requestEncoding);
        } catch (IOException e) {
            var error = ConnectError.invalidArgument("Decompression failed: " + e.getMessage());
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        } finally {
            requestBytes.release();
        }

        Object decoded;
        try {
            if (decompressed.readableBytes() > maxRequestBytes) {
                var error = ConnectError.resourceExhausted("Message size " + decompressed.readableBytes() +
                    " exceeds maxRequestBytes " + maxRequestBytes);
                ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
                return;
            }

            decoded = selectedCodec.decode(decompressed, exchange.methodDefinition().requestType());
        } catch (IOException e) {
            var error = ConnectError.invalidArgument("Deserialization failed: " + e.getMessage());
            ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer));
            return;
        } finally {
            decompressed.release();
        }

        ConnectInterceptor.Decision decision = interceptorPipeline.interceptCall(exchange);
        ConnectCallObserver observer;
        switch (decision) {
            case ConnectInterceptor.Decision.Continue(ConnectCallObserver o) -> observer = o;
            case ConnectInterceptor.Decision.Reject(ConnectCallObserver o, ConnectError error) -> {
                observer = o;
                writeRejectedCall(ctx, observer, error);
                return;
            }
        }

        ConnectCompression responseCompression = ConnectCompressionNegotiation.selectResponseEncoding(
            requestEncoding, request.headers().get(HttpHeaderNames.ACCEPT_ENCODING), compressionRegistry);

        ctx.pipeline().addAfter(ConnectPipeline.UNARY_GET_REQUEST_HANDLER, ConnectPipeline.UNARY_RESPONSE_HANDLER,
            new UnaryResponseProcessingHandler(exchange, selectedCodec,
                responseCompression, true, observer, jsonSerializer));

        ctx.fireChannelRead(exchange);
        observer.onRequestPayload(decoded);
        ctx.fireChannelRead(new ConnectPayload(decoded));
        observer.onRequestFinished();
        ctx.fireChannelRead(ConnectEndOfStream.INSTANCE);
    }

    private void writeRejectedCall(
        ChannelHandlerContext ctx,
        ConnectCallObserver observer,
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

    private @Nullable ConnectCodec codecForEncoding(@Nullable String encoding) {
        if (encoding == null || encoding.isEmpty()) {
            return null;
        }
        ConnectCodec codec = codecRegistry.byName(encoding);
        if (codec == null) {
            return null;
        }
        try {
            ConnectMediaType.unaryContentTypeFor(codec.name());
        } catch (IllegalArgumentException e) {
            return null;
        }
        return codec;
    }

    private @Nullable ConnectCompression requestEncoding(@Nullable String compressionName) {
        if (compressionName == null || compressionName.isEmpty()) {
            return ConnectIdentityCompression.INSTANCE;
        }
        return compressionRegistry.resolve(compressionName);
    }

    private static ByteBuf messageBytes(QueryStringDecoder query) {
        String message = firstQueryValue(query, "message");
        if (message == null) {
            message = "";
        }
        if ("1".equals(firstQueryValue(query, "base64"))) {
            return Unpooled.wrappedBuffer(Base64.getUrlDecoder().decode(padBase64(message)));
        }
        return Unpooled.wrappedBuffer(message.getBytes(StandardCharsets.UTF_8));
    }

    private static String padBase64(String value) {
        int remainder = value.length() % 4;
        if (remainder == 0) {
            return value;
        }
        return value + "=".repeat(4 - remainder);
    }

    private static @Nullable String firstQueryValue(QueryStringDecoder query, String name) {
        List<String> values = query.parameters().get(name);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.getFirst();
    }
}
