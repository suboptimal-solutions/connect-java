package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectMessage;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.model.ConnectMethodType;
import org.jspecify.annotations.Nullable;

import java.io.IOException;

class StreamingHandler extends ChannelDuplexHandler {
    private final ConnectCallExchange exchange;
    private final int maxFrameBytes;
    private final ConnectCodecRegistry codecRegistry;
    private final ConnectCompressionRegistry compressionRegistry;
    private final ConnectJsonSerializer jsonSerializer;
    private final ConnectInterceptorPipeline interceptorPipeline;

    private ConnectCallObserver observer = ConnectCallObserver.NOOP;
    private @Nullable ConnectCodec codec;
    private ConnectEnvelope.@Nullable Decoder decoder;
    private ConnectCompression requestEncoding = ConnectIdentityCompression.INSTANCE;
    private ConnectCompression responseEncoding = ConnectIdentityCompression.INSTANCE;
    private boolean headersHookFired;
    private boolean responseStarted;
    private boolean closed;
    private int requestPayloadsForwarded;
    private int responsePayloadsWritten;

    StreamingHandler(
        ConnectCallExchange exchange,
        int maxFrameBytes,
        ConnectCodecRegistry codecRegistry,
        ConnectCompressionRegistry compressionRegistry,
        ConnectJsonSerializer jsonSerializer,
        ConnectInterceptorPipeline interceptorPipeline)
    {
        this.exchange = exchange;
        this.maxFrameBytes = maxFrameBytes;
        this.codecRegistry = codecRegistry;
        this.compressionRegistry = compressionRegistry;
        this.jsonSerializer = jsonSerializer;
        this.interceptorPipeline = interceptorPipeline;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest request) {
            handleRequest(ctx, request);
        } else if (msg instanceof HttpContent content) {
            try {
                handleContent(ctx, content);
            } finally {
                content.release();
            }

            if (msg instanceof LastHttpContent) {
                handleLastContent(ctx);
            }
        } else {
            // Netty channel pipeline misconfiguration
            ctx.close();
        }
    }

    private void handleLastContent(ChannelHandlerContext ctx) {
        if (closed) {
            return;
        }
        if (exchange.methodDefinition().type() == ConnectMethodType.SERVER_STREAMING &&
            requestPayloadsForwarded == 0)
        {
            writeStreamingError(ctx, ConnectError.unimplemented(
                "Server-streaming method requires exactly one request message"));
            return;
        }
        observer.onRequestFinished();
        ctx.fireChannelRead(ConnectEndOfStream.INSTANCE);
    }

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request) {
        String codecName = ConnectMediaType.codecNameFor(request);
        ConnectCodec selectedCodec = codecName != null ? codecRegistry.byName(codecName) : null;
        if (selectedCodec == null) {
            ctx.writeAndFlush(HttpResponses.unsupportedMediaType());
            closed = true;
            return;
        }
        codec = selectedCodec;

        String versionError = ConnectProtocolVersion.validate(request.headers());
        if (versionError != null) {
            writeStreamingError(ctx, ConnectError.invalidArgument(versionError));
            return;
        }

        String requestEncodingName = ConnectCompressionNegotiation.compressionNameFor(
            request.headers().get("connect-content-encoding"));
        ConnectCompression selectedRequestEncoding = requestEncoding(requestEncodingName);
        if (selectedRequestEncoding == null) {
            String message = "Unsupported connect-content-encoding: " + requestEncodingName
                + "; supported: " + ConnectCompressionNegotiation.formatSupportedEncodings(compressionRegistry);
            writeStreamingError(ctx, ConnectError.unimplemented(message));
            return;
        }
        requestEncoding = selectedRequestEncoding;

        responseEncoding = ConnectCompressionNegotiation.selectResponseEncoding(
            requestEncoding, request.headers().get("connect-accept-encoding"), compressionRegistry);

        ConnectInterceptor.Decision decision = interceptorPipeline.interceptCall(exchange);
        switch (decision) {
            case ConnectInterceptor.Decision.Continue(ConnectCallObserver o) -> observer = o;
            case ConnectInterceptor.Decision.Reject(ConnectCallObserver o, ConnectError error) -> {
                observer = o;
                writeStreamingError(ctx, error);
                return;
            }
        }

        decoder = new ConnectEnvelope.Decoder(ctx.alloc(), maxFrameBytes);
        ctx.fireChannelRead(exchange);
    }

    private void handleContent(ChannelHandlerContext ctx, HttpContent content) {
        if (closed) {
            return;
        }

        assert decoder != null;
        assert codec != null;

        ConnectEnvelope.Decoder d = decoder;
        d.append(content.content());
        try {
            ConnectEnvelope.DecodedFrame frame;
            while ((frame = d.pollFrame()) != null) {
                ByteBuf payload = frame.payload();
                if ((frame.flags() & ConnectEnvelope.FLAG_COMPRESSED) != 0) {
                    if (requestEncoding.isIdentity()) {
                        payload.release();
                        writeStreamingError(ctx, ConnectError.internal(
                            "Compressed frame but no connect-content-encoding"));
                        return;
                    }
                    ByteBuf compressed = payload;
                    try {
                        payload = requestEncoding.decompress(compressed, ctx.alloc());
                    } catch (IOException e) {
                        writeStreamingError(ctx, ConnectError.invalidArgument(
                            "Decompression failed: " + e.getMessage()));
                        return;
                    } finally {
                        compressed.release();
                    }
                }

                if (exchange.methodDefinition().type() == ConnectMethodType.SERVER_STREAMING &&
                    requestPayloadsForwarded >= 1)
                {
                    payload.release();
                    writeStreamingError(ctx, ConnectError.unimplemented(
                        "Server-streaming method received more than one request message"));
                    return;
                }

                try {
                    Object decoded = codec.decode(payload, exchange.methodDefinition().requestType());
                    observer.onRequestPayload(decoded);
                    ctx.fireChannelRead(new ConnectPayload(decoded));
                    requestPayloadsForwarded++;
                } catch (IOException e) {
                    writeStreamingError(ctx, ConnectError.invalidArgument(
                        "Deserialization failed: " + e.getMessage()));
                    return;
                } finally {
                    payload.release();
                }
            }
        } catch (ConnectEnvelope.FrameTooLargeException e) {
            writeStreamingError(ctx, ConnectError.resourceExhausted(e.getMessage()));
            return;
        }

        if (content instanceof LastHttpContent && d.readableBytes() > 0) {
            writeStreamingError(ctx, ConnectError.invalidArgument("Truncated envelope"));
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof ConnectMessage)) {
            ctx.write(msg, promise);
            return;
        }

        if (closed) {
            promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
            ReferenceCountUtil.release(msg);
            return;
        }

        assert codec != null;

        switch (msg) {
            case ConnectPayload data -> writeOutboundData(ctx, data, promise);
            case ConnectEndOfStream ignored -> writeCompleted(ctx, promise);
            case ConnectError error -> writeConnectError(ctx, error, promise);
            default -> ctx.write(msg, promise);
        }
    }

    private void writeOutboundData(ChannelHandlerContext ctx, ConnectPayload data, ChannelPromise promise) {
        assert codec != null;

        if (!exchange.methodDefinition().responseType().isAssignableFrom(data.data().getClass())) {
            writeStreamingError(ctx,
                ConnectError.internal("Invalid service response, " +
                    "expected: " + exchange.methodDefinition().responseType().getSimpleName() +
                    ", got: " + data.data().getClass().getSimpleName()));
            promise.setSuccess();
            return;
        }

        if (exchange.methodDefinition().type() == ConnectMethodType.CLIENT_STREAMING &&
            responsePayloadsWritten >= 1)
        {
            writeStreamingError(ctx, ConnectError.internal(
                "Client-streaming method produced more than one response message"));
            promise.setSuccess();
            return;
        }

        ByteBuf encoded;
        try {
            encoded = codec.encode(data.data(), ctx.alloc());
        } catch (IOException e) {
            writeStreamingError(ctx,
                ConnectError.internal("Serialization failed: " + e.getMessage()));
            promise.setSuccess();
            return;
        }
        byte flags = 0;
        ByteBuf payload = encoded;
        if (!responseEncoding.isIdentity()) {
            try {
                payload = responseEncoding.compress(encoded, ctx.alloc());
                flags = ConnectEnvelope.FLAG_COMPRESSED;
            } catch (IOException e) {
                encoded.release();
                writeStreamingError(ctx,
                    ConnectError.internal("ConnectCompression failed: " + e.getMessage()));
                promise.setSuccess();
                return;
            }
            encoded.release();
        }
        ensureResponseStarted(ctx);
        try {
            ByteBuf buf = ConnectEnvelope.encode(ctx.alloc(), flags, payload);
            observer.onResponsePayload(data.data());
            responsePayloadsWritten++;
            ctx.writeAndFlush(new DefaultHttpContent(buf), promise);
        } finally {
            payload.release();
        }
    }

    private void writeCompleted(ChannelHandlerContext ctx, ChannelPromise promise) {
        assert codec != null;

        if (exchange.methodDefinition().type() == ConnectMethodType.CLIENT_STREAMING &&
            responsePayloadsWritten == 0)
        {
            writeStreamingError(ctx, ConnectError.internal(
                "Client-streaming method produced no response message"));
            promise.setSuccess();
            return;
        }

        ensureResponseStarted(ctx);
        observer.onResponseTrailers(exchange.responseTrailersBuilder(), null);
        ConnectEndStreamMeta.Builder meta = ConnectEndStreamMeta.builder();
        ((ResponseTrailersBuilder) exchange.responseTrailersBuilder()).applyTo(meta);
        byte[] endBody = jsonSerializer.endStream(new ConnectEndStreamResponse(null, meta.build()));
        ByteBuf buf = ConnectEnvelope.encode(ctx.alloc(), ConnectEnvelope.FLAG_END_STREAM, endBody);
        ctx.write(new DefaultHttpContent(buf));
        ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT, promise)
            .addListener(future -> {
                if (future.isSuccess()) {
                    observer.onCallComplete(null);
                }
            });
        closed = true;
    }

    private void writeConnectError(ChannelHandlerContext ctx, ConnectError error, ChannelPromise promise) {
        assert codec != null;

        writeStreamingError(ctx, error);
        promise.setSuccess();
    }

    private void ensureResponseStarted(ChannelHandlerContext ctx) {
        assert codec != null;

        if (responseStarted) {
            return;
        }
        responseStarted = true;

        DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        String streamingContentType = ConnectMediaType.streamingContentTypeFor(codec.name());
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, streamingContentType)
            .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        if (!responseEncoding.isIdentity()) {
            response.headers().set("connect-content-encoding", responseEncoding.name());
        }
        if (!headersHookFired) {
            headersHookFired = true;
            observer.onResponseHeaders(exchange.responseHeadersBuilder());
            ((ResponseHeadersBuilder) exchange.responseHeadersBuilder()).applyTo(response.headers());
        }
        ctx.write(response);
    }

    private @Nullable ConnectCompression requestEncoding(@Nullable String encodingName) {
        return encodingName == null ? ConnectIdentityCompression.INSTANCE : compressionRegistry.resolve(encodingName);
    }

    private void writeStreamingError(ChannelHandlerContext ctx, ConnectError error) {
        if (closed) {
            return;
        }

        ensureResponseStarted(ctx);
        observer.onResponseTrailers(exchange.responseTrailersBuilder(), error);
        ConnectEndStreamMeta.Builder meta = ConnectEndStreamMeta.builder();
        ((ResponseTrailersBuilder) exchange.responseTrailersBuilder()).applyTo(meta);
        byte[] body = jsonSerializer.endStream(new ConnectEndStreamResponse(error, meta.build()));
        ByteBuf buf = ConnectEnvelope.encode(ctx.alloc(), ConnectEnvelope.FLAG_END_STREAM, body);
        ctx.write(new DefaultHttpContent(buf));
        ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
            .addListener(future -> {
                if (future.isSuccess()) {
                    observer.onCallComplete(error);
                }
            });
        closed = true;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (decoder != null) {
            decoder.close();
        }
        notifyCancel();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (decoder != null) {
            decoder.close();
        }
        notifyCancel();
    }

    private void notifyCancel() {
        if (!closed) {
            closed = true;
            ConnectError canceledError = ConnectError.canceled("Client disconnected");
            observer.onResponseTrailers(exchange.responseTrailersBuilder(), canceledError);
            observer.onCallComplete(canceledError);
        }
    }
}
