package io.suboptimal.connectjava.protocol.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectMessage;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.protocol.ConnectMediaType;
import io.suboptimal.connectjava.protocol.client.ConnectCallTerminatedException;
import org.jspecify.annotations.Nullable;

import java.io.IOException;

/**
 * Outbound Connect unary response state machine.
 *
 * <p>The handler is installed by a successful unary request handler and consumes the
 * outbound Connect message sequence produced by the terminal handler. A successful unary call
 * transitions from awaiting the first {@link ConnectPayload}, to awaiting {@link ConnectEndOfStream},
 * to terminated after exactly one HTTP response is written. {@link ConnectError} and protocol
 * state violations write Connect JSON error responses and terminate the call.
 */
class UnaryResponseProcessingServerHandler extends ChannelOutboundHandlerAdapter {
    private final ConnectCallExchange exchange;
    private final ConnectCodec responseCodec;
    private final ConnectCompression responseCompression;
    private final boolean varyAcceptEncoding;
    private final ConnectServerCallObserver observer;
    private final ConnectJsonSerializer jsonSerializer;

    private State state = State.AWAITING_RESPONSE;
    private @Nullable FullHttpResponse pendingResponse;

    UnaryResponseProcessingServerHandler(
        ConnectCallExchange exchange,
        ConnectCodec responseCodec,
        ConnectCompression responseCompression,
        boolean varyAcceptEncoding,
        ConnectServerCallObserver observer,
        ConnectJsonSerializer jsonSerializer)
    {
        this.exchange = exchange;
        this.responseCodec = responseCodec;
        this.responseCompression = responseCompression;
        this.varyAcceptEncoding = varyAcceptEncoding;
        this.observer = observer;
        this.jsonSerializer = jsonSerializer;
    }

    @Override
    public final void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (!(msg instanceof ConnectMessage)) {
            ctx.write(msg, promise);
            return;
        }

        if (state == State.TERMINATED) {
            promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
            ReferenceCountUtil.release(msg);
            return;
        }

        switch (msg) {
            case ConnectPayload ignore when state != State.AWAITING_RESPONSE -> {
                var error = ConnectError.internal("Unary RPC produced more than one response payload");
                ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer), promise);
                terminate();
            }
            case ConnectPayload data -> {
                state = prepareResponse(ctx, data, promise);
            }
            case ConnectEndOfStream ignored when state != State.AWAITING_COMPLETE -> {
                var error = ConnectError.internal("Unary RPC completed without a response");
                ctx.writeAndFlush(HttpResponses.protocolError(error, jsonSerializer), promise);
                terminate();
            }
            case ConnectEndOfStream ignored -> {
                writeResponse(ctx, promise);
                terminate();
            }
            case ConnectError error -> {
                writeObservedError(ctx, error, promise);
                terminate();
            }
            default -> ctx.write(msg, promise);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        if (state != State.TERMINATED) {
            ConnectError canceledError = ConnectError.canceled("Client disconnected");
            observer.onResponseTrailers(exchange.responseTrailersBuilder(), canceledError);
            observer.onCallComplete(canceledError);
        }
        terminate();
    }

    private State prepareResponse(ChannelHandlerContext ctx, ConnectPayload data, ChannelPromise promise) {
        if (!exchange.methodDefinition().responseType().isAssignableFrom(data.data().getClass())) {
            var error = ConnectError.internal("Invalid service response, " +
                "expected: " + exchange.methodDefinition().responseType().getSimpleName() +
                ", got: " + data.data().getClass().getSimpleName());
            writeObservedError(ctx, error, promise);
            return State.TERMINATED;
        }

        ByteBuf encoded;
        try {
            encoded = responseCodec.encode(data.data(), ctx.alloc());
        } catch (IOException e) {
            writeObservedError(ctx, ConnectError.internal("Serialization failed: " + e.getMessage()), promise);
            return State.TERMINATED;
        }

        ByteBuf decompressed;
        if (responseCompression.isIdentity()) {
            decompressed = encoded;
        } else {
            try {
                decompressed = responseCompression.compress(encoded, ctx.alloc());
            } catch (IOException e) {
                writeObservedError(ctx, ConnectError.internal("ConnectCompression failed: " + e.getMessage()), promise);
                return State.TERMINATED;
            } finally {
                encoded.release();
            }
        }

        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK, decompressed);
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, ConnectMediaType.unaryContentTypeFor(responseCodec.name()))
            .set(HttpHeaderNames.CONTENT_LENGTH, decompressed.readableBytes());
        if (!responseCompression.isIdentity()) {
            response.headers().set(HttpHeaderNames.CONTENT_ENCODING, responseCompression.name());
        }
        if (varyAcceptEncoding) {
            response.headers().add(HttpHeaderNames.VARY, "Accept-Encoding");
        }
        observer.onResponseHeaders(exchange.responseHeadersBuilder());
        ((ResponseHeadersBuilder) exchange.responseHeadersBuilder()).applyTo(response.headers());
        observer.onResponsePayload(data.data());

        pendingResponse = response;
        promise.setSuccess();
        return State.AWAITING_COMPLETE;
    }

    private void writeResponse(ChannelHandlerContext ctx, ChannelPromise promise) {
        FullHttpResponse response = pendingResponse;
        pendingResponse = null;
        observer.onResponseTrailers(exchange.responseTrailersBuilder(), null);
        ((ResponseTrailersBuilder) exchange.responseTrailersBuilder()).applyTo(response.headers());
        ctx.writeAndFlush(response, promise)
            .addListener(future -> {
                if (future.isSuccess()) {
                    observer.onCallComplete(null);
                }
            });
    }

    private void writeObservedError(ChannelHandlerContext ctx, ConnectError error, ChannelPromise promise) {
        FullHttpResponse response = HttpResponses.protocolError(error, jsonSerializer);
        observer.onResponseHeaders(exchange.responseHeadersBuilder());
        ((ResponseHeadersBuilder) exchange.responseHeadersBuilder()).applyTo(response.headers());
        observer.onResponseTrailers(exchange.responseTrailersBuilder(), error);
        ((ResponseTrailersBuilder) exchange.responseTrailersBuilder()).applyTo(response.headers());
        ctx.writeAndFlush(response, promise)
            .addListener(future -> {
                if (future.isSuccess()) {
                    observer.onCallComplete(error);
                }
            });
    }

    private void terminate() {
        state = State.TERMINATED;

        if (pendingResponse != null) {
            ReferenceCountUtil.release(pendingResponse);
            pendingResponse = null;
        }
    }

    private enum State {
        AWAITING_RESPONSE,
        AWAITING_COMPLETE,
        TERMINATED
    }
}
