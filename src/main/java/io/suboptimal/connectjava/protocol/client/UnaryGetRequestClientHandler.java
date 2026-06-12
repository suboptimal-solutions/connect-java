package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectProtocolHttpHeaders;
import io.suboptimal.connectjava.protocol.ConnectProtocolVersion;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.Base64;

class UnaryGetRequestClientHandler extends ChannelOutboundHandlerAdapter {
    private final ConnectClientCallStart callStart;
    private final ConnectClientProtocolConfig config;
    private final ConnectClientCallObserver observer;
    private final ConnectCodec codec;
    private final ConnectCompression requestEncoding;

    private byte @Nullable [] payloadBytes;
    private State state = State.IDLE;

    private enum State { IDLE, WAITING_PAYLOAD, WAITING_EOS, TERMINATED }

    UnaryGetRequestClientHandler(ConnectClientCallStart callStart,
                                 ConnectClientProtocolConfig config,
                                 ConnectClientCallObserver observer) {
        this.callStart = callStart;
        this.config = config;
        this.observer = observer;
        this.codec = ClientHandlerSupport.selectRequestCodec(config, callStart.codecName());
        this.requestEncoding = ClientHandlerSupport.selectRequestEncoding(config, callStart.requestHeaders());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (state == State.TERMINATED) {
            promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
            ReferenceCountUtil.release(msg);
            return;
        }

        switch (msg) {
            case ConnectClientCallStart ignored when state == State.IDLE -> {
                state = State.WAITING_PAYLOAD;
                promise.setSuccess();
            }
            case ConnectPayload data when state == State.WAITING_PAYLOAD -> {
                try {
                    ByteBuf encoded = codec.encode(data.data(), ctx.alloc());
                    if (!requestEncoding.isIdentity()) {
                        ByteBuf compressed;
                        try {
                            compressed = requestEncoding.compress(encoded, ctx.alloc());
                        } finally {
                            encoded.release();
                        }
                        payloadBytes = ClientHandlerSupport.toByteArray(compressed);
                        compressed.release();
                    } else {
                        payloadBytes = ClientHandlerSupport.toByteArray(encoded);
                        encoded.release();
                    }
                } catch (IOException e) {
                    promise.tryFailure(e);
                    state = State.TERMINATED;
                    return;
                }
                observer.onRequestPayload(data.data());
                state = State.WAITING_EOS;
                promise.setSuccess();
            }
            case ConnectEndOfStream ignored when state == State.WAITING_EOS -> {
                byte[] bytes = payloadBytes != null ? payloadBytes : new byte[0];
                payloadBytes = null;

                String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
                String codecName = codec.name();

                StringBuilder uriBuilder = new StringBuilder();
                uriBuilder.append('/').append(callStart.serviceDefinition().serviceName())
                    .append('/').append(callStart.methodDefinition().methodName())
                    .append("?message=").append(encoded)
                    .append("&encoding=").append(codecName)
                    .append("&base64=1")
                    .append("&connect=").append(ConnectProtocolVersion.QUERY_VERSION);

                if (!requestEncoding.isIdentity()) {
                    uriBuilder.append("&compression=").append(requestEncoding.name());
                }

                FullHttpRequest request = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.GET, uriBuilder.toString(), Unpooled.EMPTY_BUFFER);

                if (callStart.timeoutMs() != null) {
                    request.headers().set(ConnectProtocolHttpHeaders.CONNECT_TIMEOUT_MS, callStart.timeoutMs());
                }

                request.headers().set(HttpHeaderNames.ACCEPT_ENCODING,
                    ConnectCompressionNegotiation.formatSupportedEncodings(config.compressionRegistry()));

                ClientHandlerSupport.copyUserHeadersForUnaryCall(callStart.requestHeaders(), request.headers());

                ctx.pipeline().addAfter(
                    ConnectClientPipeline.UNARY_GET_HANDLER,
                    ConnectClientPipeline.UNARY_RESPONSE_HANDLER,
                    new UnaryResponseClientHandler(callStart, config, observer));

                observer.onRequestFinished();
                state = State.TERMINATED;
                ctx.write(request, promise);
            }
            default -> {
                promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
                ReferenceCountUtil.release(msg);
            }
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // Once the request is sent (TERMINATED) the response handler owns the terminal
        // callback. If the channel is torn down before that — i.e. this outbound-only
        // handler is removed while still pending — it must deliver onCallComplete itself,
        // since no response handler was installed yet.
        if (state != State.TERMINATED) {
            state = State.TERMINATED;
            observer.onCallComplete(ConnectError.canceled("Connection reset"));
        }
    }

}
