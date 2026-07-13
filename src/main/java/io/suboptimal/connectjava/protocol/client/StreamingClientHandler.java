package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import io.suboptimal.connectjava.api.ConnectClientCallStart;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorOrigin;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.protocol.ConnectCallTerminatedException;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectEnvelope;
import io.suboptimal.connectjava.protocol.ConnectMediaType;
import io.suboptimal.connectjava.protocol.ConnectProtocolHttpHeaders;
import io.suboptimal.connectjava.protocol.ConnectProtocolVersion;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class StreamingClientHandler extends ChannelDuplexHandler {
    private final ConnectClientCallStart callStart;
    private final ConnectClientProtocolConfig config;
    private final ConnectClientCallObserver observer;
    private final ConnectCodec codec;
    private final ConnectCompression requestEncoding;

    private final ClientCallDeadline deadline = new ClientCallDeadline();
    private ConnectCompression responseEncoding = ConnectIdentityCompression.INSTANCE;
    private ConnectEnvelope. @Nullable Decoder decoder;
    private int requestPayloadsSent;
    private int responsePayloadsReceived;
    // Client-streaming carries a single response: buffer it and surface it only once end-of-stream
    // confirms exactly one message arrived, so a protocol-violating extra message yields zero
    // surfaced payloads instead of one. Unused for server-streaming (which delivers eagerly).
    private @Nullable Object bufferedClientStreamResponse;
    private boolean endStreamReceived;
    private boolean closed;

    private enum OutboundState { IDLE, HEADERS_SENT, AWAITING_RESPONSE }
    private OutboundState outboundState = OutboundState.IDLE;

    StreamingClientHandler(ConnectClientCallStart callStart,
                           ConnectClientProtocolConfig config,
                           ConnectClientCallObserver observer)
    {
        this.callStart = callStart;
        this.config = config;
        this.observer = observer;
        this.codec = ClientHandlerSupport.selectRequestCodec(config, callStart.codecName());
        this.requestEncoding = ClientHandlerSupport.selectRequestEncoding(config, callStart.requestHeaders());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // Added by the dispatcher as the call starts, so this marks the client's local deadline start.
        Long timeoutMs = callStart.timeoutMs();
        if (timeoutMs != null) {
            deadline.schedule(ctx, timeoutMs, () -> onDeadlineExpired(ctx));
        }
    }

    private void onDeadlineExpired(ChannelHandlerContext ctx) {
        if (closed) {
            return;
        }
        closeDecoder();
        fail(ctx, Map.of(), ConnectError.deadlineExceeded("Deadline exceeded"));
        ctx.close();
    }

    /**
     * Delivers the single terminal completion for a failed stream: marks it closed, notifies the
     * observer, and forwards an {@link ConnectEndOfStream} carrying {@code error}. Callers own any
     * surrounding cleanup (buffer release, decoder close, deadline cancel, promise completion) and
     * must guard on {@link #closed} where a double completion is possible.
     */
    private void fail(ChannelHandlerContext ctx, Map<String, List<String>> trailers, ConnectError error) {
        closed = true;
        observer.onCallComplete(error);
        ctx.fireChannelRead(new ConnectEndOfStream(trailers, error));
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (closed) {
            promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
            ReferenceCountUtil.release(msg);
            return;
        }

        switch (msg) {
            case ConnectClientCallStart ignored when outboundState == OutboundState.IDLE -> {
                responseEncoding = ConnectIdentityCompression.INSTANCE;

                String uri = "/" + callStart.serviceDefinition().serviceName()
                    + "/" + callStart.methodDefinition().methodName();

                DefaultHttpRequest request = new DefaultHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, uri);

                request.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, ConnectMediaType.streamingContentTypeFor(codec.name()))
                    .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED)
                    .set(ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION, ConnectProtocolVersion.HEADER_VERSION);

                String authority = ClientHandlerSupport.resolveAuthority(callStart, ctx);
                if (authority != null) {
                    request.headers().set(HttpHeaderNames.HOST, authority);
                }

                if (callStart.timeoutMs() != null) {
                    request.headers().set(ConnectProtocolHttpHeaders.CONNECT_TIMEOUT_MS, callStart.timeoutMs());
                }

                if (!requestEncoding.isIdentity()) {
                    request.headers().set(ConnectProtocolHttpHeaders.CONNECT_CONTENT_ENCODING, requestEncoding.name());
                }

                String acceptEncoding = ConnectCompressionNegotiation.formatSupportedEncodings(config.compressionRegistry());
                if (!acceptEncoding.isEmpty()) {
                    request.headers().set(ConnectProtocolHttpHeaders.CONNECT_ACCEPT_ENCODING, acceptEncoding);
                }

                ClientHandlerSupport.copyUserHeadersForStreamCall(callStart.requestHeaders(), request.headers());

                outboundState = OutboundState.HEADERS_SENT;
                promise.setSuccess();
                ctx.write(request);
                ctx.flush();
            }
            case ConnectPayload data when outboundState == OutboundState.HEADERS_SENT -> {
                ConnectMethodType type = callStart.methodDefinition().type();
                if (type == ConnectMethodType.SERVER_STREAMING && requestPayloadsSent >= 1) {
                    promise.setSuccess();
                    fail(ctx, Map.of(), ConnectError.unimplemented("Server-streaming method requires exactly one request message"));
                    return;
                }

                ByteBuf encoded;
                try {
                    encoded = codec.encode(data.data(), ctx.alloc());
                } catch (IOException e) {
                    promise.setSuccess();
                    fail(ctx, Map.of(), ConnectError.internal("Serialization failed: " + e.getMessage()));
                    return;
                }

                byte flags = 0;
                ByteBuf payload = encoded;
                if (!requestEncoding.isIdentity()) {
                    try {
                        payload = requestEncoding.compress(encoded, ctx.alloc());
                        flags = ConnectEnvelope.FLAG_COMPRESSED;
                    } catch (IOException e) {
                        encoded.release();
                        promise.setSuccess();
                        fail(ctx, Map.of(), ConnectError.internal("Compression failed: " + e.getMessage()));
                        return;
                    }
                    encoded.release();
                }

                try {
                    ByteBuf buf = ConnectEnvelope.encode(ctx.alloc(), flags, payload);
                    observer.onRequestPayload(data.data());
                    requestPayloadsSent++;
                    ctx.writeAndFlush(new DefaultHttpContent(buf), promise);
                } finally {
                    payload.release();
                }
            }
            case ConnectEndOfStream ignored when outboundState == OutboundState.HEADERS_SENT -> {
                observer.onRequestFinished();
                outboundState = OutboundState.AWAITING_RESPONSE;
                ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT, promise);
            }
            default -> {
                promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
                ReferenceCountUtil.release(msg);
            }
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpResponse response) {
            handleHttpResponse(ctx, response);
        } else if (msg instanceof HttpContent content) {
            try {
                handleHttpContent(ctx, content);
            } finally {
                content.release();
            }
            if (msg instanceof LastHttpContent) {
                handleLastHttpContent(ctx);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void handleHttpResponse(ChannelHandlerContext ctx, HttpResponse response) {
        int statusCode = response.status().code();

        // ТП.1: as soon as HTTP response metadata arrives, fire ResponseStart before any checks.
        Map<String, List<String>> headersMap = ClientHandlerSupport.toApplicationHeaderMap(response.headers());
        ConnectResponseMeta responseMeta = new ConnectResponseMeta(statusCode, headersMap);
        observer.onResponseHeaders(responseMeta);
        ctx.fireChannelRead(new ConnectClientResponseStart(
            callStart.serviceDefinition(), callStart.methodDefinition(), responseMeta));

        if (statusCode != 200) {
            // Transport rejection: no EndStreamResponse envelope, no trailers (E.1: origin=TRANSPORT).
            ConnectError error = new ConnectError(
                    ConnectErrorCode.fromHttpStatus(statusCode),
                    response.status().reasonPhrase())
                .withOrigin(ConnectErrorOrigin.TRANSPORT);
            fail(ctx, Map.of(), error);
            return;
        }

        String respCodecName = ConnectMediaType.codecNameFor(response);
        ConnectCodec respCodec = respCodecName != null
                ? config.codecRegistry().byName(respCodecName)
                : null;
        if (respCodec == null) {
            fail(ctx, Map.of(), ConnectError.unknown("Unsupported or missing Content-Type in response"));
            return;
        }

        String requestCodecName = callStart.codecName();
        if (requestCodecName != null && !requestCodecName.equals(respCodecName)) {
            fail(ctx, Map.of(), ConnectError.internal(
                    "Response codec '" + respCodecName + "' does not match request codec '" + requestCodecName + "'"));
            return;
        }

        String encodingHeader = response.headers().get(ConnectProtocolHttpHeaders.CONNECT_CONTENT_ENCODING);
        String encodingName = ConnectCompressionNegotiation.compressionNameFor(encodingHeader);
        if (encodingName != null) {
            ConnectCompression c = config.compressionRegistry().resolve(encodingName);
            if (c != null) {
                responseEncoding = c;
            }
        }

        decoder = new ConnectEnvelope.Decoder(ctx.alloc(), config.parameters().maxFrameBytes());
    }

    private void handleHttpContent(ChannelHandlerContext ctx, HttpContent content) {
        if (closed || decoder == null) {
            return;
        }

        decoder.append(content.content());
        try {
            ConnectEnvelope.DecodedFrame frame;
            while ((frame = decoder.pollFrame()) != null) {
                ByteBuf payload = frame.payload();
                boolean isEndStream = (frame.flags() & ConnectEnvelope.FLAG_END_STREAM) != 0;

                if (isEndStream) {
                    handleEndStreamFrame(ctx, frame.flags(), payload);
                    return;
                } else {
                    handleDataFrame(ctx, frame.flags(), payload);
                    if (closed) {
                        return;
                    }
                }
            }
        } catch (ConnectEnvelope.FrameTooLargeException e) {
            fail(ctx, Map.of(), ConnectError.resourceExhausted(e.getMessage()));
        }
    }

    private void handleDataFrame(ChannelHandlerContext ctx, byte flags, ByteBuf payload) {
        boolean isCompressed = (flags & ConnectEnvelope.FLAG_COMPRESSED) != 0;
        if (isCompressed && responseEncoding.isIdentity()) {
            payload.release();
            fail(ctx, Map.of(), ConnectError.internal("Received compressed message but no compression was negotiated"));
            return;
        }

        ByteBuf decompressed = payload;
        if ((flags & ConnectEnvelope.FLAG_COMPRESSED) != 0) {
            try {
                decompressed = responseEncoding.decompress(payload, ctx.alloc());
            } catch (IOException e) {
                fail(ctx, Map.of(), ConnectError.internal("Decompression failed: " + e.getMessage()));
                return;
            } finally {
                payload.release();
            }
        }

        ConnectMethodType type = callStart.methodDefinition().type();

        if (type == ConnectMethodType.CLIENT_STREAMING && responsePayloadsReceived >= 1) {
            decompressed.release();
            fail(ctx, Map.of(), ConnectError.unimplemented(
                "Client-streaming method received more than one response message"));
            return;
        }

        Object decoded;
        try {
            decoded = codec.decode(decompressed, callStart.methodDefinition().responseType());
        } catch (IOException e) {
            fail(ctx, Map.of(), ConnectError.internal("Deserialization failed: " + e.getMessage()));
            return;
        } finally {
            decompressed.release();
        }

        if (type == ConnectMethodType.CLIENT_STREAMING) {
            // Defer delivery until end-of-stream validates the single-message cardinality.
            bufferedClientStreamResponse = decoded;
            responsePayloadsReceived++;
            return;
        }

        observer.onResponsePayload(decoded);
        responsePayloadsReceived++;
        ctx.fireChannelRead(new ConnectPayload(decoded));
    }

    private void handleEndStreamFrame(ChannelHandlerContext ctx, byte flags, ByteBuf payload) {
        deadline.cancel();
        endStreamReceived = true;
        closed = true;

        ByteBuf decompressed = payload;
        if ((flags & ConnectEnvelope.FLAG_COMPRESSED) != 0) {
            try {
                decompressed = responseEncoding.decompress(payload, ctx.alloc());
            } catch (IOException e) {
                fail(ctx, Map.of(), ConnectError.internal("Decompression failed: " + e.getMessage()));
                return;
            } finally {
                payload.release();
            }
        }

        byte[] jsonBytes = ClientHandlerSupport.toByteArray(decompressed);
        decompressed.release();

        ConnectError error = config.jsonDeserializer().parseStreamError(jsonBytes);
        Map<String, List<String>> trailers = config.jsonDeserializer().parseStreamMetadata(jsonBytes);

        if (error != null) {
            fail(ctx, trailers, error);
        } else {
            if (callStart.methodDefinition().type() == ConnectMethodType.CLIENT_STREAMING) {
                if (responsePayloadsReceived == 0) {
                    fail(ctx, trailers, ConnectError.unimplemented(
                            "Client-streaming method received no response message"));
                    return;
                }
                // Exactly one message arrived and the stream ended cleanly: surface it now.
                assert bufferedClientStreamResponse != null;
                observer.onResponsePayload(bufferedClientStreamResponse);
                ctx.fireChannelRead(new ConnectPayload(bufferedClientStreamResponse));
                bufferedClientStreamResponse = null;
            }

            // Success: end-of-stream precedes completion, and carries no error.
            ctx.fireChannelRead(new ConnectEndOfStream(trailers, null));
            observer.onCallComplete(null);
        }
    }

    private void handleLastHttpContent(ChannelHandlerContext ctx) {
        if (!endStreamReceived && !closed) {
            fail(ctx, Map.of(), ConnectError.internal("Truncated stream"));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        deadline.cancel();
        closeDecoder();
        if (!closed) {
            fail(ctx, Map.of(), ConnectError.canceled("Connection reset"));
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        deadline.cancel();
        closeDecoder();
        if (!closed) {
            fail(ctx, Map.of(), ConnectError.canceled("Connection reset"));
        }
    }

    private void closeDecoder() {
        if (decoder != null) {
            decoder.close();
            decoder = null;
        }
    }
}
