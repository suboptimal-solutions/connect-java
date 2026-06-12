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
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.model.ConnectMethodType;
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

    private ConnectCompression responseEncoding = ConnectIdentityCompression.INSTANCE;
    private ConnectEnvelope. @Nullable Decoder decoder;
    private int requestPayloadsSent;
    private int responsePayloadsReceived;
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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (closed) {
            promise.tryFailure(ConnectCallTerminatedException.INSTANCE);
            ReferenceCountUtil.release(msg);
            return;
        }

        switch (msg) {
            case ConnectClientCallStart ignored when outboundState == OutboundState.IDLE -> {
                ConnectMethodType type = callStart.methodDefinition().type();
                if (type == ConnectMethodType.BIDI_STREAMING) {
                    closed = true;
                    promise.setSuccess();
                    ConnectError error = ConnectError.unimplemented("Bidi streaming not supported on HTTP/1.1");
                    observer.onCallComplete(error);
                    ctx.fireChannelRead(error);
                    return;
                }

                responseEncoding = ConnectIdentityCompression.INSTANCE;

                String uri = "/" + callStart.serviceDefinition().serviceName()
                    + "/" + callStart.methodDefinition().methodName();

                DefaultHttpRequest request = new DefaultHttpRequest(
                    HttpVersion.HTTP_1_1, HttpMethod.POST, uri);

                request.headers()
                    .set(HttpHeaderNames.CONTENT_TYPE, ConnectMediaType.streamingContentTypeFor(codec.name()))
                    .set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED)
                    .set(ConnectProtocolHttpHeaders.CONNECT_PROTOCOL_VERSION, ConnectProtocolVersion.HEADER_VERSION);

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
                    closed = true;
                    promise.setSuccess();
                    ConnectError error = ConnectError.unimplemented("Server-streaming method requires exactly one request message");
                    observer.onCallComplete(error);
                    ctx.fireChannelRead(error);
                    return;
                }

                ByteBuf encoded;
                try {
                    encoded = codec.encode(data.data(), ctx.alloc());
                } catch (IOException e) {
                    closed = true;
                    promise.setSuccess();
                    ConnectError error = ConnectError.internal("Serialization failed: " + e.getMessage());
                    observer.onCallComplete(error);
                    ctx.fireChannelRead(error);
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
                        closed = true;
                        promise.setSuccess();
                        ConnectError error = ConnectError.internal("Compression failed: " + e.getMessage());
                        observer.onCallComplete(error);
                        ctx.fireChannelRead(error);
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
        if (statusCode != 200) {
            closed = true;
            ConnectError error = new ConnectError(ClientHandlerSupport.httpStatusToErrorCode(statusCode),
                    response.status().reasonPhrase());
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        String respCodecName = ClientHandlerSupport.codecNameForContentType(response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        ConnectCodec respCodec = respCodecName != null
                ? config.codecRegistry().byName(respCodecName)
                : null;

        if (respCodec == null) {
            closed = true;
            ConnectError error = ConnectError.unknown("Unsupported or missing Content-Type in response");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        String requestCodecName = callStart.codecName();
        if (requestCodecName != null && !requestCodecName.equals(respCodecName)) {
            closed = true;
            ConnectError error = ConnectError.internal(
                    "Response codec '" + respCodecName + "' does not match request codec '" + requestCodecName + "'");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
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

        Map<String, List<String>> headersMap = ClientHandlerSupport.toHeaderMap(response.headers());
        ConnectResponseMeta responseMeta = new ConnectResponseMeta(statusCode, headersMap, Map.of());
        decoder = new ConnectEnvelope.Decoder(ctx.alloc(), config.parameters().maxFrameBytes());
        observer.onResponseHeaders(responseMeta);
        ctx.fireChannelRead(new ConnectClientResponseStart(
            callStart.serviceDefinition(), callStart.methodDefinition(), responseMeta));
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
            closed = true;
            ConnectError error = ConnectError.resourceExhausted(e.getMessage());
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
        }
    }

    private void handleDataFrame(ChannelHandlerContext ctx, byte flags, ByteBuf payload) {
        boolean isCompressed = (flags & ConnectEnvelope.FLAG_COMPRESSED) != 0;
        if (isCompressed && responseEncoding.isIdentity()) {
            payload.release();
            closed = true;
            ConnectError error = ConnectError.internal("Received compressed message but no compression was negotiated");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        ByteBuf decompressed = payload;
        if ((flags & ConnectEnvelope.FLAG_COMPRESSED) != 0) {
            try {
                decompressed = responseEncoding.decompress(payload, ctx.alloc());
            } catch (IOException e) {
                closed = true;
                ConnectError error = ConnectError.internal("Decompression failed: " + e.getMessage());
                observer.onCallComplete(error);
                ctx.fireChannelRead(error);
                return;
            } finally {
                payload.release();
            }
        }

        ConnectMethodType type = callStart.methodDefinition().type();

        if (type == ConnectMethodType.CLIENT_STREAMING && responsePayloadsReceived >= 1) {
            decompressed.release();
            closed = true;
            ConnectError error = ConnectError.unimplemented(
                "Client-streaming method received more than one response message");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        Object decoded;
        try {
            decoded = codec.decode(decompressed, callStart.methodDefinition().responseType());
        } catch (IOException e) {
            closed = true;
            ConnectError error = ConnectError.internal("Deserialization failed: " + e.getMessage());
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        } finally {
            decompressed.release();
        }

        observer.onResponsePayload(decoded);
        responsePayloadsReceived++;
        ctx.fireChannelRead(new ConnectPayload(decoded));
    }

    private void handleEndStreamFrame(ChannelHandlerContext ctx, byte flags, ByteBuf payload) {
        endStreamReceived = true;
        closed = true;

        ByteBuf decompressed = payload;
        if ((flags & ConnectEnvelope.FLAG_COMPRESSED) != 0) {
            try {
                decompressed = responseEncoding.decompress(payload, ctx.alloc());
            } catch (IOException e) {
                ConnectError error = ConnectError.internal("Decompression failed: " + e.getMessage());
                observer.onCallComplete(error);
                ctx.fireChannelRead(error);
                return;
            } finally {
                payload.release();
            }
        }

        byte[] jsonBytes = ClientHandlerSupport.toByteArray(decompressed);
        decompressed.release();

        ConnectError error = config.jsonDeserializer().parseEndStreamError(jsonBytes);
        Map<String, List<String>> trailers = config.jsonDeserializer().parseEndStreamMetadata(jsonBytes);

        if (error != null) {
            observer.onCallComplete(error);
            ctx.fireChannelRead(new ConnectEndOfStream(trailers, error));
        } else {
            if (callStart.methodDefinition().type() == ConnectMethodType.CLIENT_STREAMING
                    && responsePayloadsReceived == 0) {
                ConnectError e = ConnectError.unimplemented(
                        "Client-streaming method received no response message");
                observer.onCallComplete(e);
                ctx.fireChannelRead(new ConnectEndOfStream(trailers, e));
                return;
            }

            ctx.fireChannelRead(new ConnectEndOfStream(trailers, null));
            observer.onCallComplete(null);
        }
    }

    private void handleLastHttpContent(ChannelHandlerContext ctx) {
        if (!endStreamReceived && !closed) {
            closed = true;
            ConnectError error = ConnectError.internal("Truncated stream");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        closeDecoder();
        if (!closed) {
            closed = true;
            ConnectError error = ConnectError.canceled("Connection reset");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        closeDecoder();
        if (!closed) {
            closed = true;
            ConnectError error = ConnectError.canceled("Connection reset");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
        }
    }

    private void closeDecoder() {
        if (decoder != null) {
            decoder.close();
            decoder = null;
        }
    }
}
