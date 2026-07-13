package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.suboptimal.connectjava.api.ConnectClientCallStart;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import io.suboptimal.connectjava.api.ConnectErrorOrigin;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;
import io.suboptimal.connectjava.protocol.ConnectMediaType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class UnaryResponseClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
    private static final byte[] EMPTY = new byte[0];

    private final ConnectClientCallStart callStart;
    private final ConnectClientProtocolConfig config;
    private final ConnectClientCallObserver observer;
    private final ClientCallDeadline deadline = new ClientCallDeadline();
    private boolean closed;

    UnaryResponseClientHandler(ConnectClientCallStart callStart,
                               ConnectClientProtocolConfig config,
                               ConnectClientCallObserver observer) {
        super(true);
        this.callStart = callStart;
        this.config = config;
        this.observer = observer;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // Installed right after the request is written, so this is the client's local deadline start.
        Long timeoutMs = callStart.timeoutMs();
        if (timeoutMs != null) {
            deadline.schedule(ctx, timeoutMs, () -> onDeadlineExpired(ctx));
        }
    }

    private void onDeadlineExpired(ChannelHandlerContext ctx) {
        if (closed) {
            return;
        }
        fail(ctx, Map.of(), ConnectError.deadlineExceeded("Deadline exceeded"));
        ctx.close();
    }

    /**
     * Delivers the single terminal completion for a failed call: marks it closed, notifies the
     * observer, and forwards an {@link ConnectEndOfStream} carrying {@code error}. Callers must
     * guard on {@link #closed} first (the pipeline guarantees exactly one completion).
     */
    private void fail(ChannelHandlerContext ctx, Map<String, List<String>> trailers, ConnectError error) {
        closed = true;
        observer.onCallComplete(error);
        ctx.fireChannelRead(new ConnectEndOfStream(trailers, error));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
        deadline.cancel();
        if (closed) {
            return;
        }
        MetaContainer meta = buildMeta(response);
        int statusCode = meta.connectResponseMeta.statusCode();

        observer.onResponseHeaders(meta.connectResponseMeta());

        ConnectClientResponseStart responseStart = new ConnectClientResponseStart(callStart.serviceDefinition(),
                callStart.methodDefinition(), meta.connectResponseMeta);

        ctx.fireChannelRead(responseStart);

        if (statusCode != 200) {
            ConnectError error = parseErrorResponse(ctx, response, statusCode);
            fail(ctx, meta.trailers(), error);
            return;
        }

        String codecName = ConnectMediaType.codecNameFor(response);
        ConnectCodec codec = codecName != null ? config.codecRegistry().byName(codecName) : null;
        if (codec == null) {
            ConnectError error = ConnectError.unknown("Unsupported or missing Content-Type in response");
            fail(ctx, meta.trailers(), error);
            return;
        }

        String requestCodecName = callStart.codecName();
        if (requestCodecName != null && !requestCodecName.equals(codecName)) {
            ConnectError error = ConnectError.internal("Response codec '" + codecName + "' does not match request codec '" + requestCodecName + "'");
            fail(ctx, meta.trailers(), error);
            return;
        }

        ConnectCompression decompression = resolveResponseEncoding(response.headers().get(HttpHeaderNames.CONTENT_ENCODING));

        ByteBuf body = response.content();
        ByteBuf decompressed;
        try {
            decompressed = ConnectCompressionNegotiation.decompressMessage(ctx.alloc(), body, decompression);
        } catch (IOException e) {
            ConnectError error = ConnectError.internal("Decompression failed: " + e.getMessage());
            fail(ctx, meta.trailers(), error);
            return;
        }

        Object decoded;
        try {
            decoded = codec.decode(decompressed, callStart.methodDefinition().responseType());
        } catch (IOException e) {
            ConnectError error = ConnectError.internal("Deserialization failed: " + e.getMessage());
            fail(ctx, meta.trailers(), error);
            return;
        } finally {
            decompressed.release();
        }

        closed = true;
        observer.onResponsePayload(decoded);
        ctx.fireChannelRead(new ConnectPayload(decoded));
        ctx.fireChannelRead(new ConnectEndOfStream(meta.trailers()));
        observer.onCallComplete(null);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        deadline.cancel();
        if (!closed) {
            fail(ctx, Map.of(), ConnectError.canceled("Connection reset"));
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        // Guards against a leaked timer when the handler is torn down (e.g. removed at the next call
        // on a keep-alive channel) without a channelInactive.
        deadline.cancel();
    }

    private ConnectError parseErrorResponse(ChannelHandlerContext ctx, FullHttpResponse response, int statusCode) {
        ConnectCompression decompression =
            resolveResponseEncoding(response.headers().get(HttpHeaderNames.CONTENT_ENCODING));

        byte[] body;
        try {
            ByteBuf decompressed = ConnectCompressionNegotiation.decompressMessage(
                ctx.alloc(), response.content(), decompression);
            try {
                body = ClientHandlerSupport.toByteArray(decompressed);
            } finally {
                decompressed.release();
            }
        } catch (IOException e) {
            // Body cannot be decoded; fall back to the HTTP-status mapping below.
            body = EMPTY;
        }

        ConnectErrorBody parsed = body.length > 0
            ? config.jsonDeserializer().parseUnaryError(body) : null;

        ConnectErrorCode code = null;
        if (parsed != null && parsed.codeName() != null) {
            code = ClientHandlerSupport.findErrorCodeByWireName(parsed.codeName());
        }

        // Recognised Connect-code in the JSON body => this is an accepted RPC error; otherwise transport rejection.
        boolean recognizedRpcError = code != null;
        if (code == null) {
            code = ConnectErrorCode.fromHttpStatus(statusCode);
        }

        String message = (parsed != null && parsed.message() != null)
            ? parsed.message() : response.status().reasonPhrase();
        java.util.List<ConnectErrorDetail> details = parsed != null ? parsed.details() : java.util.List.of();

        ConnectErrorOrigin origin = recognizedRpcError ? ConnectErrorOrigin.RPC : ConnectErrorOrigin.TRANSPORT;
        return new ConnectError(code, message, details, origin);
    }

    private ConnectCompression resolveResponseEncoding(String encodingHeader) {
        String name = ConnectCompressionNegotiation.compressionNameFor(encodingHeader);
        if (name == null) {
            return ConnectIdentityCompression.INSTANCE;
        }
        ConnectCompression c = config.compressionRegistry().resolve(name);
        return c != null ? c : ConnectIdentityCompression.INSTANCE;
    }

    private static MetaContainer buildMeta(FullHttpResponse response) {
        int statusCode = response.status().code();
        Map<String, List<String>> all = new LinkedHashMap<>();
        all.putAll(ClientHandlerSupport.toHeaderMap(response.headers()));
        all.putAll(ClientHandlerSupport.toHeaderMap(response.trailingHeaders()));

        Map<String, List<String>> headers = new LinkedHashMap<>();
        Map<String, List<String>> trailers = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : all.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith("trailer-")) {
                trailers.put(name.substring("trailer-".length()), entry.getValue());
            } else {
                headers.put(name, entry.getValue());
            }
        }

        return new MetaContainer(new ConnectResponseMeta(statusCode, headers), trailers);
    }

    private record MetaContainer(ConnectResponseMeta connectResponseMeta, Map<String, List<String>> trailers) { }
}
