package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import io.suboptimal.connectjava.protocol.ConnectCompressionNegotiation;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class UnaryResponseClientHandler extends SimpleChannelInboundHandler<FullHttpResponse> {
    private final ConnectClientCallStart callStart;
    private final ConnectClientProtocolConfig config;
    private final ConnectClientCallObserver observer;
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
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
        int statusCode = response.status().code();
        ConnectResponseMeta meta = buildMeta(statusCode, response);

        observer.onResponseHeaders(meta);

        ConnectClientResponseStart responseStart =
                new ConnectClientResponseStart(callStart.serviceDefinition(), callStart.methodDefinition(), meta);

        ctx.fireChannelRead(responseStart);

        if (statusCode != 200) {
            ConnectError error = parseErrorResponse(ctx, response, statusCode);
            closed = true;
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        String codecName = ClientHandlerSupport.codecNameForContentType(response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        ConnectCodec codec = codecName != null ? config.codecRegistry().byName(codecName) : null;
        if (codec == null) {
            ConnectError error = ConnectError.unknown("Unsupported or missing Content-Type in response");
            closed = true;
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        String requestCodecName = callStart.codecName();
        if (requestCodecName != null && !requestCodecName.equals(codecName)) {
            ConnectError error = ConnectError.internal("Response codec '" + codecName + "' does not match request codec '" + requestCodecName + "'");
            closed = true;
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        ConnectCompression decompression = resolveResponseEncoding(response.headers().get(HttpHeaderNames.CONTENT_ENCODING));

        ByteBuf body = response.content();
        ByteBuf decompressed;
        try {
            decompressed = ConnectCompressionNegotiation.decompressMessage(ctx.alloc(), body, decompression);
        } catch (IOException e) {
            ConnectError error = ConnectError.internal("Decompression failed: " + e.getMessage());
            closed = true;
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        }

        Object decoded;
        try {
            decoded = codec.decode(decompressed, callStart.methodDefinition().responseType());
        } catch (IOException e) {
            ConnectError error = ConnectError.internal("Deserialization failed: " + e.getMessage());
            closed = true;
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
            return;
        } finally {
            decompressed.release();
        }

        closed = true;
        observer.onResponsePayload(decoded);
        ctx.fireChannelRead(new ConnectPayload(decoded));
        ctx.fireChannelRead(ConnectEndOfStream.INSTANCE);
        observer.onCallComplete(null);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (!closed) {
            closed = true;
            ConnectError error = ConnectError.canceled("Connection reset");
            observer.onCallComplete(error);
            ctx.fireChannelRead(error);
        }
        ctx.fireChannelInactive();
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
            body = new byte[0];
        }

        ConnectErrorBody parsed = body.length > 0
            ? config.jsonDeserializer().parseErrorBody(body) : null;

        ConnectErrorCode code = null;
        if (parsed != null && parsed.codeName() != null) {
            code = ClientHandlerSupport.findErrorCodeByWireName(parsed.codeName());
        }
        if (code == null) {
            code = ClientHandlerSupport.httpStatusToErrorCode(statusCode);
        }

        String message = (parsed != null && parsed.message() != null)
            ? parsed.message() : response.status().reasonPhrase();

        java.util.List<ConnectErrorDetail> details = parsed != null ? parsed.details() : java.util.List.of();
        return new ConnectError(code, message, details);
    }

    private ConnectCompression resolveResponseEncoding(String encodingHeader) {
        String name = ConnectCompressionNegotiation.compressionNameFor(encodingHeader);
        if (name == null) {
            return ConnectIdentityCompression.INSTANCE;
        }
        ConnectCompression c = config.compressionRegistry().resolve(name);
        return c != null ? c : ConnectIdentityCompression.INSTANCE;
    }

    private static ConnectResponseMeta buildMeta(int statusCode, FullHttpResponse response) {
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
        return new ConnectResponseMeta(statusCode, headers, trailers);
    }
}
