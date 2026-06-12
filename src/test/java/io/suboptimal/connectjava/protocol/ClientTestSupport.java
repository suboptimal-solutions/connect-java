package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.protocol.client.ConnectClientCallObserver;
import io.suboptimal.connectjava.protocol.client.ConnectClientInterceptor;
import io.suboptimal.connectjava.protocol.client.ConnectClientProtocolConfig;
import io.suboptimal.connectjava.protocol.client.ConnectClientProtocolParameters;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Shared helpers and recording doubles for client-side handler tests. */
public final class ClientTestSupport {
    static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    static final int MAX_FRAME_BYTES = 1024 * 1024;

    private static final ChannelHandler NOOP_TERMINAL = new ChannelInboundHandlerAdapter();

    private ClientTestSupport() {}

    public static ConnectClientProtocolConfig config() {
        return baseBuilder().build();
    }

    public static ConnectClientProtocolConfig config(List<ConnectClientInterceptor> interceptors) {
        return baseBuilder().interceptors(interceptors).build();
    }

    public static ConnectClientProtocolConfig configWithMaxFrameBytes(int maxFrameBytes) {
        return ConnectClientProtocolConfig.builder(
            () -> NOOP_TERMINAL,
            new ConnectClientProtocolParameters(MAX_RESPONSE_BYTES, maxFrameBytes),
            ConnectProtobufCodecs.defaults()).build();
    }

    private static ConnectClientProtocolConfig.Builder baseBuilder() {
        return ConnectClientProtocolConfig.builder(
            () -> NOOP_TERMINAL,
            new ConnectClientProtocolParameters(MAX_RESPONSE_BYTES, MAX_FRAME_BYTES),
            ConnectProtobufCodecs.defaults());
    }

    public static ConnectCodec protoCodec() {
        ConnectCodec codec = ConnectProtobufCodecs.defaults().byName("proto");
        if (codec == null) {
            throw new IllegalStateException("proto codec not registered");
        }
        return codec;
    }

    static ConnectCompression gzip() {
        ConnectCompression gzip = ConnectCompressionRegistry.standard().resolve("gzip");
        if (gzip == null) {
            throw new IllegalStateException("gzip not registered");
        }
        return gzip;
    }

    /** Encodes a message with the given codec and returns the raw bytes. */
    public static byte[] encode(ConnectCodec codec, Object message) {
        ByteBuf buf = null;
        try {
            buf = codec.encode(message, UnpooledByteBufAllocator.DEFAULT);
            return ByteBufUtil.getBytes(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (buf != null) {
                buf.release();
            }
        }
    }

    /** gzip-compresses raw bytes. */
    public static byte[] gzipCompress(byte[] raw) {
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer(raw.length).writeBytes(raw);
        ByteBuf compressed = null;
        try {
            compressed = gzip().compress(input, UnpooledByteBufAllocator.DEFAULT);
            return ByteBufUtil.getBytes(compressed);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            input.release();
            if (compressed != null) {
                compressed.release();
            }
        }
    }

    /** gzip-decompresses raw bytes. */
    public static byte[] gzipDecompress(byte[] compressed) {
        ByteBuf input = UnpooledByteBufAllocator.DEFAULT.buffer(compressed.length).writeBytes(compressed);
        ByteBuf decompressed = null;
        try {
            decompressed = gzip().decompress(input, UnpooledByteBufAllocator.DEFAULT);
            return ByteBufUtil.getBytes(decompressed);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            input.release();
            if (decompressed != null) {
                decompressed.release();
            }
        }
    }

    /** Records observer callbacks for ordering and arity assertions. */
    public static final class RecordingObserver implements ConnectClientCallObserver {
        public final List<String> events = new ArrayList<>();
        final List<Object> requestPayloads = new ArrayList<>();
        final List<Object> responsePayloads = new ArrayList<>();
        @Nullable ConnectResponseMeta responseMeta;
        public @Nullable ConnectError completeError;
        public int completeCount;

        @Override
        public void onRequestPayload(Object payload) {
            events.add("onRequestPayload");
            requestPayloads.add(payload);
        }

        @Override
        public void onRequestFinished() {
            events.add("onRequestFinished");
        }

        @Override
        public void onResponseHeaders(ConnectResponseMeta meta) {
            events.add("onResponseHeaders");
            responseMeta = meta;
        }

        @Override
        public void onResponsePayload(Object payload) {
            events.add("onResponsePayload");
            responsePayloads.add(payload);
        }

        @Override
        public void onCallComplete(@Nullable ConnectError error) {
            events.add("onCallComplete");
            completeCount++;
            completeError = error;
        }
    }

    /** Interceptor that always rejects with the given error. */
    public static ConnectClientInterceptor rejectingInterceptor(ConnectError error) {
        return callStart -> ConnectClientInterceptor.reject(error);
    }

    /** Interceptor that continues with the given observer. */
    public static ConnectClientInterceptor continuingInterceptor(ConnectClientCallObserver observer) {
        return callStart -> ConnectClientInterceptor.continueWith(observer);
    }
}
