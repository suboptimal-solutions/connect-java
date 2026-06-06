package io.suboptimal.connectjava.codec.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;

public final class ConnectProtobufCodec implements ConnectCodec {
    public static final ConnectProtobufCodec INSTANCE = new ConnectProtobufCodec();

    private final ConcurrentHashMap<Class<?>, Parser<?>> parserCache = new ConcurrentHashMap<>();

    private ConnectProtobufCodec() {}

    @Override
    public String name() {
        return "proto";
    }

    @Override
    public ByteBuf encode(Object value, ByteBufAllocator alloc) throws IOException {
        if (!(value instanceof Message msg)) {
            throw new IOException("Payload must be a protobuf Message, got: " + value.getClass().getSimpleName());
        }
        byte[] bytes = msg.toByteArray();
        return alloc.buffer(bytes.length).writeBytes(bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(ByteBuf bytes, Class<T> type) throws IOException {
        Parser<T> parser = (Parser<T>) parserCache.computeIfAbsent(type, cls -> {
            try {
                return (Parser<?>) cls.getMethod("parser").invoke(null);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException("Cannot get protobuf parser for " + cls.getName(), e);
            }
        });
        try {
            return parser.parseFrom(bytes.nioBuffer());
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Failed to deserialize protobuf: " + e.getMessage(), e);
        }
    }
}
