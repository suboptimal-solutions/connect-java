package io.suboptimal.connectjava.codec.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.TypeRegistry;
import com.google.protobuf.util.JsonFormat;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

public final class ConnectProtobufJsonCodec implements ConnectCodec {
    public static final ConnectProtobufJsonCodec INSTANCE = new ConnectProtobufJsonCodec();

    private final JsonFormat.Printer printer;
    private final JsonFormat.Parser parser;

    private ConnectProtobufJsonCodec() {
        this.printer = JsonFormat.printer().omittingInsignificantWhitespace();
        this.parser = JsonFormat.parser();
    }

    ConnectProtobufJsonCodec(TypeRegistry typeRegistry) {
        this.printer = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .usingTypeRegistry(typeRegistry);
        this.parser = JsonFormat.parser()
            .usingTypeRegistry(typeRegistry);
    }

    @Override
    public String name() {
        return "json";
    }

    @Override
    public ByteBuf encode(Object value, ByteBufAllocator alloc) throws IOException {
        if (!(value instanceof Message msg)) {
            throw new IOException("Payload must be a protobuf Message, got: " + value.getClass().getSimpleName());
        }
        try {
            byte[] bytes = printer.print(msg).getBytes(StandardCharsets.UTF_8);
            return alloc.buffer(bytes.length).writeBytes(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Failed to serialize to JSON: " + e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(ByteBuf bytes, Class<T> type) throws IOException {
        Message.Builder builder;
        try {
            builder = (Message.Builder) type.getMethod("newBuilder").invoke(null);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException("Cannot create builder for " + type.getName(), e);
        }
        String json = bytes.toString(StandardCharsets.UTF_8);
        try {
            parser.merge(json, builder);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("Failed to deserialize from JSON: " + e.getMessage(), e);
        }
        return (T) builder.build();
    }
}
