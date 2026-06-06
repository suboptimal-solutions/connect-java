package io.suboptimal.connectjava.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

/**
 * Serializes and deserializes RPC payloads.
 */
public interface ConnectCodec {
    /**
     * Returns the canonical lower-case protocol-facing codec name.
     */
    String name();

    /**
     * Serializes a decoded service payload. Implementations infer the wire representation from
     * the runtime value.
     */
    ByteBuf encode(Object value, ByteBufAllocator alloc) throws IOException;

    /**
     * Deserializes bytes into the target Java payload type declared by the service method.
     */
    <T> T decode(ByteBuf bytes, Class<T> type) throws IOException;
}
