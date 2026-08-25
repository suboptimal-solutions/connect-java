package io.suboptimal.connectjava.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

/**
 * Compresses and decompresses independent RPC messages.
 */
public interface ConnectCompression {
    /**
     * Returns the canonical lower-case protocol-facing compression name.
     */
    String name();

    ByteBuf compress(ByteBuf input, ByteBufAllocator alloc) throws IOException;

    ByteBuf decompress(ByteBuf input, ByteBufAllocator alloc) throws IOException;

    default boolean isIdentity() {
        return false;
    }
}
