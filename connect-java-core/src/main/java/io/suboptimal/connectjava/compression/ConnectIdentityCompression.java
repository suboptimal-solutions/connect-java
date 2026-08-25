package io.suboptimal.connectjava.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public final class ConnectIdentityCompression implements ConnectCompression {
    public static final ConnectIdentityCompression INSTANCE = new ConnectIdentityCompression();

    private ConnectIdentityCompression() {}

    @Override
    public String name() {
        return ConnectCompressionRegistry.IDENTITY_NAME;
    }

    @Override
    public ByteBuf compress(ByteBuf input, ByteBufAllocator alloc) {
        return input.retainedDuplicate();
    }

    @Override
    public ByteBuf decompress(ByteBuf input, ByteBufAllocator alloc) {
        return input.retainedDuplicate();
    }

    @Override
    public boolean isIdentity() {
        return true;
    }
}
