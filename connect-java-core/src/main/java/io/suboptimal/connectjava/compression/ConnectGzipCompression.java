package io.suboptimal.connectjava.compression;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;

import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class ConnectGzipCompression implements ConnectCompression {
    public static final ConnectGzipCompression INSTANCE = new ConnectGzipCompression();

    private ConnectGzipCompression() {}

    @Override
    public String name() {
        return "gzip";
    }

    @Override
    public ByteBuf compress(ByteBuf input, ByteBufAllocator alloc) throws IOException {
        ByteBuf out = alloc.buffer();
        try (
            ByteBufInputStream in = new ByteBufInputStream(input.retainedDuplicate(), true);
            GZIPOutputStream gzip = new GZIPOutputStream(new ByteBufOutputStream(out))
        ) {
            in.transferTo(gzip);
        } catch (IOException | RuntimeException e) {
            out.release();
            throw e;
        }
        return out;
    }

    @Override
    public ByteBuf decompress(ByteBuf input, ByteBufAllocator alloc) throws IOException {
        ByteBuf out = alloc.buffer();
        try (
            ByteBufInputStream in = new ByteBufInputStream(input.retainedDuplicate(), true);
            GZIPInputStream gzip = new GZIPInputStream(in);
            ByteBufOutputStream decoded = new ByteBufOutputStream(out)
        ) {
            gzip.transferTo(decoded);
        } catch (IOException | RuntimeException e) {
            out.release();
            throw e;
        }
        return out;
    }
}
