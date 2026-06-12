package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.jetbrains.annotations.ApiStatus;
import org.jspecify.annotations.Nullable;

/**
 * Connect streaming envelope framing: {@code 1 byte flags | 4 byte big-endian length | payload bytes}.
 *
 * <p>Encoding is stateless; decoding is stateful (maintains a buffer accumulator).
 */
@ApiStatus.Internal
public final class ConnectEnvelope {
    public static final byte FLAG_COMPRESSED = 0x01;
    public static final byte FLAG_END_STREAM = 0x02;
    static final int HEADER_SIZE = 5;

    private ConnectEnvelope() {}

    public record DecodedFrame(byte flags, ByteBuf payload) {}

    public static final class FrameTooLargeException extends RuntimeException {
        FrameTooLargeException(String message) {
            super(message, null, false, false);
        }
    }

    public static ByteBuf encode(ByteBufAllocator alloc, byte flags, byte[] payload) {
        ByteBuf buf = alloc.buffer(HEADER_SIZE + payload.length);
        buf.writeByte(flags);
        buf.writeInt(payload.length);
        buf.writeBytes(payload);
        return buf;
    }

    public static ByteBuf encode(ByteBufAllocator alloc, byte flags, ByteBuf payload) {
        ByteBuf buf = alloc.buffer(HEADER_SIZE + payload.readableBytes());
        buf.writeByte(flags);
        buf.writeInt(payload.readableBytes());
        buf.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        return buf;
    }

    public static final class Decoder {
        private final ByteBuf accumulator;
        private final int maxFrameBytes;
        private boolean closed;

        public Decoder(ByteBufAllocator alloc, int maxFrameBytes) {
            this.accumulator = alloc.buffer();
            this.maxFrameBytes = maxFrameBytes;
        }

        /**
         * Appends incoming bytes to the internal accumulator. No-op if the decoder is closed
         * or {@code buf} has no readable bytes.
         */
        public void append(ByteBuf buf) {
            if (closed) {
                return;
            }

            if (buf.isReadable()) {
                accumulator.writeBytes(buf);
            }
        }

        /**
         * Returns the next complete envelope frame and advances past it, or {@code null} if the
         * accumulator does not yet hold a full frame. Throws {@link FrameTooLargeException} if
         * the declared payload length exceeds the configured maximum. Returns {@code null} if
         * the decoder is closed.
         */
        @Nullable
        public DecodedFrame pollFrame() {
            if (closed) {
                return null;
            }
            accumulator.discardSomeReadBytes();
            if (accumulator.readableBytes() < HEADER_SIZE) {
                return null;
            }
            int readerIndex = accumulator.readerIndex();
            byte flags = accumulator.getByte(readerIndex);
            long length = accumulator.getUnsignedInt(readerIndex + 1);
            if (length > maxFrameBytes) {
                throw new FrameTooLargeException(
                    "Envelope declared length " + length + " exceeds maximum " + maxFrameBytes);
            }
            int total = HEADER_SIZE + (int) length;
            if (accumulator.readableBytes() < total) {
                return null;
            }
            accumulator.skipBytes(HEADER_SIZE);
            ByteBuf payload = accumulator.readRetainedSlice((int) length);
            return new DecodedFrame(flags, payload);
        }

        public int readableBytes() {
            return closed ? 0 : accumulator.readableBytes();
        }

        public void close() {
            if (!closed) {
                closed = true;
                accumulator.release();
            }
        }
    }
}
