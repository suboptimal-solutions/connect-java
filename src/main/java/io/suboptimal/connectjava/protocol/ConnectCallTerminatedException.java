package io.suboptimal.connectjava.protocol;

/**
 * Singleton signal used when a Connect response handler receives outbound RPC messages after
 * the call has already reached a terminal state. Applies to both unary and streaming calls.
 *
 * <p>The underlying Netty channel is not closed — HTTP/1.1 keep-alive may reuse it for the next
 * request — so {@link java.nio.channels.ClosedChannelException} would be misleading. This
 * exception specifically signals that the RPC call (not the transport) is over.
 *
 * <p>The exception is intentionally stackless and unsuppressed. It is attached to the failed
 * {@link io.netty.channel.ChannelPromise} for late writes that are ignored after the call has
 * terminated.
 */
public class ConnectCallTerminatedException extends RuntimeException {
    public static final ConnectCallTerminatedException INSTANCE = new ConnectCallTerminatedException();

    public ConnectCallTerminatedException() {
        super("Connect RPC call has terminated", null, false, false);
    }
}
