package io.suboptimal.connectjava.grpcbridge;

/**
 * Which {@link GrpcCallExecutor} a bridge under test ends up with.
 *
 * <p>The two are meant to be indistinguishable from outside the bridge: same responses in the same
 * order, same listener callbacks, same status. Most of the bridge's threading exists to keep that
 * true, and a regression in it shows up as one mode passing and the other not - which is what
 * tests parameterized over this enum are for.
 */
enum ExecutorMode {
    /** {@code withDirectExecutor()}: the call runs on the channel's event loop. */
    DIRECT,
    /** {@code withServiceExecutor(...)} over a {@link ManualExecutor} the test drains itself. */
    EXECUTOR
}
