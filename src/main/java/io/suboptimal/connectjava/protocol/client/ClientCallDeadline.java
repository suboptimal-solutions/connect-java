package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.TimeUnit;

/**
 * Per-call deadline timer for the client pipeline. Schedules a one-shot task on the channel's event
 * loop that fires when the call's {@code timeoutMs} elapses, unless it is cancelled first.
 *
 * <p>All methods must be invoked on the channel event loop, matching the single-threaded model of the
 * client handlers; the timer keeps no cross-thread state. The owning handler is responsible for
 * guarding its terminal delivery (so a late fire is a no-op) and for calling {@link #cancel()} on
 * every terminal and teardown path so no timer outlives its call on a keep-alive channel.
 */
final class ClientCallDeadline {
    private @Nullable ScheduledFuture<?> future;

    /** Schedules {@code onExpire} to run after {@code timeoutMs}; a no-op if already scheduled. */
    void schedule(ChannelHandlerContext ctx, long timeoutMs, Runnable onExpire) {
        if (future == null) {
            future = ctx.executor().schedule(onExpire, timeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    /** Cancels the pending task, if any. Safe to call repeatedly and after the task has fired. */
    void cancel() {
        if (future != null) {
            future.cancel(false);
            future = null;
        }
    }
}
