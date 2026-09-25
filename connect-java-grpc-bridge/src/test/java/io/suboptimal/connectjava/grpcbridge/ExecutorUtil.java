package io.suboptimal.connectjava.grpcbridge;

import io.netty.channel.embedded.EmbeddedChannel;

/** Helpers for driving a bridge whose calls run on a {@link ManualExecutor}. */
final class ExecutorUtil {
    private ExecutorUtil() {}

    /**
     * Runs everything a call has queued, in either place, until nothing is left.
     *
     * <p>Two queues need draining and each feeds the other: a call-executor task produces a write,
     * which in executor mode is posted to the event loop rather than made inline, and a task run on
     * the loop can hand work back to the call executor. Hence the loop rather than one pass of
     * each. It ends after an iteration that left the executor empty, by which point the writes that
     * iteration posted have already run.
     *
     * <p>{@code EmbeddedChannel.writeInbound} runs pending channel tasks itself, which is why the
     * direct mode never needs any of this - there the call finishes inside {@code writeInbound},
     * and the writes it makes are inline anyway.
     */
    static void settleAllTasks(EmbeddedChannel channel, ManualExecutor executor) {
        do {
            executor.runAllTasks();
            channel.runPendingTasks();
        } while (!executor.isEmpty());
    }
}
