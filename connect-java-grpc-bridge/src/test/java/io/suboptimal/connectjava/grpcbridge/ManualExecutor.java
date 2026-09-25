package io.suboptimal.connectjava.grpcbridge;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * Executor that runs nothing until a test says so.
 *
 * <p>Handed to {@code ConnectGrpcBridge.Builder.withServiceExecutor}, it makes the executor mode
 * deterministic: a call does not advance inside {@code writeInbound} the way it does in direct
 * mode, it advances exactly when {@link #runAllTasks()} is called. That is also what lets a test
 * place an event - a cancellation, say - at a chosen point of a call instead of racing it.
 *
 * <p>Tasks run on whichever thread drains, normally the test's own. The queue is concurrent
 * anyway: a test service is free to answer from a thread of its own, and its submissions arrive
 * here from that thread.
 */
class ManualExecutor implements Executor {
    private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();

    @Override
    public void execute(Runnable command) {
        taskQueue.add(command);
    }

    /** Runs everything queued, including whatever those tasks queue in turn. */
    int runAllTasks() {
        int runTasks = 0;
        Runnable task;
        while ((task = taskQueue.poll()) != null) {
            task.run();
            runTasks++;
        }
        return runTasks;
    }

    int getSize() {
        return taskQueue.size();
    }

    boolean isEmpty() {
        return taskQueue.isEmpty();
    }
}
