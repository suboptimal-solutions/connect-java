package io.suboptimal.connectjava.grpcbridge;

import io.netty.util.concurrent.ThreadAwareExecutor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * {@link GrpcCallExecutor} that runs the call on the channel event loop.
 *
 * <p>The direct mode, selected by {@code ConnectGrpcBridge.Builder.withDirectExecutor()}: the event loop is
 * both the transport thread and the thread service code runs on, so a task submitted from the event
 * loop normally executes inline, queueing nothing.
 *
 * <p>Because every task ends up on the event loop, the write hop in {@code BridgeServerCall} always
 * finds itself already there and writes directly. That is a property of this executor, not of the
 * bridge - see the threading notes on {@link GrpcBridgeHandler}.
 *
 * <p>Inline execution is only safe while nothing is queued. A service is allowed to call
 * {@code ServerCall} methods from its own thread and then continue on the event loop - the first
 * call gets queued, and running the second one inline would let it overtake the first. The
 * {@code pendingOffLoop} latch prevents that: once anything has been queued, everything queues
 * until the queue drains.
 *
 * <p>A submission made from <em>inside</em> a running task is the one case that runs inline
 * regardless of the latch. It cannot overtake anything: everything submitted earlier has already
 * run, and everything submitted later is behind the running task in the queue. Without this,
 * whether a nested submission nests or is deferred would depend on whether the enclosing task
 * happened to be queued - and a service that calls {@code close(...)} and then throws out of a
 * listener callback would report a different status in the two cases.
 *
 * <p>This deliberately diverges from gRPC, whose {@code SerializeReentrantCallsDirectExecutor}
 * defers reentrant tasks instead. gRPC can afford to: its serializing executor sits on the inbound
 * path only, so a nested {@code ServerCall} operation never reaches it.
 */
final class NettyEventLoopCallExecutor implements GrpcCallExecutor {
    private static final VarHandle PENDING_OFF_LOOP_OPS;

    static {
        try {
            PENDING_OFF_LOOP_OPS = MethodHandles.lookup()
                .findVarHandle(NettyEventLoopCallExecutor.class, "pendingOffLoop", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ThreadAwareExecutor executor;

    // Guards against misorder of events
    @SuppressWarnings("unused")
    private volatile int pendingOffLoop;

    /**
     * True while this executor is running one of its own tasks. Needs no synchronisation, and for a
     * stronger reason than its counterpart in {@link SerializingCallExecutor}: it is written only
     * from inside {@link #runTask}, which always runs on the event loop, and read only after
     * {@code executor.isExecutorThread} has confirmed the caller is on that same loop. No other
     * thread can observe it, so this field is genuinely confined - not benignly raced.
     */
    private boolean runningTask;

    NettyEventLoopCallExecutor(ThreadAwareExecutor executor) {
        this.executor = executor;
    }

    /** Unconditionally: both branches of {@link #execute} end up on the loop. */
    @Override
    public boolean runsOnEventLoop() {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        if (executor.isExecutorThread(Thread.currentThread()) && (runningTask || hasNoPendingOffLoopOps())) {
            runTask(command);
        } else {
            incrPendingOffLoopOps();
            boolean succeeded = false;
            try {
                executor.execute(() -> {
                    try {
                        runTask(command);
                    } finally {
                        decrPendingOffLoopOps();
                    }
                });
                succeeded = true;
            } finally {
                // A rejected submission must not leave the latch raised: every later task would
                // queue forever, and the inline fast path would never come back.
                if (!succeeded) {
                    decrPendingOffLoopOps();
                }
            }
        }
    }

    /** Restores rather than clears the flag: nesting can go deeper than one level. */
    private void runTask(Runnable command) {
        boolean previous = runningTask;
        runningTask = true;
        try {
            command.run();
        } finally {
            runningTask = previous;
        }
    }

    private boolean hasNoPendingOffLoopOps() {
        return (int) PENDING_OFF_LOOP_OPS.getVolatile(this) == 0;
    }

    private void incrPendingOffLoopOps() {
        PENDING_OFF_LOOP_OPS.getAndAdd(this, 1);
    }

    private void decrPendingOffLoopOps() {
        PENDING_OFF_LOOP_OPS.getAndAdd(this, -1);
    }
}
