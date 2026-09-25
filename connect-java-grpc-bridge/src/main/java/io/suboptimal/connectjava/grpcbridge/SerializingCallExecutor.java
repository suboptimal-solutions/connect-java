package io.suboptimal.connectjava.grpcbridge;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * {@link GrpcCallExecutor} that runs the call on an application-supplied {@link Executor}.
 *
 * <p>The executor mode: service and interceptor code runs off the event loop and may block without
 * stalling the channel. One instance per call wraps the one shared delegate, and that is what turns
 * an executor promising nothing about ordering into the per-call serialization domain
 * {@link GrpcCallExecutor} requires.
 *
 * <p>Mechanically this is {@code io.grpc.internal.SerializingExecutor}: tasks go into a queue, and
 * whoever wins the {@link #state} latch submits a single drain to the delegate. A submission that
 * loses the latch adds to the queue and returns - the drain already running picks it up. The drain
 * releases the latch and then re-checks the queue, because a task added between the last
 * {@code poll} and the release would otherwise sit there with nothing scheduled to run it.
 *
 * <p><b>Consecutive tasks may run on different delegate threads.</b> This is why
 * {@link GrpcBridgeHandler} can keep its call state in plain, non-volatile fields: every path
 * between two tasks crosses a happens-before edge. Either both run inside one drain, on one thread,
 * or the earlier drain's {@link #resetState} - a volatile write - is read by the {@link
 * #setRunningState} CAS that claims the next one, which is in turn ordered before the delegate
 * starts it. Do not make those handler fields volatile to "fix" the thread hopping; the edge is
 * already there, and volatile would not add one.
 *
 * <p>The delegate must have spare capacity when a call is in flight - see
 * {@code ConnectGrpcBridge.Builder.withServiceExecutor} for what happens to cancellation otherwise.
 */
final class SerializingCallExecutor implements GrpcCallExecutor {
    private static final Logger log = LoggerFactory.getLogger(SerializingCallExecutor.class);

    private static final VarHandle STATE_OPS;

    static {
        try {
            STATE_OPS = MethodHandles.lookup()
                .findVarHandle(SerializingCallExecutor.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final int STOPPED_STATE = 0;
    private static final int RUNNING_STATE = 1;

    /**
     * Latch deciding who owns the drain, and the only thing keeping two drains from running at
     * once.
     *
     * <p>CAS'd by whichever thread <em>submits</em>, so it goes up before the delegate has picked a
     * thread to drain on. That is why it cannot be folded into {@link #runningThread}: between the
     * CAS and the start of the drain this executor is claimed but has no running thread at all, and
     * a single field storing the submitter's identity would let that submitter run a task inline
     * while the drain runs one concurrently.
     */
    @SuppressWarnings("unused")
    private volatile int state;

    /**
     * The thread currently inside {@link #runTasks}, or {@code null} while no drain is running.
     * Recognises the nested submission {@link GrpcCallExecutor} requires to run inline.
     *
     * <p>Deliberately not volatile. The race is benign in one direction only: the check can fail to
     * spot a nesting, but it cannot claim one that is not there. A thread reads its own identity
     * from this field only if it wrote it, and a thread always observes its own most recent write,
     * so a stale read yields {@code null} or some other thread - both of which answer "no". A wrong
     * answer therefore costs an inline fast path, never mutual exclusion.
     */
    private @Nullable Thread runningThread;

    private final Executor executor;

    private final Queue<Runnable> runQueue = new ConcurrentLinkedQueue<>();

    public SerializingCallExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * Never: the delegate is an arbitrary application executor. It may in fact land on the channel's
     * loop - {@code Runnable::run} does, for submissions made from there - but only sometimes, which
     * is the one answer the write hop cannot work with.
     */
    @Override
    public boolean runsOnEventLoop() {
        return false;
    }

    @Override
    public void execute(Runnable command) {
        if (runningThread == Thread.currentThread()) {
            // A continuation of the task already running on this thread, so it cannot overtake
            // anything. Running it in the caller's stack means an exception propagates to the
            // caller instead of being logged by the drain below - matching direct mode, where that
            // is what lets the handler's catch around a listener callback turn it into failInternal.
            command.run();
        } else {
            runQueue.add(command);
            schedule(command);
        }
    }

    private void schedule(@Nullable Runnable command) {
        if (setRunningState()) {
            boolean success = false;
            try {
                executor.execute(this::runTasks);
                success = true;
            } finally {
                // A rejected submission must not leave the latch raised: nothing would ever drain
                // the queue again, because every later submission would lose the CAS.
                if (!success) {
                    if (command != null) {
                        runQueue.remove(command);
                    }
                    resetState();
                }
            }
        }
    }

    private void runTasks() {
        runningThread = Thread.currentThread();
        Runnable r;
        try {
            while ((r = runQueue.poll()) != null) {
                try {
                    r.run();
                } catch (RuntimeException e) {
                    // Nowhere to propagate to: the delegate thread is not the caller's. gRPC's
                    // SerializingExecutor logs here too. An Error is deliberately not caught, and
                    // then the tail below is skipped along with it - the queue is stranded until
                    // some later submission claims the latch again.
                    log.warn("Exception while executing runnable {}", r, e);
                }
            }
        } finally {
            // Clear before releasing, never after. resetState is a volatile write, so it acts as a
            // release barrier and pins this null ahead of itself. Released first, the next drain
            // could start on another thread and have this line overwrite its runningThread, leaving
            // that drain unable to recognise its own nested submissions.
            runningThread = null;
            resetState();
        }
        if (!runQueue.isEmpty()) {
            schedule(null);
        }
    }

    private boolean setRunningState() {
        return STATE_OPS.compareAndSet(this, STOPPED_STATE, RUNNING_STATE);
    }

    private void resetState() {
        STATE_OPS.getAndSet(this, STOPPED_STATE);
    }
}
