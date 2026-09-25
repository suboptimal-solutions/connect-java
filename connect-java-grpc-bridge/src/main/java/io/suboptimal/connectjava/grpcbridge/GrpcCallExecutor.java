package io.suboptimal.connectjava.grpcbridge;

import java.util.concurrent.Executor;

/**
 * The serialization domain of one call.
 *
 * <p>Everything that touches {@link GrpcBridgeHandler}'s state machine is submitted here: inbound
 * pipeline events, listener callbacks, and the {@code ServerCall} operations a service invokes
 * from whatever thread it happens to be on. Implementations must run submitted tasks <b>one at a
 * time and in submission order</b>. That is exactly what gRPC requires of whoever drives a
 * {@code ServerCall.Listener}: "the caller is free to call an instance from multiple threads, but
 * only one call simultaneously" ({@code ServerCall.java:50-53}).
 *
 * <p>The interface exists for that contract, not for its single method: it adds nothing to
 * {@link Executor} that a compiler can check. Naming it is the point. A plain {@code Executor}
 * promises neither ordering nor exclusion, so an implementation that provides both is not
 * interchangeable with one that does not, and a field typed {@code Executor} would hide that.
 *
 * <p><b>A task submitted from inside a running task must run inline, before the enclosing task
 * returns.</b> This is required, not an optimisation: a service that calls {@code close(...)} from
 * a listener callback and then throws has to see its own status reach the client, and a deferred
 * {@code close} loses that race to the {@code INTERNAL} the bridge reports for the escaped
 * throwable. Ordering is unaffected either way - everything submitted earlier has already run, and
 * everything submitted later sits behind the enclosing task. Implementations recognise the case
 * differently ({@link NettyEventLoopCallExecutor} tracks a flag, {@link SerializingCallExecutor}
 * compares the draining thread) but must not differ in the outcome, because a service can tell.
 *
 * <p>Beyond that case, {@link #execute} may run a task inline rather than queueing it whenever
 * doing so cannot overtake work already submitted.
 *
 * <p>Confinement to "the call executor" is not confinement to one thread: an implementation may
 * move consecutive tasks between threads, as long as each pair is separated by a happens-before
 * edge. The handler relies on that, not on thread identity.
 *
 * <p>Writing to the channel is deliberately <b>not</b> routed through this executor: an
 * implementation is free to run tasks off the event loop, while Netty writes have to happen on it.
 * See {@link GrpcBridgeHandler} for how the two hops stay ordered.
 */
sealed interface GrpcCallExecutor extends Executor permits NettyEventLoopCallExecutor, SerializingCallExecutor {

    /**
     * Whether every task this executor runs is guaranteed to run on the event loop of the channel
     * it serves.
     *
     * <p>Decides how the write hop reaches the event loop: {@code true} lets a write happen inline,
     * {@code false} makes it post a task. What matters is that the answer is the same for every
     * write of a call, because the two are not interchangeable in one call - a posted write waits
     * for the loop's next task-draining phase, so an inline write issued afterwards would overtake
     * it and put response messages on the wire out of order.
     *
     * <p>Hence a property of the executor, fixed for its lifetime, rather than a per-write question
     * about the current thread: "am I on the loop right now" can answer differently for two writes
     * of the same call, and an executor that lands on the loop only sometimes ({@code Runnable::run}
     * as a delegate, say) would then mix the two.
     *
     * <p>Answer {@code false} unless the guarantee is unconditional. It costs one task hand-off per
     * write; a wrong {@code true} costs correctness.
     */
    boolean runsOnEventLoop();
}
