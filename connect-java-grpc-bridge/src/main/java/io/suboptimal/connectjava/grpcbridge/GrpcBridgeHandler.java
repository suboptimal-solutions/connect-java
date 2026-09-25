package io.suboptimal.connectjava.grpcbridge;

import com.google.protobuf.Any;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutor;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import io.suboptimal.connectjava.api.ConnectMessage;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * Terminal Netty handler that bridges Connect protocol messages to a gRPC service call.
 *
 * <p>The handler owns the whole state machine for one call. The {@link ServerCall} handed to the
 * service is a thin inner facade ({@code BridgeServerCall}) that forwards into that one state
 * machine, so inbound pipeline events and outbound {@code ServerCall} operations never mutate two
 * objects kept in sync.
 *
 * <p>Message flow:
 * <pre>
 * ConnectCallExchange -&gt; startCall     -&gt; handler.startCall(bridgeCall, metadata) -&gt; Listener
 * ConnectPayload      -&gt; messageQueue  -&gt; listener.onMessage(decoded)      when demand allows
 * ConnectEndOfStream  -&gt; messageQueue  -&gt; listener.onHalfClose()           when the queue drains
 * request(n)          -&gt; demand += n   -&gt; drains whatever the queue is holding
 * sendMessage(resp)   -&gt; ctx.write(ConnectPayload)
 * close(OK)           -&gt; ctx.writeAndFlush(ConnectEndOfStream) -&gt; listener.onComplete()
 * close(err)          -&gt; ctx.writeAndFlush(ConnectError)       -&gt; listener.onComplete()
 * channel writable    -&gt; listener.onReady()
 * channel closed      -&gt; listener.onCancel()
 * </pre>
 *
 * <p><b>Inbound flow control:</b> nothing is handed to the listener until the service asks for it.
 * Inbound events queue in arrival order and {@code request(n)} releases them, so a service that
 * blocks, or one using {@code disableAutoRequest()}, decides its own pace. The end-of-stream marker
 * queues with the messages rather than being tracked beside them: that is what keeps
 * {@code onHalfClose} behind every message that preceded it, and what makes delivering it twice
 * impossible. It is ordered by demand but not charged against it - a service is never obliged to
 * ask for anything in order to learn that the client is done.
 *
 * <p>That queue is bounded by <b>taking over the channel's auto-read setting</b>: past
 * {@link #MESSAGE_Q_HIGH_WATERMARK} queued events the handler calls {@code setAutoRead(false)}, and
 * back under {@link #MESSAGE_Q_LOW_WATERMARK} it calls {@code setAutoRead(true)}. For the lifetime
 * of a call that setting belongs to the bridge, and a handler placed alongside this one must not
 * also drive it. The bound is soft: the check runs after an event is queued, and muting the socket
 * does not discard what the current read cycle already decoded.
 *
 * <p><b>Outbound flow control</b> is the mirror image, and is the transport's to drive rather than
 * the service's: {@code isReady()} reports whether the channel can take another message, and
 * {@code onReady} is delivered when Netty says it became writable again. Both are advisory. A
 * service is free to ignore them and keep calling {@code sendMessage}, at the price of buffering
 * inside the channel; the pair exists so that it does not have to.
 *
 * <p>One instance serves exactly one call: connect-java builds a fresh pipeline per call
 * (its {@code RoutingHandler} removes itself after routing, so the pipeline is single-use).
 *
 * <p><b>Threading:</b> the call state machine - {@link #state}, {@link #listener},
 * {@link #exchange} - is confined to the {@link GrpcCallExecutor}. Inbound pipeline events and the
 * {@code ServerCall} operations a service invokes from its own thread are both submitted there, so
 * the two directions are serialized against each other instead of racing. Which executor that is
 * comes from {@link ConnectGrpcBridge}: {@link NettyEventLoopCallExecutor} runs the call on the channel
 * event loop, {@link SerializingCallExecutor} runs it on an application executor.
 *
 * <p>Confinement to "the call executor" is therefore not confinement to one thread. The serializing
 * executor may run consecutive tasks on different threads; the plain fields above are safe because
 * it chains the happens-before edges between them, not because one thread owns them.
 *
 * <p>Writing to the channel is a second, separate hop, because {@code ctx.write} has to happen on
 * the event loop wherever the call executor runs. That hop needs no ordering latch of its own - it
 * is only ever reached from inside a call-executor task, and those are already serialized, so the
 * writes inherit their order. Do not "unify" the two hops: routing writes through the call
 * executor, or guarding them with a second latch, is what this arrangement exists to avoid.
 *
 * <p>Which of the two ways it reaches the loop - writing inline or posting a task - comes from
 * {@link GrpcCallExecutor#runsOnEventLoop()}, and is therefore the same for every write of a call.
 * That uniformity is the load-bearing part, not the choice itself: a posted write waits for the
 * loop's next task-draining phase ({@code SingleThreadIoEventLoop.run} alternates {@code runIo} with
 * {@code runAllTasks}), so an inline write issued after it would overtake it and put response
 * messages on the wire out of order. Deciding per write with {@code inEventLoop()} is what would
 * allow that mix, which is why the decision belongs to the executor.
 *
 * <p>Two fields sit outside that confinement, both because cancellation must not have to wait for
 * service code that may be blocked on the call executor:
 * <ul>
 *   <li>{@link #grpcContext} - {@code final}, so it is safely visible everywhere without a
 *       {@code volatile} read. Cancelling it belongs to {@link #cancelExecutor}.</li>
 *   <li>{@link #cancelled} - volatile, and backs {@code ServerCall.isCancelled()}, one of the two
 *       methods gRPC documents as safe to call concurrently. The other is {@code request(int)},
 *       which hops onto the call executor rather than reading anything.</li>
 * </ul>
 *
 * <p>Writability is a third piece of state outside the call executor, and the only one the handler
 * does not own: {@code isReady()} reads it straight off the channel instead of mirroring it into a
 * field, so there is nothing to keep in sync and nothing to go stale.
 *
 * <p>In direct mode this has a consequence worth knowing: listener callbacks, and therefore service
 * and interceptor code, run on the event loop, so blocking work there stalls the channel. Executor
 * mode exists to lift that restriction.
 */
final class GrpcBridgeHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(GrpcBridgeHandler.class);

    /** Defines when the handler is going to stop reading new messages from a socket */
    private static final int MESSAGE_Q_HIGH_WATERMARK = 128;
    /** Defines when the handler continues automatically reading new messages from a socket */
    private static final int MESSAGE_Q_LOW_WATERMARK = 32;

    /** Lifecycle of the single call this handler serves. */
    private enum State {
        /** Registered in the pipeline, waiting for the {@link ConnectCallExchange}. */
        AWAITING_CALL,
        /** Inside {@code startCall}: the listener does not exist yet. */
        STARTING_CALL,
        /** The listener exists and the call is in flight. */
        CALL_ACTIVE,
        /** The response is on the wire. Terminal. */
        CLOSED,
        /** The channel died before the call completed. Terminal. */
        CANCELLED;

        boolean isTerminal() {
            return this == CLOSED || this == CANCELLED;
        }
    }

    /**
     * Client-facing description for any failure thrown by service or interceptor code.
     *
     * <p>Deliberately constant: an exception message can carry internal detail - SQL fragments,
     * file paths, hostnames - that must not reach a client able to trigger the bug. gRPC makes
     * the same choice: {@code ServerImpl.internalClose} sends this fixed text and keeps the
     * throwable local via {@code Status.withCause}, which is never serialized. The throwable is
     * logged instead.
     */
    private static final String APPLICATION_ERROR = "Application error processing RPC";

    private final ConnectGrpcBridge bridge;

    /**
     * The call's cancellation scope, and the parent of whatever context the call attaches around
     * listener callbacks.
     *
     * <p>Built in the constructor rather than once the request headers are known, which is where
     * gRPC builds its own ({@code ServerImpl.createContext:645}). Being {@code final} is the
     * point: the field is read from the call executor and from {@link #cancelExecutor}, and a
     * field assigned later would need every one of those reads ordered against the assignment -
     * a race no {@code volatile} can close, because the cancel path could still read {@code null}
     * and silently drop the cancellation.
     *
     * <p>Deferring it buys nothing anyway. A deadline cannot be attached to an existing
     * {@code CancellableContext}, but it does not need to be attached to this one: once the
     * exchange arrives the call executor can derive a child with {@code withDeadline}, and
     * cancelling a parent cancels its children. This field stays the outer scope that the
     * transport side owns.
     *
     * <p>Derived from {@link Context#ROOT} rather than {@code Context.current()} so that a call
     * never inherits whatever happened to be attached to the thread that built the pipeline. gRPC
     * does the same with a root forked once per server ({@code ServerImpl:154}).
     */
    private final Context.CancellableContext grpcContext;

    private State state = State.AWAITING_CALL;
    private @Nullable ChannelHandlerContext ctx;
    private @Nullable ConnectCallExchange exchange;
    private ServerCall.@Nullable Listener<Object> listener;

    /**
     * Inbound events waiting for the service to ask for them, in arrival order.
     *
     * <p>Holds the end-of-stream marker as well as payloads, which is what keeps
     * {@code onHalfClose} behind every message that preceded it. A boolean would have to be both
     * set and cleared; a position in a queue cannot be delivered twice, because polling it removes
     * it. Call-executor-confined, like the rest of the state machine.
     *
     * <p>Sized for a full burst - {@link #MESSAGE_Q_HIGH_WATERMARK} payloads plus the marker - so
     * the common case never reallocates. That is a hint, not a bound: the watermark is checked
     * after the message is added, and disabling auto-read does not discard whatever the current
     * read cycle already decoded, so the queue can legitimately overshoot and grow.
     */
    private final Queue<ConnectMessage> messageQueue =
        new ArrayDeque<>(MESSAGE_Q_HIGH_WATERMARK + 1);

    /**
     * Messages the service has asked for and not yet been given, via {@code ServerCall.request(n)}.
     *
     * <p>Gates payloads only. The end-of-stream marker is ordered behind them but costs nothing,
     * because a service is never obliged to request anything to learn that the client is done -
     * gRPC decrements its own counter only for message bodies ({@code MessageDeframer:280}) and
     * fires half-close from {@code isStalled()} (`:300`), which means "nothing left to deliver"
     * rather than "demand available". Charge the marker and a service that requests exactly the
     * messages it expects never gets {@code onHalfClose}, and the call hangs.
     */
    private int demand;

    /**
     * True while {@link #flushMessages} is walking the queue.
     *
     * <p>A stub that calls {@code request(n)} from inside {@code onMessage} submits from the call
     * executor's own thread, and such a submission runs inline by contract - so without this the
     * delivery loop would re-enter once per queued message instead of unwinding, at roughly five
     * stack frames each. gRPC guards the same reentrancy with {@code inDelivery}
     * ({@code MessageDeframer.deliver():260-266}).
     */
    private boolean flushingMessages;

    /**
     * The channel's auto-read setting as this handler last left it.
     *
     * <p>The bridge owns that setting for the lifetime of the call - see the class javadoc.
     * Initialised to Netty's own default so the first watermark check compares against the truth.
     */
    private boolean autoRead = true;

    /**
     * The two executors are a pair, chosen together for the configured mode, and both are set either
     * in the constructor or in {@link #handlerAdded} - never later, and never one without the other.
     *
     * <p>{@code cancelExecutor} exists so that cancelling {@link #grpcContext} cannot end up queued
     * behind service code the call executor is blocked in. It is deliberately <em>not</em> the
     * serializing executor, which is exactly gRPC's arrangement: {@code ServerImpl:896} hands its
     * {@code ContextCloser} to the raw application executor with the comment "The callExecutor might
     * be busy doing user work. To avoid waiting, use an executor that is not serializing."
     *
     * <p>Which means it inherits gRPC's unstated requirement: the raw executor must have capacity to
     * spare while a call is in flight. Give the bridge a bounded pool with every thread inside
     * service code and cancellation is starved - the client is gone, and nothing tells the service.
     * gRPC's default executor is an unbounded cached pool for this reason
     * ({@code GrpcUtil.SHARED_CHANNEL_EXECUTOR}); the bridge's default is virtual threads. See
     * {@code ConnectGrpcBridge.Builder.withServiceExecutor}.
     *
     * <p>Nullable only because the direct-mode pair needs a {@code ChannelHandlerContext}, which
     * does not exist until the handler joins a pipeline. Both are non-null from then on.
     */
    private @Nullable GrpcCallExecutor callExecutor;

    /** Paired with {@link #callExecutor}; see there. */
    private @Nullable Executor cancelExecutor;

    /**
     * Backs {@code ServerCall.isCancelled()}: read from any thread. Written by the cancellation
     * listener below, on whichever thread cancels the context, and again on the call executor
     * right before {@code onCancel} is delivered.
     */
    private volatile boolean cancelled;

    /**
     * @param executor the application executor to run the call on, or {@code null} for direct mode,
     *                 where the call runs on the channel event loop
     */
    GrpcBridgeHandler(ConnectGrpcBridge bridge, @Nullable Executor executor) {
        this.bridge = bridge;
        this.grpcContext = Context.ROOT.withCancellation();

        // Executor mode: the serializer gives one call its ordering, the raw executor stays as the
        // cancellation path that must not queue behind it. See the callExecutor field.
        if (executor != null) {
            this.callExecutor = new SerializingCallExecutor(executor);
            this.cancelExecutor = executor;
        }

        // What tells a cancellation apart from a completion: cancelCall cancels with a cause,
        // notifyComplete cancels without one. Registered with a direct executor so the flag flips
        // on whichever thread called cancel(), instead of waiting for a call executor that may be
        // sitting in blocked service code. This is ServerCallImpl:298-311 verbatim.
        this.grpcContext.addListener(cancelledContext -> {
            if (cancelledContext.cancellationCause() != null) {
                cancelled = true;
            }
        }, Runnable::run);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        this.ctx = ctx;

        // Direct mode, deferred to here because it needs the channel's event loop. Its cancel
        // executor is direct: the job of a separate one is to keep cancellation out of the queue
        // behind blocked service code, and here cancelCall already runs on the event loop, so there
        // is nowhere better to move it. Posting would only push it behind the onCancel it has to
        // precede.
        if (callExecutor == null) {
            callExecutor = new NettyEventLoopCallExecutor(ctx.executor());
            cancelExecutor = Runnable::run;
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelCall();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        cancelCall();
        ctx.fireChannelInactive();
    }

    /**
     * Turns the channel becoming writable again into {@code Listener.onReady}.
     *
     * <p>Netty fires this on both edges, so the check is what makes it a false-to-true transition -
     * the flip to unwritable is what {@code isReady()} is for and needs no callback of its own. The
     * value is read here, on the event loop that owns it, rather than inside the task: writability
     * is transport state, and the notification it produces is allowed to be stale by the time the
     * listener runs.
     *
     * <p>The event keeps travelling either way. A handler downstream may care about writability
     * even when this call does not.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        assert callExecutor != null : "executor was initialized when the handler has been added to the chain";

        if (ctx.channel().isWritable()) {
            callExecutor.execute(this::notifyReady);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert callExecutor != null : "executor was initialized when the handler has been added to the chain";

        callExecutor.execute(() -> {
            switch (msg) {
                case ConnectCallExchange callExchange -> startCall(callExchange);
                case ConnectPayload payload -> enqueueMessage(payload);
                case ConnectEndOfStream endOfStream -> enqueueMessage(endOfStream);
                default -> ctx.fireChannelRead(msg);
            }
        });
    }

    private void startCall(ConnectCallExchange callExchange) {
        if (state != State.AWAITING_CALL) {
            failInternal("Received RPC request in state " + state);
            return;
        }
        state = State.STARTING_CALL;

        exchange = callExchange;
        String serviceName = exchange.serviceDefinition().serviceName();
        String methodName = exchange.methodDefinition().methodName();

        ServerMethodDefinition<?, ?> smd;
        try {
            smd = bridge.method(serviceName, methodName);
        } catch (IllegalArgumentException e) {
            // Nothing was started, so there is no listener to notify: reply and go terminal.
            state = State.CLOSED;
            writeTerminal(Status.UNIMPLEMENTED.withDescription(e.getMessage()), null);
            return;
        }

        @SuppressWarnings("unchecked")
        ServerCallHandler<Object, Object> callHandler =
            (ServerCallHandler<Object, Object>) smd.getServerCallHandler();
        @SuppressWarnings("unchecked")
        MethodDescriptor<Object, Object> md =
            (MethodDescriptor<Object, Object>) smd.getMethodDescriptor();

        Metadata metadata;
        try {
            metadata = MetadataMapping.extractMetadataFromConnectRequestHeaders(
                callExchange.requestMeta());
        } catch (IllegalArgumentException e) {
            // A -bin request header did not hold valid base64: a client protocol error,
            // reported before anything was started.
            state = State.CLOSED;
            writeTerminal(Status.INVALID_ARGUMENT.withDescription(
                "Malformed binary RPC request header: " + e.getMessage()), null);
            return;
        }

        assert ctx != null : "context was initialized when the handler has been added to the chain";
        assert callExecutor != null : "executor was initialized when the handler has been added to the chain";

        // Created only now, when the context and the descriptor are both known, so BridgeCall
        // can hold them as final fields rather than reading volatiles off the event loop.
        BridgeServerCall bridgeCall = new BridgeServerCall(ctx, callExecutor, md);

        Context previous = grpcContext.attach();
        try {
            listener = callHandler.startCall(bridgeCall, metadata);
        } catch (Throwable t) {
            if (!state.isTerminal()) {
                state = State.CLOSED;
                writeTerminal(Status.fromThrowable(t), null);
            }
            return;
        } finally {
            grpcContext.detach(previous);
        }

        // Interceptor closed the call from inside startCall without delegating to next.startCall() - the standard gRPC
        // rejection pattern, e.g. failed auth. In this case close() could not reach a listener that did not exist
        // yet, so deliver its onComplete now.
        if (state.isTerminal()) {
            notifyComplete();
            return;
        }

        state = State.CALL_ACTIVE;

        // Unconditional, exactly like gRPC's initial notification on stream allocation: it is a
        // suggestion, and one made while the channel happens to be saturated is a spurious
        // notification, which Listener.onReady explicitly allows.
        notifyReady();
    }

    /**
     * Delivers {@code onReady}, the transport's invitation to write more.
     *
     * <p>Deliberately does not re-check writability, although the channel may well have filled up
     * again while this task waited its turn. gRPC leaves the same window open by the same
     * construction: readiness is tested on the transport thread
     * ({@code AbstractStream.TransportState.notifyIfReady:375-387}), the notification is posted to
     * the application executor
     * ({@code ServerImpl.JumpToApplicationThreadServerStreamListener.onReady:147-169}), and the
     * listener that eventually runs checks only cancellation ({@code ServerCallImpl:389-397}).
     * {@code Listener.onReady} documents that window instead of closing it: "Because there is a
     * processing delay to deliver this notification, it is possible for concurrent writes to cause
     * {@code isReady() == false} within this callback. Handle 'spurious' notifications by checking
     * {@code isReady()}'s current value."
     *
     * <p>Re-checking would be correct - nothing would be lost, since every later transition to
     * writable fires its own event and posts its own task - but it could only narrow the window,
     * never close it: writability can flip again between the check and the callback. Filtering most
     * spurious notifications while looking like it filters all of them is the worse shape, and each
     * one it would remove costs a service a single {@code isReady()} read in the handler pattern
     * gRPC documents.
     */
    private void notifyReady() {
        if (state != State.CALL_ACTIVE) {
            return; // no listener yet, or the call is over and has nothing left to write
        }

        assert listener != null : "listener was initialized when the call has started";

        Context previous = grpcContext.attach();
        try {
            listener.onReady();
        } catch (Throwable t) {
            failInternal("onReady", t);
        } finally {
            grpcContext.detach(previous);
        }
    }

    private void enqueueMessage(ConnectMessage message) {
        if (state == State.AWAITING_CALL) {
            failInternal("RPC request hasn't been started");
            return;
        }
        if (state != State.CALL_ACTIVE) {
            return; // the call already finished; silently drop late inbound messages
        }

        assert ctx != null : "context was initialized when the handler has been added to the chain";

        messageQueue.add(message);
        flushMessages();
    }

    private void flushMessages() {
        if (state != State.CALL_ACTIVE) {
            return; // the call already finished; silently drop late inbound messages
        }

        assert listener != null : "listener was initialized when the call has started";

        // Recursive flush - just skip
        if (flushingMessages) {
            return;
        }

        Context previous = grpcContext.attach();
        flushingMessages = true;
        String callback = "onMessage";
        try {
            ConnectMessage message;
            while ((message = messageQueue.peek()) != null) {
                switch (message) {
                    case ConnectPayload(Object payload) -> {
                        // If there is no demand just stop flushing the queue
                        if (demand <= 0) {
                            return;
                        }
                        demand--;
                        messageQueue.poll();
                        listener.onMessage(payload);
                    }
                    case ConnectEndOfStream ignore -> {
                        callback = "onHalfClose";
                        messageQueue.poll();
                        listener.onHalfClose();
                    }
                    case ConnectCallExchange ignore -> dropUnexpected(message);
                    case ConnectError ignore -> dropUnexpected(message);
                }
            }
        } catch (Throwable t) {
            failInternal(callback, t);
        } finally {
            flushingMessages = false;
            grpcContext.detach(previous);

            manageAutoRead();
        }
    }

    /** Drops a message that cannot appear in the queue, without stranding the delivery loop. */
    private void dropUnexpected(ConnectMessage message) {
        log.error("Unexpected {} in the inbound message queue, dropping it",
            message.getClass().getSimpleName());
        messageQueue.poll();
    }

    /**
     * Applies the queue-depth watermark to the channel's auto-read setting.
     *
     * <p>Called on both edges - after a message is queued and after the delivery loop drains one -
     * because either alone deadlocks. Checked only on delivery, the watermark never fires while a
     * service has stopped requesting, which is the case it exists for. Checked only on arrival,
     * disabling auto-read removes the very events that would re-enable it, and the call stalls with
     * an empty queue and a muted socket.
     *
     * <p>Unlike every other channel operation in this handler, this one is <em>not</em> hopped onto
     * the event loop, and that is deliberate. {@code ctx.write} is neither thread-safe nor
     * order-safe from an arbitrary thread; {@code setAutoRead} is an atomic field flip whose two
     * side effects - {@code channel.read()} and {@code clearReadPending()} - post themselves onto
     * the loop already. Consecutive calls come from the serialized call executor, so they cannot
     * race each other either. Hopping would add a task per crossing and change nothing, including
     * the overshoot: reads stop a hand-off later either way, because Netty defers the effect
     * regardless of who asks.
     *
     * <p>{@code qSize} counts the end-of-stream marker along with payloads. One element either way
     * does not matter to a watermark; it is worth knowing when reading a debug log.
     */
    private void manageAutoRead() {
        assert ctx != null : "context was initialized when the handler has been added to the chain";

        int qSize = messageQueue.size();
        if (autoRead && qSize >= MESSAGE_Q_HIGH_WATERMARK) {
            if (log.isDebugEnabled()) {
                log.debug("High watermark for inbound message queue reached. Disabling auto-read from a socket.");
            }
            ctx.channel().config().setAutoRead(false);
            autoRead = false;
        } else if (!autoRead && qSize <= MESSAGE_Q_LOW_WATERMARK) {
            if (log.isDebugEnabled()) {
                log.debug("Low watermark for inbound message queue reached. Enabling auto-read from a socket.");
            }
            ctx.channel().config().setAutoRead(true);
            autoRead = true;
        }
    }

    /**
     * Delivers {@code onComplete} and cancels the call context. Mutually exclusive with
     * {@link #cancelCall}: the {@code Listener} contract requires exactly one of
     * {@code onComplete}/{@code onCancel}, and the state machine enforces that.
     */
    private void notifyComplete() {
        assert state != State.AWAITING_CALL;
        assert listener != null : "listener was initialized when the call has started";

        Context previous = grpcContext.attach();
        try {
            listener.onComplete();
        } catch (Throwable t) {
            // The terminal message is already on the wire, so the client cannot be told. gRPC
            // does not report this either: ServerImpl leaves the onComplete/onCancel dispatch
            // uncaught and its SerializingExecutor logs whatever escapes.
            log.warn("Uncaught exception from onComplete", t);
        } finally {
            // The absent cause is load-bearing, not an omission: it is what marks this a
            // completion rather than a cancellation. The listener registered in the constructor
            // raises the cancelled flag only for a cause-bearing cancel, and channel teardown
            // afterwards calls cancel() again - which finds the context already cancelled and
            // does nothing. Pass a cause here and every completed call reports isCancelled().
            grpcContext.detachAndCancel(previous, null);
        }
    }

    /**
     * Terminates the call with an {@code internal} Connect error.
     *
     * <p>The bridge never lets a failure escape into the Netty pipeline: a contract violation by
     * a service or interceptor, or an exception thrown out of a listener callback, is reported to
     * the client the same way gRPC reports one - as a terminal error on the call - instead of
     * surfacing as an exception with no meaningful handler above it.
     */
    private void failInternal(String message) {
        if (state.isTerminal()) {
            return;
        }
        State previousState = state;
        state = State.CLOSED;
        writeTerminal(Status.INTERNAL.withDescription(message), null);
        if (previousState == State.CALL_ACTIVE) {
            notifyComplete();
        }
    }

    /**
     * Terminates the call after service or interceptor code threw out of {@code callback}.
     *
     * <p>The throwable is logged rather than reported: see {@link #APPLICATION_ERROR} for why the
     * client is told nothing beyond a fixed description.
     */
    private void failInternal(String callback, Throwable t) {
        log.warn("Uncaught exception from {}, closing the call", callback, t);
        failInternal(APPLICATION_ERROR);
    }

    /**
     * Aborts the call when the channel goes away before the response was written.
     *
     * <p>The work splits across the two executors, following {@code ServerImpl.closedInternal}
     * (:881-917). Cancelling the context is what unblocks anyone waiting on it, so it must not
     * queue behind service code the call executor may be stuck in; deciding whether
     * {@code onCancel} is owed is a state-machine question and belongs on the call executor like
     * every other one.
     *
     * <p>Called for every call, not only cancelled ones: {@code handlerRemoved} fires on normal
     * teardown too. Both halves are no-ops in that case - the context was already cancelled by
     * {@link #notifyComplete} and the state is already terminal.
     */
    private void cancelCall() {
        assert callExecutor != null : "executor was initialized when the handler has been added to the chain";
        assert cancelExecutor != null : "executor was initialized when the handler has been added to the chain";

        cancelExecutor.execute(() -> grpcContext.cancel(Status.CANCELLED.asRuntimeException()));

        callExecutor.execute(() -> {
            if (state == State.AWAITING_CALL || state.isTerminal()) {
                return;
            }
            state = State.CANCELLED;

            // Set here as well as by the context's cancellation listener, because nothing orders
            // the two executors against each other: as soon as the cancel executor stops being
            // the event loop, the cancel above and this task are independent submissions and
            // either may win. A callback that inspects isCancelled() before starting cleanup has
            // to see true. gRPC writes the flag twice for the same reason
            // (ServerCallImpl.closedInternal:374).
            cancelled = true;

            assert listener != null : "listener was initialized when the call has started";

            Context previous = grpcContext.attach();
            try {
                listener.onCancel();
            } catch (Throwable t) {
                // The channel is already gone, so there is nobody left to report to.
                log.warn("Uncaught exception from onCancel", t);
            } finally {
                // Detach without cancelling: the cancel executor above owns the cancellation,
                // and it carries the cause that marks this a cancellation rather than a
                // completion - unlike notifyComplete, which cancels with none.
                grpcContext.detach(previous);
            }
        });
    }

    /**
     * Writes the terminal Connect message for {@code status} and applies gRPC trailers.
     */
    private void writeTerminal(Status status, @Nullable Metadata trailers) {
        assert ctx != null : "context was initialized when the handler has been added to the chain";
        assert callExecutor != null : "executor was initialized when the handler has been added to the chain";

        //noinspection resource
        EventExecutor eventExecutor = ctx.executor();
        if (callExecutor.runsOnEventLoop()) {
            // Not a condition, an invariant: this is reached from inside a call-executor task, and
            // such an executor runs those on the loop. Posting instead of failing would silently mix
            // inline and posted writes, which is the bug this arrangement prevents.
            assert eventExecutor.inEventLoop() : "an executor that runs on the event loop ran a task off it";
            writeTerminalMessageOnEventLoop(status, trailers);
        } else {
            eventExecutor.execute(() -> writeTerminalMessageOnEventLoop(status, trailers));
        }
    }

    private void writeTerminalMessageOnEventLoop(Status status, @Nullable Metadata trailers) {
        assert ctx != null : "context was initialized when the handler has been added to the chain";

        // No exchange yet is a reachable state, not a broken invariant: a payload or an
        // end-of-stream arriving before the exchange is rejected by failInternal, which lands
        // here with nothing to apply trailers to.
        ConnectCallExchange callExchange = exchange;
        if (callExchange != null && trailers != null) {
            MetadataMapping.putMetadataToConnectResponseTrailers(
                trailers, callExchange.responseTrailersBuilder());
        }

        // The terminal message flushes, individual payloads do not: connect-java flushes every
        // payload it writes, but nothing downstream is obliged to flush on its own, and this is
        // the point where the response is complete. It also flushes whatever payloads are still
        // sitting in the outbound buffer.
        if (status.isOk()) {
            ctx.writeAndFlush(ConnectEndOfStream.INSTANCE);
            return;
        }
        ConnectErrorCode code = StatusMapping.toConnect(status.getCode());
        String message = status.getDescription() != null ? status.getDescription() : "";
        ctx.writeAndFlush(new ConnectError(code, message, extractDetails(status, trailers)));
    }

    private void applyHeadersOnEventLoop(Metadata headers) {
        // Unlike writeTerminalMessageOnEventLoop, this is only reachable through BridgeServerCall,
        // which does not exist until startCall has assigned the exchange.
        assert exchange != null : "exchange was initialized when the call has started";

        MetadataMapping.putMetadataToConnectResponseHeaders(
            headers, exchange.responseHeadersBuilder());
    }

    private void writeMessageOnEventLoop(Object message) {
        assert ctx != null : "context was initialized when the handler has been added to the chain";

        ctx.write(new ConnectPayload(message));
    }

    /**
     * Translates gRPC's {@code grpc-status-details-bin} trailer into Connect error details.
     *
     * <p>The trailer itself is not forwarded:
     * {@link MetadataMapping#putMetadataToConnectResponseTrailers} treats it as a reserved
     * protocol header, and Connect carries structured details in the error's {@code details}
     * field rather than in a gRPC-specific trailer, so forwarding both would duplicate the
     * same payload.
     */
    private static List<ConnectErrorDetail> extractDetails(
        Status status, @Nullable Metadata trailers)
    {
        if (trailers == null) {
            return List.of();
        }
        com.google.rpc.Status rpcStatus;
        try {
            rpcStatus = StatusProto.fromStatusAndTrailers(status, trailers);
        } catch (IllegalArgumentException e) {
            // The embedded proto status code disagrees with the gRPC status code.
            return List.of();
        }
        List<Any> anyList = rpcStatus.getDetailsList();
        if (anyList.isEmpty()) {
            return List.of();
        }
        List<ConnectErrorDetail> details = new ArrayList<>(anyList.size());
        for (Any any : anyList) {
            details.add(new ConnectErrorDetail(
                typeNameFromUrl(any.getTypeUrl()),
                any.getValue().toByteArray()));
        }
        return List.copyOf(details);
    }

    private static String typeNameFromUrl(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash >= 0 ? typeUrl.substring(slash + 1) : typeUrl;
    }

    /**
     * The {@link ServerCall} handed to the gRPC service.
     *
     * <p>Holds no call state of its own beyond the two flags gRPC requires it to validate before
     * mutating anything; everything else forwards into the enclosing handler's state machine.
     * Being created only once the executors and the descriptor are known lets them all be
     * {@code final}, which publishes them safely to whatever thread the service runs on without a
     * {@code volatile} read.
     *
     * <p>Each method hops onto the call executor before touching state, and the write it produces
     * hops from there onto the event loop - see the threading notes on the enclosing class.
     */
    private final class BridgeServerCall extends ServerCall<Object, Object> {
        private final GrpcCallExecutor callExecutor;
        private final EventExecutor eventLoopExecutor;
        private final Channel channel;
        private final MethodDescriptor<Object, Object> methodDescriptor;

        private boolean sendHeadersCalled;
        private boolean closeCalled;

        BridgeServerCall(
            ChannelHandlerContext ctx, GrpcCallExecutor callExecutor,
            MethodDescriptor<Object, Object> methodDescriptor)
        {
            this.callExecutor = callExecutor;
            this.eventLoopExecutor = ctx.executor();
            this.channel = ctx.channel();
            this.methodDescriptor = methodDescriptor;
        }

        @Override
        public void request(int numMessages) {
            // Validated on the caller's thread, like the IllegalStateException checks below and
            // like gRPC, which rejects it in MessageDeframer.request:157 rather than on whatever
            // thread ends up delivering.
            if (numMessages <= 0) {
                throw new IllegalArgumentException("demand must be a positive integer");
            }

            callExecutor.execute(() -> {
                // Saturating, because gRPC lets a service ask for Integer.MAX_VALUE and two such
                // calls would otherwise wrap to a negative demand, silently stopping delivery for
                // the rest of the call.
                demand = (int) Math.min((long) demand + numMessages, Integer.MAX_VALUE);
                flushMessages();
            });
        }

        @Override
        public void sendHeaders(Metadata headers) {
            if (sendHeadersCalled) {
                throw new IllegalStateException("sendHeaders has already been called");
            }
            if (closeCalled) {
                throw new IllegalStateException("call is closed");
            }
            // Set only once both checks pass: a throwing ServerCall method must leave the call
            // untouched, as gRPC's ServerCallImpl does by validating before any mutation.
            sendHeadersCalled = true;

            callExecutor.execute(() -> {
                if (state.isTerminal()) {
                    // The call already produced its terminal message. A second one must not be written,
                    // so there is no way to report this - and if the call was cancelled, the service is
                    // simply racing a client that already went away, which is not an error at all.
                    return;
                }

                if (callExecutor.runsOnEventLoop()) {
                    assert eventLoopExecutor.inEventLoop() : "an executor that runs on the event loop ran a task off it";
                    applyHeadersOnEventLoop(headers);
                } else {
                    eventLoopExecutor.execute(() -> applyHeadersOnEventLoop(headers));
                }
            });
        }

        @Override
        public void sendMessage(Object message) {
            if (!sendHeadersCalled) {
                throw new IllegalStateException("sendHeaders has not been called");
            }
            if (closeCalled) {
                throw new IllegalStateException("call is closed");
            }

            callExecutor.execute(() -> {
                if (state.isTerminal()) {
                    return; // see sendHeaders
                }

                if (callExecutor.runsOnEventLoop()) {
                    assert eventLoopExecutor.inEventLoop() : "an executor that runs on the event loop ran a task off it";
                    writeMessageOnEventLoop(message);
                } else {
                    eventLoopExecutor.execute(() -> writeMessageOnEventLoop(message));
                }
            });
        }

        @Override
        public void close(Status status, Metadata trailers) {
            if (closeCalled) {
                throw new IllegalStateException("call already closed");
            }
            closeCalled = true;

            callExecutor.execute(() -> {
                if (state.isTerminal()) {
                    return;
                }
                State previousState = state;
                state = State.CLOSED;
                writeTerminal(status, trailers);

                if (previousState == State.CALL_ACTIVE) {
                    notifyComplete();
                }
            });
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        /**
         * Whether the channel can take another message without piling one up behind it.
         *
         * <p>Reads Netty's writability rather than mirroring it into a field of its own: it is a
         * volatile read on {@code ChannelOutboundBuffer}, safe from any thread, and it cannot go
         * stale between the transition and this call the way a copy would. A closed call is never
         * ready even while the channel still is, which is {@code ServerCallImpl.isReady:202-207}.
         *
         * <p>{@code closeCalled} is read here without synchronisation, as gRPC reads its own. The
         * only thread that can see it stale is one racing its own {@code close()}, and
         * {@code ServerCall} forbids that: "the caller is free to call an instance from multiple
         * threads, but only one call simultaneously".
         */
        @Override
        public boolean isReady() {
            return !closeCalled && channel.isWritable();
        }

        @Override
        public MethodDescriptor<Object, Object> getMethodDescriptor() {
            return methodDescriptor;
        }
    }
}
