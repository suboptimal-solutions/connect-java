package io.suboptimal.connectjava.grpcbridge;

import io.grpc.BindableService;
import io.netty.channel.embedded.EmbeddedChannel;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * A bridged service on an {@link EmbeddedChannel}, driven the same way in either
 * {@link ExecutorMode}.
 *
 * <p>What differs between the modes is <em>when</em> work happens, so every method that hands
 * something to the bridge also lets the call catch up before returning. In {@link
 * ExecutorMode#DIRECT} there is nothing to catch up on - the call ran inside {@code writeInbound}.
 * In {@link ExecutorMode#EXECUTOR} nothing has run at all: the inbound event sits in the manual
 * executor, and the writes it will eventually produce are posted to the event loop rather than made
 * inline, so both queues have to be drained. Settling after each message reproduces the direct
 * mode's rhythm, which is what makes a shared assertion meaningful.
 *
 * <p>A test that wants to observe a call mid-flight - to deliver a cancellation while its executor
 * still holds work, say - reaches past this class and drives {@link #channel} and its executor
 * itself.
 */
final class GrpcCallHarness {
    final ConnectGrpcBridge bridge;
    final EmbeddedChannel channel;

    /** {@code null} in direct mode, where there is no executor to drain. */
    private final @Nullable ManualExecutor executor;

    GrpcCallHarness(BindableService service, ExecutorMode mode) {
        ConnectGrpcBridge.Builder builder = ConnectGrpcBridge.builder().addService(service);
        this.executor = mode == ExecutorMode.EXECUTOR ? new ManualExecutor() : null;
        this.bridge = executor != null
            ? builder.withServiceExecutor(executor).build()
            : builder.withDirectExecutor().build();
        this.channel = new EmbeddedChannel(bridge.create());
    }

    /** The exchange that starts {@code methodName} on this harness's bridge, with no headers. */
    ConnectCallExchange exchange(String methodName) {
        return TestExchanges.create(bridge, methodName, Map.of());
    }

    ConnectCallExchange exchange(String methodName, Map<String, List<String>> headers) {
        return TestExchanges.create(bridge, methodName, headers);
    }

    /** For a test that has to hold on to the builders it will assert against afterwards. */
    ConnectCallExchange exchange(
        String methodName,
        ConnectResponseHeadersBuilder headersBuilder,
        ConnectResponseTrailersBuilder trailersBuilder)
    {
        return TestExchanges.create(bridge, methodName, Map.of(), headersBuilder, trailersBuilder);
    }

    /** Feeds one inbound message, then lets the call executor and the event loop catch up. */
    void writeInbound(Object message) {
        channel.writeInbound(message);
        settle();
    }

    /** Closes the channel, then lets the cancellation that follows run. */
    void close() {
        channel.close();
        settle();
    }

    <T> @Nullable T readOutbound() {
        return channel.readOutbound();
    }

    /**
     * Lets the call catch up after something other than an inbound message moved it - a service
     * calling {@code ServerCall.request(n)} from the test's own thread, typically. A no-op in
     * direct mode, where that call already ran to completion.
     */
    void settle() {
        if (executor != null) {
            ExecutorUtil.settleAllTasks(channel, executor);
        }
    }
}
