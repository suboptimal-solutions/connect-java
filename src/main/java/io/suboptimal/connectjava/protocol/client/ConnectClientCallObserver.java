package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectError;
import org.jspecify.annotations.Nullable;

/**
 * Stateful observer for one client-side Connect RPC call, attached by a
 * {@link ConnectClientInterceptor} via {@link ConnectClientInterceptor#continueWith}.
 *
 * <p><strong>Direction differs from the server observer.</strong> On the client the {@code onRequest*}
 * callbacks are <em>outbound</em> — they fire as the client <em>sends</em> the request — whereas the
 * server-side {@code io.suboptimal.connectjava.protocol.ConnectCallObserver} treats them as inbound.
 * Likewise {@link #onResponseHeaders} here delivers an immutable {@link ConnectResponseMeta} that the
 * client <em>received</em> and can only read; the server observer instead receives a mutable builder it
 * can use to shape the response it emits.
 *
 * <p>Request callbacks are invoked in interceptor registration order (FIFO); response-header and
 * completion callbacks are invoked in reverse registration order (LIFO) so outer interceptors observe
 * the response last. Observer exceptions are not swallowed: they propagate through the Netty pipeline
 * like other user-code failures.
 *
 * <p>If the interceptor returned a {@code Continue} decision, the pipeline guarantees exactly one
 * {@link #onCallComplete} for the lifetime of the call, regardless of success, failure, or
 * cancellation.
 */
public interface ConnectClientCallObserver {
    /**
     * Called for each outbound request payload as it is encoded and sent.
     *
     * <p>Invoked in interceptor registration order (FIFO).
     */
    default void onRequestPayload(Object payload) {}

    /**
     * Called once after the last request payload has been sent and the request is fully flushed.
     *
     * <p>Invoked in interceptor registration order (FIFO).
     */
    default void onRequestFinished() {}

    /**
     * Called once when the response headers arrive, before any response payload is delivered.
     *
     * <p>{@code meta} is an immutable, read-only view of the received leading metadata; it cannot be
     * mutated. Invoked in reverse interceptor registration order (LIFO).
     */
    default void onResponseHeaders(ConnectResponseMeta meta) {}

    /**
     * Called for each successfully decoded response payload.
     *
     * <p>Invoked in interceptor registration order (FIFO).
     */
    default void onResponsePayload(Object payload) {}

    /**
     * Called exactly once when the call terminates.
     *
     * <p>{@code error} is {@code null} for a successful RPC and non-null for a failed or cancelled
     * one. Invoked in reverse interceptor registration order (LIFO).
     */
    default void onCallComplete(@Nullable ConnectError error) {}

    /** No-op implementation of the client call observer. */
    ConnectClientCallObserver NOOP = new ConnectClientCallObserver() {};
}
