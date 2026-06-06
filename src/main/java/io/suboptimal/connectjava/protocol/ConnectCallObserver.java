package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import org.jspecify.annotations.Nullable;

/**
 * Stateful observer for one Connect RPC call.
 *
 * <p>{@code onResponsePayload} is invoked in interceptor registration order. Header,
 * trailer, and completion callbacks are invoked in reverse registration order so outer
 * interceptors see the final metadata state last. Observer exceptions are not swallowed:
 * they propagate through the Netty pipeline like other user-code failures.
 */
public interface ConnectCallObserver {
    ConnectCallObserver NOOP = new NoOpConnectCallObserver();

    // Inbound (FIFO)

    /**
     * Called for each decoded inbound request payload before the corresponding
     * {@link ConnectPayload} is forwarded to the terminal handler.
     *
     * <p>Invoked in interceptor registration order (FIFO). Not called if the call was rejected
     * before {@link ConnectInterceptor#interceptCall} returned.
     */
    default void onRequestPayload(Object payload) {}

    /**
     * Called after the last inbound request payload has been delivered and before
     * {@link ConnectEndOfStream} is forwarded to the terminal handler.
     *
     * <p>Invoked in interceptor registration order (FIFO). Not called if the call was rejected
     * before {@link ConnectInterceptor#interceptCall} returned.
     */
    default void onRequestFinished() {}

    // Outbound (LIFO)

    /**
     * Called once before Connect response headers are written.
     */
    default void onResponseHeaders(ConnectResponseHeadersBuilder headers) {}

    /**
     * Called for each successfully encoded response payload before it is written.
     */
    default void onResponsePayload(Object payload) {}

    /**
     * Called once before terminal Connect trailers are serialized.
     *
     * <p>{@code error} is {@code null} for a successful RPC and non-null for an error response.
     */
    default void onResponseTrailers(ConnectResponseTrailersBuilder trailers, @Nullable ConnectError error) {}

    /**
     * Called after the terminal Connect response write completes successfully.
     */
    default void onCallComplete(@Nullable ConnectError error) {}

    /**
     * No-op implementation of the call observer.
     */
    record NoOpConnectCallObserver() implements ConnectCallObserver {}
}
