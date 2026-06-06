package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.protocol.ConnectCallObserver;

/**
 * Mutable Connect response trailers collected by {@link ConnectCallObserver}s.
 *
 * <p>For unary RPCs, trailers are serialized as {@code Trailer-*} response headers. For
 * streaming RPCs, trailers are serialized as the {@code metadata} object in the
 * EndStreamResponse envelope.
 *
 * <p>Not thread-safe. Mutations must be made from the Netty event loop thread.
 * After the terminal response has been written, any mutation throws {@link IllegalStateException}.
 */
public interface ConnectResponseTrailersBuilder {
    /**
     * Adds a trailer with the given name and value. If a trailer with the same name already
     * exists, this adds an additional value rather than replacing existing ones.
     *
     * @throws IllegalStateException if the terminal response has already been written
     */
    ConnectResponseTrailersBuilder add(CharSequence name, CharSequence value);

    /**
     * Sets a trailer, replacing any existing values for the given name.
     *
     * @throws IllegalStateException if the terminal response has already been written
     */
    ConnectResponseTrailersBuilder set(CharSequence name, CharSequence value);
}
