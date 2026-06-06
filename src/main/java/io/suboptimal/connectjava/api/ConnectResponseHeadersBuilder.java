package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.protocol.ConnectCallObserver;

/**
 * Mutable Connect response headers collected by {@link ConnectCallObserver}s.
 *
 * <p>Mutations are applied to the wire response after all header observers run. Operation
 * order is preserved, including the difference between {@link #set(CharSequence, CharSequence)}
 * and {@link #add(CharSequence, CharSequence)}.
 *
 * <p>Not thread-safe. Mutations must be made from the Netty event loop thread.
 * After the response headers have been written, any mutation throws {@link IllegalStateException}.
 */
public interface ConnectResponseHeadersBuilder {
    /**
     * Adds a header with the given name and value. If a header with the same name already exists,
     * this adds an additional value rather than replacing existing ones.
     *
     * @throws IllegalStateException if the response headers have already been written
     */
    ConnectResponseHeadersBuilder add(CharSequence name, CharSequence value) throws IllegalStateException;

    /**
     * Sets a header, replacing any existing values for the given name.
     *
     * @throws IllegalStateException if the response headers have already been written
     */
    ConnectResponseHeadersBuilder set(CharSequence name, CharSequence value) throws IllegalStateException;
}
