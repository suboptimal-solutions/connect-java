package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;

/**
 * Immutable view of a Connect RPC call passed to each {@link io.suboptimal.connectjava.protocol.ConnectInterceptor}.
 *
 * <p>{@link #responseHeadersBuilder()} accepts mutations until the first response payload (or the
 * terminal response for unary calls) is written; after that point any mutation throws
 * {@link IllegalStateException}.
 *
 * <p>{@link #responseTrailersBuilder()} accepts mutations until the terminal response is written;
 * after that point any mutation throws {@link IllegalStateException}.
 */
public record ConnectCallExchange(
    ConnectServiceDefinition serviceDefinition,
    ConnectMethodDefinition methodDefinition,
    ConnectRequestMeta requestMeta,
    ConnectResponseHeadersBuilder responseHeadersBuilder,
    ConnectResponseTrailersBuilder responseTrailersBuilder) implements ConnectMessage
{}
