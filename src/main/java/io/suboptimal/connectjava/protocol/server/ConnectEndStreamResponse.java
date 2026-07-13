package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.api.ConnectError;
import org.jspecify.annotations.Nullable;

/**
 * Payload of a Connect streaming EndStreamResponse envelope.
 *
 * @param error    optional Connect error; {@code null} on a successful stream completion
 * @param metadata trailer metadata to include in the {@code "metadata"} object;
 *                 omitted from the serialized output when empty
 */
public record ConnectEndStreamResponse(@Nullable ConnectError error, ConnectEndStreamMeta metadata) {}
