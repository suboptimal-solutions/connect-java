package io.suboptimal.connectjava.api;

/**
 * A single rich error detail attached to a {@link ConnectError}.
 *
 * @param type  fully-qualified Protobuf message name (e.g. {@code google.rpc.RetryInfo})
 * @param value serialized Protobuf bytes of the detail message
 */
public record ConnectErrorDetail(String type, byte[] value) {}
