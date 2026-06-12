package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.api.ConnectError;

/**
 * SPI for serializing Connect protocol JSON bodies to bytes.
 *
 * <p>Two call sites exist in the Connect implementation:
 * <ul>
 *   <li>Unary error responses — produced by {@link #error(ConnectError)} and written as the
 *       HTTP response body with an appropriate Connect error HTTP status code.</li>
 *   <li>Streaming EndStreamResponse envelopes — produced by
 *       {@link #endStream(ConnectEndStreamResponse)} and written as the final framed envelope
 *       with flag {@code 0x02}.</li>
 * </ul>
 *
 * <p>The default implementation is {@link ConnectStringBuilderJsonSerializer#INSTANCE}.
 * A custom implementation (e.g. Jackson-based) can be supplied via
 * {@link ConnectServerProtocolConfig.Builder#jsonSerializer(ConnectJsonSerializer)}.
 */
public interface ConnectJsonSerializer {
    /**
     * Serializes a Connect error to a UTF-8 JSON body.
     *
     * <p>Example output: {@code {"code":"unimplemented","message":"not supported"}}
     *
     * @param error the Connect error to serialize
     * @return UTF-8-encoded JSON bytes
     */
    byte[] error(ConnectError error);

    /**
     * Serializes a Connect EndStreamResponse to a UTF-8 JSON body.
     *
     * <p>Example output for a successful completion with trailer metadata:
     * {@code {"metadata":{"grpc-status":["0"]}}}
     *
     * @param message the end-stream response to serialize
     * @return UTF-8-encoded JSON bytes
     */
    byte[] endStream(ConnectEndStreamResponse message);
}
