package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * SPI for deserializing Connect protocol JSON bodies from bytes.
 *
 * <p>Two call sites exist in the client Connect implementation:
 * <ul>
 *   <li>Unary error responses — parsed by {@link #parseError(byte[])} from the HTTP response
 *       body when the server returns a non-200 status.</li>
 *   <li>Streaming EndStreamResponse envelopes — parsed by {@link #parseEndStreamError(byte[])}
 *       and {@link #parseEndStreamMetadata(byte[])} from the final framed envelope with
 *       flag {@code 0x02}.</li>
 * </ul>
 *
 * <p>The default implementation is {@link ConnectStringBuilderJsonDeserializer#INSTANCE}.
 * A custom implementation can be supplied via
 * {@link io.suboptimal.connectjava.protocol.client.ConnectClientProtocolConfig.Builder#jsonDeserializer(ConnectJsonDeserializer)}.
 */
public interface ConnectJsonDeserializer {

    /**
     * Parses a Connect error from a UTF-8 JSON body.
     *
     * <p>Expected input: {@code {"code":"not_found","message":"...","details":[...]}}
     *
     * @param body UTF-8-encoded JSON bytes
     * @return parsed error, or {@code null} if the body cannot be parsed as a Connect error
     */
    @Nullable ConnectError parseError(byte[] body);

    /**
     * Parses the error field from a Connect streaming EndStreamResponse JSON body.
     *
     * <p>Expected input: {@code {"error":{"code":"not_found","message":"...","details":[...]}}}
     * for an error, or {@code {}} / {@code {"metadata":{...}}} for a successful completion.
     * An unrecognized error code is treated as {@code unknown} rather than as a missing error.
     *
     * @param body UTF-8-encoded JSON bytes
     * @return parsed error, or {@code null} if no error is present (successful completion)
     */
    @Nullable ConnectError parseEndStreamError(byte[] body);

    /**
     * Parses trailing metadata from a Connect streaming EndStreamResponse JSON body.
     *
     * <p>Extracts the top-level {@code "metadata"} field. Keys are returned as on the wire
     * (no {@code trailer-} prefix is added or stripped).
     *
     * @param body UTF-8-encoded JSON bytes
     * @return trailing metadata map; empty if the {@code metadata} field is absent
     */
    Map<String, List<String>> parseEndStreamMetadata(byte[] body);

    /**
     * Parses the structured fields of a Connect unary error body without resolving the code.
     *
     * <p>Use this when the caller needs to apply its own fallback logic for the error code
     * (e.g. fall back to the HTTP-to-Connect mapping when the code is absent or unrecognized).
     *
     * @param body UTF-8-encoded JSON bytes
     * @return parsed body fields, or {@code null} if the body does not look like a Connect error
     */
    @Nullable ConnectErrorBody parseErrorBody(byte[] body);
}
