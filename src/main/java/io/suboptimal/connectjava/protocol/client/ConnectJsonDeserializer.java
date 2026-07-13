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
 *   <li>Unary error responses — parsed by {@link #parseUnaryError(byte[])} from the HTTP response
 *       body when the server returns a non-200 status. Returns raw {@link ConnectErrorBody} rather
 *       than a resolved {@link ConnectError} because a unary error has two sources for the code:
 *       the JSON body <em>or</em> an HTTP-status fallback. The caller resolves the code and applies
 *       the fallback.</li>
 *   <li>Streaming EndStreamResponse envelopes — parsed by {@link #parseStreamError(byte[])}
 *       and {@link #parseStreamMetadata(byte[])} from the final framed envelope with flag
 *       {@code 0x02}. The stream has exactly one source (the {@code EndStreamResponse} envelope),
 *       so the method returns a fully resolved {@link ConnectError} directly; {@code null} means
 *       successful completion.</li>
 * </ul>
 *
 * <p>The default implementation is {@link ConnectStringBuilderJsonDeserializer#INSTANCE}.
 * A custom implementation can be supplied via
 * {@link io.suboptimal.connectjava.protocol.client.ConnectClientProtocolConfig.Builder#jsonDeserializer(ConnectJsonDeserializer)}.
 */
public interface ConnectJsonDeserializer {

    /**
     * Parses the structured fields of a Connect unary error body without resolving the code.
     *
     * <p>The caller applies its own fallback logic for the error code (e.g. fall back to the
     * HTTP-to-Connect mapping when the code is absent or unrecognised). This is intentional:
     * unary responses have two sources for the error code — the JSON body <em>or</em> the HTTP
     * status — and only the handler has both.
     *
     * @param body UTF-8-encoded JSON bytes
     * @return parsed body fields, or {@code null} if the body does not look like a Connect error
     */
    @Nullable ConnectErrorBody parseUnaryError(byte[] body);

    /**
     * Parses the error field from a Connect streaming EndStreamResponse JSON body.
     *
     * <p>Expected input: {@code {"error":{"code":"not_found","message":"...","details":[...]}}}
     * for an error, or {@code {}} / {@code {"metadata":{...}}} for a successful completion.
     * An unrecognised error code is treated as {@code unknown} rather than as a missing error.
     *
     * <p>Returns a fully resolved {@link ConnectError} (not raw fields) because the streaming
     * EndStreamResponse envelope is the sole source of the error code — no HTTP-status fallback
     * is needed.
     *
     * @param body UTF-8-encoded JSON bytes
     * @return parsed error, or {@code null} if no error is present (successful completion)
     */
    @Nullable ConnectError parseStreamError(byte[] body);

    /**
     * Parses trailing metadata from a Connect streaming EndStreamResponse JSON body.
     *
     * <p>Extracts the top-level {@code "metadata"} field. Keys are returned as on the wire
     * (no {@code trailer-} prefix is added or stripped).
     *
     * @param body UTF-8-encoded JSON bytes
     * @return trailing metadata map; empty if the {@code metadata} field is absent
     */
    Map<String, List<String>> parseStreamMetadata(byte[] body);
}
