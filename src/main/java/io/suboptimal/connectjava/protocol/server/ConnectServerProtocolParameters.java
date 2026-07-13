package io.suboptimal.connectjava.protocol.server;

/**
 * Configures request size limits and optional CORS policy for {@link ConnectServerProtocol}.
 *
 * @param maxRequestBytes maximum aggregated request body size for unary calls
 * @param maxFrameBytes   maximum payload size of a single server-streaming Connect envelope,
 *                        not counting the 5-byte envelope prefix; envelopes whose declared
 *                        length exceeds this limit are rejected with a streaming error
 * @param cors            CORS policy; use {@link ConnectCorsParameters#disabled()} to opt out
 */
public record ConnectServerProtocolParameters(int maxRequestBytes, int maxFrameBytes, ConnectCorsParameters cors) {
    public ConnectServerProtocolParameters {
        if (maxRequestBytes <= 0) {
            throw new IllegalArgumentException("maxRequestBytes must be positive");
        }
        if (maxFrameBytes <= 0) {
            throw new IllegalArgumentException("maxFrameBytes must be positive");
        }
    }

    /** Convenience constructor with CORS disabled. */
    public ConnectServerProtocolParameters(int maxRequestBytes, int maxFrameBytes) {
        this(maxRequestBytes, maxFrameBytes, ConnectCorsParameters.disabled());
    }
}
