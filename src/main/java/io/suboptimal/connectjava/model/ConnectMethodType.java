package io.suboptimal.connectjava.model;

/**
 * Streaming cardinality of a Connect method.
 */
public enum ConnectMethodType {
    /** Single request, single response. */
    UNARY,
    /** Single request, stream of response frames. */
    SERVER_STREAMING,
    /** Stream of request frames, single response. */
    CLIENT_STREAMING,
    /** Stream of request frames, stream of response frames. */
    BIDI_STREAMING
}
