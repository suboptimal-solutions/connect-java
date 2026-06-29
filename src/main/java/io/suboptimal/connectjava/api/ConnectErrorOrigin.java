package io.suboptimal.connectjava.api;

/**
 * Distinguishes how a {@link ConnectError} arose on the client.
 *
 * <ul>
 *   <li>{@link #RPC} — a Connect RPC-level outcome: the server (or the client itself)
 *       produced a Connect error with a meaningful code. This is the default for every
 *       {@code ConnectError}.</li>
 *   <li>{@link #TRANSPORT} — a transport-level rejection at the gate: a non-200 HTTP
 *       response whose body is not a recognized Connect error (e.g. 404/415/405/505 with
 *       {@code text/plain}). The Connect RPC never started.</li>
 * </ul>
 */
public enum ConnectErrorOrigin {
    RPC,
    TRANSPORT
}
