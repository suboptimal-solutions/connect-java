package io.suboptimal.connectjava.protocol.server;

import java.util.Set;

/**
 * CORS configuration for {@link ConnectServerProtocol}.
 *
 * <p>Use {@link #disabled()} when CORS is not needed, {@link #defaultsForAnyOrigin()} for
 * wildcard-origin CORS, or {@link #defaultsForOrigins(Set)} for exact-origin allowlists.
 *
 * @param enabled           whether a {@link io.netty.handler.codec.http.cors.CorsHandler} is installed
 * @param anyOrigin         if {@code true}, allow any origin ({@code *}); ignored when disabled
 * @param allowedOrigins    exact origins to allow; must be non-empty when {@code enabled && !anyOrigin}
 * @param allowedMethods    HTTP methods to advertise in preflight responses
 * @param allowedHeaders    request headers to advertise in preflight responses
 * @param exposedHeaders    response headers to expose to browser callers
 * @param maxAgeSeconds     preflight cache duration in seconds; must be {@code >= 0}
 * @param allowCredentials  whether to set {@code Access-Control-Allow-Credentials: true};
 *                          incompatible with {@code anyOrigin}
 * @param allowPrivateNetwork whether to set {@code Access-Control-Allow-Private-Network: true}
 */
public record ConnectCorsParameters(
    boolean enabled,
    boolean anyOrigin,
    Set<String> allowedOrigins,
    Set<String> allowedMethods,
    Set<String> allowedHeaders,
    Set<String> exposedHeaders,
    long maxAgeSeconds,
    boolean allowCredentials,
    boolean allowPrivateNetwork
) {
    public static final Set<String> CONNECT_METHODS = Set.of("GET", "POST");
    public static final Set<String> CONNECT_HEADERS = Set.of(
        "Content-Type", "Connect-Protocol-Version", "Connect-Timeout-Ms", "X-User-Agent");
    private static final long DEFAULT_MAX_AGE = 7200L;

    public ConnectCorsParameters {
        if (enabled) {
            if (!anyOrigin && allowedOrigins.isEmpty()) {
                throw new IllegalArgumentException(
                    "allowedOrigins must be non-empty when CORS is enabled and anyOrigin is false");
            }
            if (allowCredentials && anyOrigin) {
                throw new IllegalArgumentException(
                    "allowCredentials cannot be combined with anyOrigin");
            }
            if (allowedMethods.isEmpty()) {
                throw new IllegalArgumentException("allowedMethods must not be empty when CORS is enabled");
            }
            if (maxAgeSeconds < 0) {
                throw new IllegalArgumentException("maxAgeSeconds must be >= 0");
            }
        }
        allowedOrigins = Set.copyOf(allowedOrigins);
        allowedMethods = Set.copyOf(allowedMethods);
        allowedHeaders = Set.copyOf(allowedHeaders);
        exposedHeaders = Set.copyOf(exposedHeaders);
    }

    /** Returns a disabled CORS configuration; no handler is installed in the pipeline. */
    public static ConnectCorsParameters disabled() {
        return new ConnectCorsParameters(
            false, false, Set.of(), Set.of(), Set.of(), Set.of(), 0L, false, false);
    }

    /** Returns an enabled configuration that allows any origin with Connect defaults. */
    public static ConnectCorsParameters defaultsForAnyOrigin() {
        return new ConnectCorsParameters(
            true, true, Set.of(), CONNECT_METHODS, CONNECT_HEADERS, Set.of(),
            DEFAULT_MAX_AGE, false, false);
    }

    /**
     * Returns an enabled configuration restricted to the supplied exact origins with Connect defaults.
     *
     * @param origins non-empty set of allowed origins
     */
    public static ConnectCorsParameters defaultsForOrigins(Set<String> origins) {
        if (origins.isEmpty()) {
            throw new IllegalArgumentException("origins must not be empty");
        }
        return new ConnectCorsParameters(
            true, false, origins, CONNECT_METHODS, CONNECT_HEADERS, Set.of(),
            DEFAULT_MAX_AGE, false, false);
    }
}
