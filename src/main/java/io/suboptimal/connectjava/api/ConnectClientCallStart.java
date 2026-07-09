package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Outbound trigger written by the terminal handler to initiate a Connect RPC call.
 *
 * @param serviceDefinition the target service
 * @param methodDefinition  the target method
 * @param requestHeaders    user request headers; keys are normalized to lower case. Protocol-managed
 *                          headers (content-type, content-length, encodings, etc.) are ignored.
 * @param preferGet         when {@code true} and the method is idempotent, the call is sent as a GET
 * @param codecName         payload codec to use (e.g. {@code "proto"} or {@code "json"}); when
 *                          {@code null} the codec registry's preferred codec is used
 * @param timeoutMs         call timeout in milliseconds; when non-null the {@code connect-timeout-ms}
 *                          request header is set; {@code null} means no timeout header is sent
 * @param authority         logical target authority ({@code host} or {@code host:port}) used as the
 *                          HTTP/1.1 {@code Host} header; when {@code null} the client derives it from
 *                          the channel's remote address
 */
public record ConnectClientCallStart(
    ConnectServiceDefinition serviceDefinition,
    ConnectMethodDefinition methodDefinition,
    Map<String, List<String>> requestHeaders,
    boolean preferGet,
    @Nullable String codecName,
    @Nullable Long timeoutMs,
    @Nullable String authority
) {
    public ConnectClientCallStart {
        Objects.requireNonNull(serviceDefinition);
        Objects.requireNonNull(methodDefinition);
        Objects.requireNonNull(requestHeaders);
        requestHeaders = requestHeaders
                .entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT),
                        e -> List.copyOf(e.getValue())));
    }

    public ConnectClientCallStart(
        ConnectServiceDefinition serviceDefinition,
        ConnectMethodDefinition methodDefinition,
        Map<String, List<String>> requestHeaders,
        boolean preferGet,
        @Nullable String codecName
    ) {
        this(serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, null, null);
    }

    public ConnectClientCallStart(
        ConnectServiceDefinition serviceDefinition,
        ConnectMethodDefinition methodDefinition,
        Map<String, List<String>> requestHeaders,
        boolean preferGet,
        @Nullable String codecName,
        @Nullable Long timeoutMs
    ) {
        this(serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs, null);
    }

}
