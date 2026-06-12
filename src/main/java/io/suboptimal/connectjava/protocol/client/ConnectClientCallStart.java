package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 */
public record ConnectClientCallStart(
    ConnectServiceDefinition serviceDefinition,
    ConnectMethodDefinition methodDefinition,
    Map<String, List<String>> requestHeaders,
    boolean preferGet,
    @Nullable String codecName,
    @Nullable Long timeoutMs
) {
    public ConnectClientCallStart {
        Objects.requireNonNull(serviceDefinition);
        Objects.requireNonNull(methodDefinition);
        Objects.requireNonNull(requestHeaders);
        requestHeaders = requestHeaders
                .entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT),
                        Map.Entry::getValue));
    }

    public ConnectClientCallStart(
        ConnectServiceDefinition serviceDefinition,
        ConnectMethodDefinition methodDefinition,
        Map<String, List<String>> requestHeaders,
        boolean preferGet,
        @Nullable String codecName
    ) {
        this(serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, null);
    }

    /**
     * Returns a copy with {@code value} appended under {@code name} (case-insensitive); other
     * fields are unchanged. Intended for interceptors adding an outgoing request header.
     */
    public ConnectClientCallStart withHeader(String name, String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        String key = name.toLowerCase(Locale.ROOT);
        Map<String, List<String>> headers = new LinkedHashMap<>(requestHeaders);
        List<String> existing = headers.get(key);
        List<String> merged = new ArrayList<>(existing != null ? existing : List.of());
        merged.add(value);
        headers.put(key, merged);
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, headers, preferGet, codecName, timeoutMs);
    }

    /** Returns a copy with {@code requestHeaders} replaced; other fields are unchanged. */
    public ConnectClientCallStart withRequestHeaders(Map<String, List<String>> requestHeaders) {
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs);
    }

    /** Returns a copy with {@code timeoutMs} replaced; other fields are unchanged. */
    public ConnectClientCallStart withTimeoutMs(@Nullable Long timeoutMs) {
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs);
    }

    /** Returns a copy with {@code codecName} replaced; other fields are unchanged. */
    public ConnectClientCallStart withCodecName(@Nullable String codecName) {
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs);
    }

    /** Returns a copy with {@code preferGet} replaced; other fields are unchanged. */
    public ConnectClientCallStart withPreferGet(boolean preferGet) {
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs);
    }
}
