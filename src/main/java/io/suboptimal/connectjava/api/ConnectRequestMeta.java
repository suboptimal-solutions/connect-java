package io.suboptimal.connectjava.api;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Request metadata decoded from the Connect transport.
 *
 * <p>Connect is HTTP-based, so request metadata is modeled directly as HTTP headers.
 * Header names are normalized to lower-case and header values are immutable. Custom
 * attributes are mutable so interceptors can attach per-call data before dispatch.
 */
public final class ConnectRequestMeta {
    private final Map<String, List<String>> headers;
    private final Map<ConnectAttributeKey<?>, Object> attributes = new HashMap<>();

    /**
     * Creates request metadata from HTTP headers.
     *
     * @param headers header name to ordered values
     */
    public ConnectRequestMeta(Map<String, List<String>> headers) {
        Map<String, List<String>> copied = new LinkedHashMap<>();
        headers.forEach((name, values) ->
            copied.put(name.toLowerCase(Locale.ROOT), List.copyOf(values)));
        this.headers = Collections.unmodifiableMap(copied);
    }

    /**
     * Returns all headers keyed by lower-case name.
     *
     * @return immutable header map
     */
    public Map<String, List<String>> headers() {
        return headers;
    }

    /**
     * Returns all values for {@code name}, or an empty list if the header is absent.
     *
     * @param name header name in any case
     * @return immutable header values
     */
    public List<String> headerValues(String name) {
        return headers.getOrDefault(name.toLowerCase(Locale.ROOT), List.of());
    }

    /**
     * Returns the first value for {@code name}, or {@code null} if the header is absent.
     *
     * @param name header name in any case
     * @return first header value, or {@code null}
     */
    public @Nullable String firstHeader(String name) {
        List<String> values = headerValues(name);
        return values.isEmpty() ? null : values.getFirst();
    }

    /**
     * Stores a custom per-call attribute.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @param <T>   the type of the attribute value
     */
    public <T> void put(ConnectAttributeKey<T> key, T value) {
        attributes.put(key, value);
    }

    /**
     * Returns the attribute stored under {@code key}, or {@code null} if absent.
     *
     * @param key the attribute key
     * @param <T> the type of the attribute value
     * @return attribute value, or {@code null}
     */
    public <T> @Nullable T get(ConnectAttributeKey<T> key) {
        Object value = attributes.get(key);
        return value != null ? key.cast(value) : null;
    }

    /**
     * Returns {@code true} if an attribute is stored under {@code key}.
     *
     * @param key the attribute key
     * @return {@code true} if present
     */
    public boolean contains(ConnectAttributeKey<?> key) {
        return attributes.containsKey(key);
    }
}
