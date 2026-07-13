package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Mutable view of an outgoing {@link ConnectClientCallStart}, handed to client interceptors so
 * they can shape the request in place (headers, timeout, codec, GET preference) instead of
 * rebuilding an immutable record on every edit.
 *
 * <p>{@code serviceDefinition} and {@code methodDefinition} are read-only: an interceptor may
 * read them but cannot change the call target. Not thread-safe; mutate only from the Netty event
 * loop. Header names are treated case-insensitively (stored lower-cased).
 */
public final class ConnectClientCallStartBuilder {
    private final ConnectServiceDefinition serviceDefinition;
    private final ConnectMethodDefinition methodDefinition;
    private final Map<String, List<String>> requestHeaders; // lower-cased keys, mutable
    private boolean preferGet;
    private @Nullable String codecName;
    private @Nullable Long timeoutMs;
    private @Nullable String authority;

    public ConnectClientCallStartBuilder(ConnectClientCallStart start) {
        Objects.requireNonNull(start);
        this.serviceDefinition = start.serviceDefinition();
        this.methodDefinition = start.methodDefinition();
        this.requestHeaders = new LinkedHashMap<>();
        start.requestHeaders().forEach((k, v) ->
            this.requestHeaders.put(k.toLowerCase(Locale.ROOT), new ArrayList<>(v)));
        this.preferGet = start.preferGet();
        this.codecName = start.codecName();
        this.timeoutMs = start.timeoutMs();
        this.authority = start.authority();
    }

    public ConnectServiceDefinition serviceDefinition() { return serviceDefinition; }
    public ConnectMethodDefinition methodDefinition() { return methodDefinition; }

    public boolean preferGet() { return preferGet; }
    public ConnectClientCallStartBuilder preferGet(boolean preferGet) {
        this.preferGet = preferGet;
        return this;
    }

    public @Nullable String codecName() { return codecName; }
    public ConnectClientCallStartBuilder codecName(@Nullable String codecName) {
        this.codecName = codecName;
        return this;
    }

    public @Nullable Long timeoutMs() { return timeoutMs; }
    public ConnectClientCallStartBuilder timeoutMs(@Nullable Long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public @Nullable String authority() { return authority; }
    public ConnectClientCallStartBuilder authority(@Nullable String authority) {
        this.authority = authority;
        return this;
    }

    /** Returns the current values for {@code name} (case-insensitive); empty list if none. */
    public List<String> headerValues(String name) {
        List<String> values = requestHeaders.get(name.toLowerCase(Locale.ROOT));
        return values != null ? List.copyOf(values) : List.of();
    }

    /** Appends {@code value} under {@code name} (case-insensitive), keeping existing values. */
    public ConnectClientCallStartBuilder addHeader(String name, String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        requestHeaders.computeIfAbsent(name.toLowerCase(Locale.ROOT), k -> new ArrayList<>()).add(value);
        return this;
    }

    /** Replaces all values for {@code name} (case-insensitive) with the single {@code value}. */
    public ConnectClientCallStartBuilder setHeader(String name, String value) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(value);
        List<String> list = new ArrayList<>();
        list.add(value);
        requestHeaders.put(name.toLowerCase(Locale.ROOT), list);
        return this;
    }

    /** Removes all values for {@code name} (case-insensitive). */
    public ConnectClientCallStartBuilder removeHeader(String name) {
        requestHeaders.remove(name.toLowerCase(Locale.ROOT));
        return this;
    }

    /** Freezes the current state into an immutable {@link ConnectClientCallStart}. */
    public ConnectClientCallStart build() {
        return new ConnectClientCallStart(
            serviceDefinition, methodDefinition, requestHeaders, preferGet, codecName, timeoutMs, authority);
    }
}
