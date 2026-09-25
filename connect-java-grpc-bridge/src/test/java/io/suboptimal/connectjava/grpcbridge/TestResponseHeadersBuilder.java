package io.suboptimal.connectjava.grpcbridge;

import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records the response headers the bridge writes, in the order it writes them.
 *
 * <p>Stands in for what connect-java hands the handler through {@code ConnectCallExchange}. The
 * real one is event-loop-confined and so is this: a test reads {@link #allValues} only after the
 * call has settled.
 */
class TestResponseHeadersBuilder implements ConnectResponseHeadersBuilder {
    private final Map<String, List<String>> headers = new LinkedHashMap<>();

    @Override
    public ConnectResponseHeadersBuilder add(CharSequence name, CharSequence value) {
        headers.computeIfAbsent(name.toString(), k -> new ArrayList<>()).add(value.toString());
        return this;
    }

    @Override
    public ConnectResponseHeadersBuilder set(CharSequence name, CharSequence value) {
        var values = new ArrayList<String>();
        values.add(value.toString());
        headers.put(name.toString(), values);
        return this;
    }

    List<String> allValues(String name) {
        return headers.getOrDefault(name, List.of());
    }
}
