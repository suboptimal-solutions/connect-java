package io.suboptimal.connectjava.grpcbridge;

import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Records the response trailers the bridge writes, in the order it writes them.
 *
 * <p>The counterpart of {@link TestResponseHeadersBuilder}; the same confinement note applies.
 */
class TestResponseTrailersBuilder implements ConnectResponseTrailersBuilder {
    private final Map<String, List<String>> trailers = new LinkedHashMap<>();

    @Override
    public ConnectResponseTrailersBuilder add(CharSequence name, CharSequence value) {
        trailers.computeIfAbsent(name.toString(), k -> new ArrayList<>()).add(value.toString());
        return this;
    }

    @Override
    public ConnectResponseTrailersBuilder set(CharSequence name, CharSequence value) {
        var values = new ArrayList<String>();
        values.add(value.toString());
        trailers.put(name.toString(), values);
        return this;
    }

    List<String> allValues(String name) {
        return trailers.getOrDefault(name, List.of());
    }
}
