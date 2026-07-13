package io.suboptimal.connectjava.api;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata from a Connect RPC response.
 *
 * @param statusCode HTTP status code
 * @param headers    leading metadata (HTTP response headers without {@code trailer-} prefix)
 */
public record ConnectResponseMeta(
    int statusCode,
    Map<String, List<String>> headers
) {
    public ConnectResponseMeta {
        headers = copyLower(headers);
    }

    private static Map<String, List<String>> copyLower(Map<String, List<String>> source) {
        return source
                .entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT),
                        Map.Entry::getValue));
    }
}
