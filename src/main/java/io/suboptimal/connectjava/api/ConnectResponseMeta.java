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
 * @param trailers   trailing metadata; for unary responses these are extracted from HTTP headers
 *                   with the {@code trailer-} prefix stripped; for streaming responses they
 *                   come from the end-stream envelope's {@code metadata} field via
 *                   {@link ConnectEndOfStream#trailers()}
 */
public record ConnectResponseMeta(
    int statusCode,
    Map<String, List<String>> headers,
    Map<String, List<String>> trailers
) {
    public ConnectResponseMeta {
        headers = copyLower(headers);
        trailers = copyLower(trailers);
    }

    private static Map<String, List<String>> copyLower(Map<String, List<String>> source) {
        return source
                .entrySet()
                .stream()
                .collect(Collectors.toUnmodifiableMap(e -> e.getKey().toLowerCase(Locale.ROOT),
                        Map.Entry::getValue));
    }
}
