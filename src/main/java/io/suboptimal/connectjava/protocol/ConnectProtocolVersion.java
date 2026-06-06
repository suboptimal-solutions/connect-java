package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.HttpHeaders;
import org.jspecify.annotations.Nullable;

import java.util.List;

final class ConnectProtocolVersion {
    public static final String HEADER_VERSION = "1";
    public static final String QUERY_VERSION = "v1";

    private ConnectProtocolVersion() {}

    static @Nullable String validate(HttpHeaders headers) {
        String connectVersion = headers.get("connect-protocol-version");
        if (connectVersion == null || HEADER_VERSION.equals(connectVersion)) {
            return null;
        }
        return formatError(connectVersion);
    }

    static @Nullable String validate(@Nullable List<String> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) {
            return null;
        }
        if (queryParams.size() > 1) {
            return formatError(String.join(",", queryParams));
        }
        String connectVersion = queryParams.getFirst();
        if (QUERY_VERSION.equals(connectVersion)) {
            return null;
        }
        return formatError(connectVersion);
    }

    private static String formatError(String connectVersion) {
        return "Unsupported Connect protocol version: " + connectVersion;
    }
}
