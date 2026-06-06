package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import org.jspecify.annotations.Nullable;

import java.util.Locale;

final class ConnectMediaType {

    private ConnectMediaType() {}

    static @Nullable String codecNameFor(HttpRequest request) {
        CharSequence mimeTypeRaw = HttpUtil.getMimeType(request);
        String mimeType = mimeTypeRaw == null ? "" : mimeTypeRaw.toString();

        return switch (mimeType.toLowerCase(Locale.ROOT)) {
            case "application/proto", "application/connect+proto" -> "proto";
            case "application/json", "application/connect+json" -> "json";
            default -> null;
        };
    }

    static String unaryContentTypeFor(String codecName) {
        return switch (codecName) {
            case "proto" -> "application/proto";
            case "json" -> "application/json";
            default -> throw new IllegalArgumentException("Unsupported Connect unary codec: " + codecName);
        };
    }

    static String streamingContentTypeFor(String codecName) {
        return switch (codecName) {
            case "proto" -> "application/connect+proto";
            case "json" -> "application/connect+json";
            default -> throw new IllegalArgumentException("Unsupported Connect streaming codec: " + codecName);
        };
    }
}
