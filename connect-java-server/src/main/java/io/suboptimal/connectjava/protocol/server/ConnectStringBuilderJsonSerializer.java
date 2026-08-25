package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorDetail;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public final class ConnectStringBuilderJsonSerializer implements ConnectJsonSerializer {
    public static final ConnectStringBuilderJsonSerializer INSTANCE = new ConnectStringBuilderJsonSerializer();

    private ConnectStringBuilderJsonSerializer() {}

    @Override
    public byte[] error(ConnectError error) {
        StringBuilder sb = new StringBuilder("{");
        appendErrorObject(sb, error);
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] endStream(ConnectEndStreamResponse message) {
        ConnectError error = message.error();
        ConnectEndStreamMeta metadata = message.metadata();
        StringBuilder sb = new StringBuilder("{");
        boolean hasField = false;
        if (error != null) {
            sb.append("\"error\":{");
            appendErrorObject(sb, error);
            sb.append('}');
            hasField = true;
        }
        if (!metadata.isEmpty()) {
            if (hasField) {
                sb.append(',');
            }
            appendMetadataObject(sb, metadata);
        }
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendErrorObject(StringBuilder sb, ConnectError error) {
        sb.append("\"code\":\"");
        appendEscaped(sb, error.code().wireName());
        sb.append('"');
        if (!error.message().isEmpty()) {
            sb.append(",\"message\":\"");
            appendEscaped(sb, error.message());
            sb.append('"');
        }
        List<ConnectErrorDetail> details = error.details();
        if (!details.isEmpty()) {
            sb.append(",\"details\":[");
            boolean first = true;
            for (ConnectErrorDetail detail : details) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append("{\"type\":\"");
                appendEscaped(sb, detail.type());
                sb.append("\",\"value\":\"");
                sb.append(Base64.getEncoder().withoutPadding().encodeToString(detail.value()));
                sb.append("\"}");
            }
            sb.append(']');
        }
    }

    private static void appendMetadataObject(StringBuilder sb, ConnectEndStreamMeta metadata) {
        sb.append("\"metadata\":{");
        boolean firstName = true;
        for (String name : metadata.names()) {
            if (!firstName) {
                sb.append(',');
            }
            firstName = false;
            sb.append('"');
            appendEscaped(sb, name);
            sb.append("\":[");
            boolean firstValue = true;
            for (String value : metadata.getAll(name)) {
                if (!firstValue) {
                    sb.append(',');
                }
                firstValue = false;
                sb.append('"');
                appendEscaped(sb, value);
                sb.append('"');
            }
            sb.append(']');
        }
        sb.append('}');
    }

    private static void appendEscaped(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
    }
}
