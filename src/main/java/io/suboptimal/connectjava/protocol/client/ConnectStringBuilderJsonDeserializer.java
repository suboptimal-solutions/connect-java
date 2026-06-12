package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ConnectStringBuilderJsonDeserializer implements ConnectJsonDeserializer {
    public static final ConnectStringBuilderJsonDeserializer INSTANCE =
        new ConnectStringBuilderJsonDeserializer();

    private ConnectStringBuilderJsonDeserializer() {}

    @Override
    public @Nullable ConnectError parseError(byte[] body) {
        String json = new String(body, StandardCharsets.UTF_8);
        String code = extractJsonString(json, "code");
        if (code == null) {
            return null;
        }
        ConnectErrorCode errorCode = findErrorCode(code);
        if (errorCode == null) {
            errorCode = ConnectErrorCode.UNKNOWN;
        }
        String message = extractJsonString(json, "message");
        List<ConnectErrorDetail> details = parseDetails(json);
        return new ConnectError(errorCode, message != null ? message : "", details);
    }

    @Override
    public @Nullable ConnectError parseEndStreamError(byte[] body) {
        String json = new String(body, StandardCharsets.UTF_8);
        int errorIdx = json.indexOf("\"error\"");
        if (errorIdx < 0) {
            return null;
        }
        int colonIdx = json.indexOf(':', errorIdx + 7);
        if (colonIdx < 0) {
            return null;
        }
        int braceOpen = json.indexOf('{', colonIdx + 1);
        if (braceOpen < 0) {
            return null;
        }
        int braceEnd = findClose(json, braceOpen, '{', '}');
        if (braceEnd < 0) {
            return null;
        }
        String errorJson = json.substring(braceOpen, braceEnd);

        String code = extractJsonString(errorJson, "code");
        ConnectErrorCode errorCode = (code != null) ? findErrorCode(code) : null;
        if (errorCode == null) {
            errorCode = ConnectErrorCode.UNKNOWN;
        }
        String message = extractJsonString(errorJson, "message");
        List<ConnectErrorDetail> details = parseDetails(errorJson);
        return new ConnectError(errorCode, message != null ? message : "", details);
    }

    @Override
    public Map<String, List<String>> parseEndStreamMetadata(byte[] body) {
        String json = new String(body, StandardCharsets.UTF_8);
        return parseMetadata(json);
    }

    @Override
    public @Nullable ConnectErrorBody parseErrorBody(byte[] body) {
        String json = new String(body, StandardCharsets.UTF_8);
        if (!json.contains("\"code\"") && !json.contains("\"message\"")
                && !json.contains("\"details\"")) {
            return null;
        }
        String codeName = extractJsonString(json, "code");
        String message = extractJsonString(json, "message");
        List<ConnectErrorDetail> details = parseDetails(json);
        return new ConnectErrorBody(codeName, message, details);
    }

    private static List<ConnectErrorDetail> parseDetails(String errorJson) {
        int detailsIdx = errorJson.indexOf("\"details\"");
        if (detailsIdx < 0) {
            return List.of();
        }
        int colonIdx = errorJson.indexOf(':', detailsIdx + 9);
        if (colonIdx < 0) {
            return List.of();
        }
        int arrayStart = errorJson.indexOf('[', colonIdx + 1);
        if (arrayStart < 0) {
            return List.of();
        }
        int arrayEnd = findClose(errorJson, arrayStart, '[', ']');
        if (arrayEnd < 0) {
            return List.of();
        }

        List<ConnectErrorDetail> result = new ArrayList<>();
        int pos = arrayStart + 1;
        while (pos < arrayEnd - 1) {
            int objStart = errorJson.indexOf('{', pos);
            if (objStart < 0 || objStart >= arrayEnd - 1) {
                break;
            }
            int objEnd = findClose(errorJson, objStart, '{', '}');
            if (objEnd < 0) {
                break;
            }
            String objJson = errorJson.substring(objStart, objEnd);
            String type = extractJsonString(objJson, "type");
            String value = extractJsonString(objJson, "value");
            if (type != null && value != null) {
                try {
                    byte[] decoded = Base64.getDecoder().decode(value);
                    result.add(new ConnectErrorDetail(type, decoded));
                } catch (IllegalArgumentException ignored) {
                    // skip malformed base64 detail
                }
            }
            pos = objEnd;
        }
        return List.copyOf(result);
    }

    private static Map<String, List<String>> parseMetadata(String json) {
        int metaIdx = json.indexOf("\"metadata\"");
        if (metaIdx < 0) {
            return Map.of();
        }
        int colonIdx = json.indexOf(':', metaIdx + 10);
        if (colonIdx < 0) {
            return Map.of();
        }
        int objStart = json.indexOf('{', colonIdx + 1);
        if (objStart < 0) {
            return Map.of();
        }
        int objEnd = findClose(json, objStart, '{', '}');
        if (objEnd < 0) {
            return Map.of();
        }

        Map<String, List<String>> result = new LinkedHashMap<>();
        int pos = objStart + 1;
        while (pos < objEnd - 1) {
            // find next key (a quoted string)
            while (pos < objEnd - 1 && json.charAt(pos) != '"') {
                pos++;
            }
            if (pos >= objEnd - 1) {
                break;
            }
            int keyEnd = skipString(json, pos);
            if (keyEnd < 0) {
                break;
            }
            String key = readString(json, pos);
            if (key == null) {
                break;
            }
            pos = keyEnd;

            // find ':'
            while (pos < objEnd && json.charAt(pos) != ':') {
                pos++;
            }
            if (pos >= objEnd) {
                break;
            }
            pos++; // skip ':'

            // find '[' for the value array
            while (pos < objEnd && json.charAt(pos) != '[') {
                pos++;
            }
            if (pos >= objEnd) {
                break;
            }
            int arrayEnd = findClose(json, pos, '[', ']');
            if (arrayEnd < 0) {
                break;
            }

            // extract string values from array
            List<String> values = new ArrayList<>();
            int aPos = pos + 1;
            while (aPos < arrayEnd - 1) {
                while (aPos < arrayEnd - 1 && json.charAt(aPos) != '"') {
                    aPos++;
                }
                if (aPos >= arrayEnd - 1) {
                    break;
                }
                int strEnd = skipString(json, aPos);
                if (strEnd < 0) {
                    break;
                }
                String val = readString(json, aPos);
                if (val != null) {
                    values.add(val);
                }
                aPos = strEnd;
            }
            result.put(key, List.copyOf(values));
            pos = arrayEnd;
        }
        return Collections.unmodifiableMap(result);
    }

    /** Returns index past the closing {@code "}, handling backslash escapes. -1 if malformed. */
    private static int skipString(String json, int openQuote) {
        int i = openQuote + 1;
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c == '\\') {
                i += 2;
                continue;
            }
            if (c == '"') {
                return i + 1;
            }
            i++;
        }
        return -1;
    }

    /** Returns the unescaped string value for the JSON string starting at {@code openQuote}. */
    private static @Nullable String readString(String json, int openQuote) {
        StringBuilder sb = new StringBuilder();
        int i = openQuote + 1;
        while (i < json.length()) {
            char c = json.charAt(i);
            if (c == '"') {
                return sb.toString();
            }
            if (c == '\\' && i + 1 < json.length()) {
                char next = json.charAt(i + 1);
                switch (next) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    default -> sb.append(next);
                }
                i += 2;
            } else {
                sb.append(c);
                i++;
            }
        }
        return null;
    }

    /** Returns index past the matching close delimiter, tracking depth and skipping strings. */
    private static int findClose(String json, int openIdx, char open, char close) {
        int depth = 1;
        int i = openIdx + 1;
        while (i < json.length() && depth > 0) {
            char c = json.charAt(i);
            if (c == '"') {
                int end = skipString(json, i);
                if (end < 0) {
                    return -1;
                }
                i = end;
                continue;
            }
            if (c == open) {
                depth++;
            } else if (c == close) {
                depth--;
            }
            i++;
        }
        return depth == 0 ? i : -1;
    }

    private static @Nullable String extractJsonString(String json, String field) {
        String key = "\"" + field + "\"";
        int keyIdx = json.indexOf(key);
        if (keyIdx < 0) {
            return null;
        }
        int colonIdx = json.indexOf(':', keyIdx + key.length());
        if (colonIdx < 0) {
            return null;
        }
        int p = colonIdx + 1;
        while (p < json.length() && Character.isWhitespace(json.charAt(p))) {
            p++;
        }
        if (p >= json.length() || json.charAt(p) != '"') {
            return null;
        }
        return readString(json, p);
    }

    private static @Nullable ConnectErrorCode findErrorCode(String wireName) {
        for (ConnectErrorCode code : ConnectErrorCode.values()) {
            if (code.wireName().equals(wireName)) {
                return code;
            }
        }
        return null;
    }
}
