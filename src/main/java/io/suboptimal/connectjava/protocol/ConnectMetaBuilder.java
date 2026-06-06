package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.HttpHeaders;
import io.suboptimal.connectjava.api.ConnectRequestMeta;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class ConnectMetaBuilder {
    private ConnectMetaBuilder() {}

    static ConnectRequestMeta fromHeaders(HttpHeaders headers) {
        Map<String, List<String>> headerMap = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : headers) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            headerMap.computeIfAbsent(key, k -> new ArrayList<>()).add(entry.getValue());
        }
        Map<String, List<String>> immutableHeaderMap = new LinkedHashMap<>();
        headerMap.forEach((k, v) -> immutableHeaderMap.put(k, List.copyOf(v)));

        return new ConnectRequestMeta(immutableHeaderMap);
    }
}
