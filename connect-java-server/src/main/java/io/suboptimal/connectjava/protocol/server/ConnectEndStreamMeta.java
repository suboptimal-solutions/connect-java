package io.suboptimal.connectjava.protocol.server;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Connect streaming end-stream metadata snapshot.
 */
public final class ConnectEndStreamMeta {
    public static final ConnectEndStreamMeta EMPTY = new ConnectEndStreamMeta(Map.of());

    private final Map<String, List<String>> values;

    private ConnectEndStreamMeta(Map<String, List<String>> values) {
        this.values = values;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public Set<String> names() {
        return values.keySet();
    }

    public List<String> getAll(String name) {
        List<String> result = values.get(name.toLowerCase(Locale.ROOT));
        return result == null ? List.of() : result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectEndStreamMeta that)) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }

    @Override
    public String toString() {
        return values.toString();
    }

    public static final class Builder {
        private final Map<String, List<String>> values = new LinkedHashMap<>();

        private Builder() {}

        public Builder set(CharSequence name, CharSequence value) {
            values.put(normalize(name), new ArrayList<>(List.of(value.toString())));
            return this;
        }

        public Builder add(CharSequence name, CharSequence value) {
            values.computeIfAbsent(normalize(name), ignored -> new ArrayList<>()).add(value.toString());
            return this;
        }

        ConnectEndStreamMeta build() {
            if (values.isEmpty()) {
                return EMPTY;
            }
            return new ConnectEndStreamMeta(values);
        }

        private static String normalize(CharSequence name) {
            return name.toString().toLowerCase(Locale.ROOT);
        }
    }
}
