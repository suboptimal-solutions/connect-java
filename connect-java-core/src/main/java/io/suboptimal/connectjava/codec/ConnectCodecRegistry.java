package io.suboptimal.connectjava.codec;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Immutable registry of payload codecs ordered by server preference.
 */
public final class ConnectCodecRegistry {

    /**
     * Codecs in server-preference order.
     */
    private final List<ConnectCodec> preferred;

    /**
     * Lookup index derived from {@link #preferred}.
     */
    private final Map<String, ConnectCodec> byName;

    private ConnectCodecRegistry(List<ConnectCodec> codecs) {
        Map<String, ConnectCodec> map = new LinkedHashMap<>();
        for (ConnectCodec codec : codecs) {
            String name = canonicalImplementationName(codec.name());
            ConnectCodec previous = map.putIfAbsent(name, codec);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate codec name: " + name);
            }
        }
        this.preferred = List.copyOf(codecs);
        this.byName = Map.copyOf(map);
    }

    /**
     * Starts a registry builder. Codecs are kept in registration order, which is also the
     * server-preference order exposed by {@link #preferred()}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Resolves a codec by protocol-facing name.
     *
     * <p>The supplied name is trimmed and lower-cased with {@link Locale#ROOT} before lookup.
     * Returns {@code null} when no codec is registered under that canonical name.
     */
    public @Nullable ConnectCodec byName(String name) {
        return byName.get(canonicalName(name));
    }

    /**
     * Returns codecs in server-preference order.
     */
    public List<ConnectCodec> preferred() {
        return preferred;
    }

    private static String canonicalName(String name) {
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("ConnectCodec name must not be empty");
        }
        return normalized;
    }

    private static String canonicalImplementationName(String name) {
        String normalized = canonicalName(name);
        if (!name.equals(normalized)) {
            throw new IllegalArgumentException("ConnectCodec name must be canonical lower-case: " + name);
        }
        return normalized;
    }

    /**
     * Mutable builder for an immutable {@link ConnectCodecRegistry}.
     */
    public static final class Builder {
        private final List<ConnectCodec> codecs = new ArrayList<>();

        private Builder() {}

        /**
         * Registers a codec.
         *
         * <p>The codec's {@link ConnectCodec#name()} must already be canonical lower-case. Duplicate
         * canonical names are rejected when {@link #build()} is called. Registration order is
         * preserved as server-preference order.
         */
        public Builder register(ConnectCodec codec) {
            codecs.add(codec);
            return this;
        }

        /**
         * Builds an immutable registry and rejects duplicate canonical codec names.
         */
        public ConnectCodecRegistry build() {
            return new ConnectCodecRegistry(codecs);
        }
    }
}
