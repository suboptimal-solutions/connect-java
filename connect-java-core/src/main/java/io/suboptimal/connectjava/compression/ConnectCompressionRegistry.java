package io.suboptimal.connectjava.compression;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Immutable registry of compression algorithms ordered by server preference.
 */
public final class ConnectCompressionRegistry {

    public static final String IDENTITY_NAME = "identity";

    private static final ConnectCompressionRegistry STANDARD = builder()
        .register(ConnectGzipCompression.INSTANCE)
        .build();

    /**
     * User-registered, non-identity algorithms in server-preference order.
     */
    private final List<ConnectCompression> preferred;

    /**
     * Lookup index for non-identity algorithms derived from {@link #preferred}.
     */
    private final Map<String, ConnectCompression> byName;

    /**
     * Names that protocols may advertise as actual compression algorithms.
     */
    private final List<String> advertisedNames;

    /**
     * Names accepted by {@link #resolve(String)}.
     */
    private final List<String> supportedNames;

    private ConnectCompressionRegistry(List<ConnectCompression> compressions) {
        Map<String, ConnectCompression> map = new LinkedHashMap<>();
        for (ConnectCompression compression : compressions) {
            String name = canonicalImplementationName(compression.name());
            if (IDENTITY_NAME.equals(name)) {
                throw new IllegalArgumentException("'identity' is a reserved compression name");
            }
            ConnectCompression previous = map.putIfAbsent(name, compression);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate compression name: " + name);
            }
        }
        this.preferred = List.copyOf(compressions);
        this.byName = Map.copyOf(map);

        List<String> advertised = new ArrayList<>();
        for (ConnectCompression compression : preferred) {
            advertised.add(compression.name());
        }
        this.advertisedNames = List.copyOf(advertised);

        List<String> names = new ArrayList<>();
        names.add(ConnectIdentityCompression.INSTANCE.name());
        for (ConnectCompression compression : preferred) {
            names.add(compression.name());
        }
        this.supportedNames = List.copyOf(names);
    }

    /**
     * Starts a registry builder. Non-identity compressions are kept in registration order, which
     * is also the server-preference order exposed by {@link #preferred()}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the conservative default registry: first-class identity plus direct one-shot gzip.
     */
    public static ConnectCompressionRegistry standard() {
        return STANDARD;
    }

    /**
     * Resolves a compression implementation by protocol-facing name.
     *
     * <p>The supplied name is trimmed and lower-cased with {@link Locale#ROOT} before lookup.
     * Returns {@code null} when the registry does not support that canonical name. Identity is
     * always supported even though it is not user-registered.
     */
    public @Nullable ConnectCompression resolve(String name) {
        String normalized = canonicalName(name);
        if (IDENTITY_NAME.equals(normalized)) {
            return ConnectIdentityCompression.INSTANCE;
        }
        return byName.get(normalized);
    }

    /**
     * Returns non-identity compression algorithms in server-preference order.
     */
    public List<ConnectCompression> preferred() {
        return preferred;
    }

    /**
     * Returns non-identity compression names that protocols may advertise.
     */
    public List<String> advertisedNames() {
        return advertisedNames;
    }

    /**
     * Returns all supported compression names: identity plus advertised names.
     */
    public List<String> supportedNames() {
        return supportedNames;
    }

    private static String canonicalName(String name) {
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("ConnectCompression name must not be empty");
        }
        return normalized;
    }

    private static String canonicalImplementationName(String name) {
        String normalized = canonicalName(name);
        if (!name.equals(normalized)) {
            throw new IllegalArgumentException("ConnectCompression name must be canonical lower-case: " + name);
        }
        return normalized;
    }

    /**
     * Mutable builder for an immutable {@link ConnectCompressionRegistry}.
     */
    public static final class Builder {
        private final List<ConnectCompression> compressions = new ArrayList<>();

        private Builder() {}

        /**
         * Registers a non-identity compression.
         *
         * <p>The compression's {@link ConnectCompression#name()} must already be canonical lower-case.
         * Duplicate canonical names and the reserved {@code identity} name are rejected when
         * {@link #build()} is called. Registration order is preserved as server-preference order.
         */
        public Builder register(ConnectCompression compression) {
            compressions.add(compression);
            return this;
        }

        /**
         * Builds an immutable registry and rejects duplicate or reserved canonical names.
         */
        public ConnectCompressionRegistry build() {
            return new ConnectCompressionRegistry(compressions);
        }
    }
}
