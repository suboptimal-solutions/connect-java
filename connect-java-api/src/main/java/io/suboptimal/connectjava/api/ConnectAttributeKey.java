package io.suboptimal.connectjava.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Typed key for an attribute stored in {@link ConnectRequestMeta}.
 *
 * <p>Keys are stored in a global pool keyed by name: {@link #valueOf(String)} returns the same
 * instance for the same name across all call sites. Identity equality is preserved — the pool
 * simply guarantees one object per name.
 *
 * <h3>Recommended usage patterns</h3>
 * <ul>
 *   <li><strong>Library/framework keys</strong> — use {@link #newInstance(String)} so that name
 *       conflicts with other libraries are caught at class-load time:
 *       <pre>{@code
 *       public static final ConnectAttributeKey<Principal> PRINCIPAL =
 *           ConnectAttributeKey.newInstance("my.lib.principal");
 *       }</pre>
 *   </li>
 *   <li><strong>Application-side keys</strong> — {@link #valueOf(String)} is idempotent and
 *       safe to call multiple times:
 *       <pre>{@code
 *       static final ConnectAttributeKey<String> TENANT =
 *           ConnectAttributeKey.valueOf("app.tenant");
 *       }</pre>
 *   </li>
 * </ul>
 *
 * <p>The generic type parameter {@code T} is a compile-time convention. Calling
 * {@code valueOf} with the same name but a different type parameter returns the pooled
 * instance with an unchecked cast — the same trade-off as Netty's {@code AttributeKey}.
 *
 * @param <T> the type of the attribute value
 */
public final class ConnectAttributeKey<T> {
    private static final ConcurrentMap<String, ConnectAttributeKey<?>> POOL =
        new ConcurrentHashMap<>();

    private final String name;

    private ConnectAttributeKey(String name) {
        this.name = name;
    }

    private static void checkName(String name) {
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
    }

    /**
     * Returns the key registered under {@code name}, creating and registering a new one if absent.
     *
     * <p>This method is idempotent: repeated calls with the same name always return the same
     * instance.
     *
     * @param name the key name; must not be empty
     * @param <T>  the type of the attribute value
     * @return the pooled key for {@code name}
     * @throws IllegalArgumentException if {@code name} is empty
     */
    @SuppressWarnings("unchecked")
    public static <T> ConnectAttributeKey<T> valueOf(String name) {
        checkName(name);
        return (ConnectAttributeKey<T>) POOL.computeIfAbsent(name, ConnectAttributeKey::new);
    }

    /**
     * Creates and registers a new key under {@code name}.
     *
     * <p>Throws {@link IllegalArgumentException} if {@code name} is already registered, making
     * naming conflicts visible at class-load time. Prefer this method for library-owned keys.
     *
     * @param name the key name; must not be empty
     * @param <T>  the type of the attribute value
     * @return the newly registered key
     * @throws IllegalArgumentException if {@code name} is empty or already registered
     */
    public static <T> ConnectAttributeKey<T> newInstance(String name) {
        checkName(name);
        ConnectAttributeKey<T> key = new ConnectAttributeKey<>(name);
        ConnectAttributeKey<?> existing = POOL.putIfAbsent(name, key);
        if (existing != null) {
            throw new IllegalArgumentException("Key already registered: '" + name + "'");
        }
        return key;
    }

    /**
     * Returns {@code true} if a key is registered under {@code name}.
     *
     * @param name the key name; must not be empty
     * @return {@code true} if registered
     * @throws IllegalArgumentException if {@code name} is empty
     */
    public static boolean exists(String name) {
        checkName(name);
        return POOL.containsKey(name);
    }

    /** Returns the name of this key. */
    public String name() {
        return name;
    }

    @SuppressWarnings("unchecked")
    T cast(Object value) {
        return (T) value;
    }

    @Override
    public String toString() {
        return "ConnectAttributeKey(" + name + ")";
    }
}
