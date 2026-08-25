package io.suboptimal.connectjava.model;

import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes a Connect service and its methods.
 *
 * @param serviceName exact Connect service name used for routing
 * @param methods     method definitions keyed by exact method name
 * @param schema      optional schema or descriptor for introspection
 */
public record ConnectServiceDefinition(
    String serviceName,
    Map<String, ConnectMethodDefinition> methods,
    @Nullable Object schema)
{
    public ConnectServiceDefinition {
        methods = Map.copyOf(methods);
    }

    public ConnectServiceDefinition(
        String serviceName,
        Collection<ConnectMethodDefinition> methods,
        @Nullable Object schema)
    {
        this(serviceName, toMap(methods), schema);
    }

    private static Map<String, ConnectMethodDefinition> toMap(Collection<ConnectMethodDefinition> methods) {
        return methods.stream().collect(Collectors.toMap(
            ConnectMethodDefinition::methodName, Function.identity()));
    }
}
