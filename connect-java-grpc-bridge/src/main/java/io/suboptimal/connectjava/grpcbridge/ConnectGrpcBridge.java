package io.suboptimal.connectjava.grpcbridge;

import io.grpc.BindableService;
import io.grpc.ServerMethodDefinition;
import io.netty.channel.ChannelHandler;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.server.spi.ConnectServerCallHandlerFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Bridge between connect-java and gRPC service stubs.
 *
 * <p>Wraps one or more {@link BindableService} implementations and provides:
 * <ul>
 *   <li>{@link ConnectServerCallHandlerFactory} for the connect-java pipeline</li>
 *   <li>{@link #serviceDefinitions()} for {@code ConnectProtocolConfig}</li>
 * </ul>
 *
 * <p>Wrapped services are canonical gRPC {@code *ImplBase} stubs and must not depend on
 * anything Connect-specific. To read incoming metadata or set response headers/trailers,
 * use a standard {@link io.grpc.ServerInterceptor} with gRPC's own {@link io.grpc.Metadata}
 * before passing the service to {@link #of} - the bridge preserves interceptors applied via
 * {@link io.grpc.ServerInterceptors#intercept}.
 */
public final class ConnectGrpcBridge implements ConnectServerCallHandlerFactory {
    private static final Logger log = LoggerFactory.getLogger(ConnectGrpcBridge.class);

    private final Map<String, ConnectServiceDefinition> serviceDefinitions;
    private final Map<String, Map<String, ServerMethodDefinition<?, ?>>> methods;
    private @Nullable final Executor serviceExecutor;

    private ConnectGrpcBridge(
        Map<String, ConnectServiceDefinition> serviceDefinitions,
        Map<String, Map<String, ServerMethodDefinition<?, ?>>> methods,
        @Nullable Executor serviceExecutor)
    {
        this.serviceDefinitions = Map.copyOf(serviceDefinitions);
        var copied = new LinkedHashMap<String, Map<String, ServerMethodDefinition<?, ?>>>();
        methods.forEach((k, v) -> copied.put(k, Map.copyOf(v)));
        this.methods = Map.copyOf(copied);
        this.serviceExecutor = serviceExecutor;

        log.info("Bridging {} gRPC service(s) over Connect: {}",
            this.serviceDefinitions.size(), this.serviceDefinitions.keySet());
        this.methods.forEach((serviceName, serviceMethods) ->
            log.debug("Service {} exposes methods {}", serviceName, serviceMethods.keySet()));
    }

    /**
     * Creates a bridge for a single service, running calls on the default executor.
     *
     * <p>That default is a virtual thread per task, so calls complete <em>asynchronously</em>: a
     * caller that writes inbound messages and immediately reads the response back - a test with an
     * {@code EmbeddedChannel}, typically - has to wait for the executor first. Use
     * {@link Builder#withDirectExecutor()} where synchronous completion is wanted.
     */
    public static ConnectGrpcBridge of(BindableService service) {
        return builder().addService(service).build();
    }

    /** Returns a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ChannelHandler create() {
        return new GrpcBridgeHandler(this, serviceExecutor);
    }

    /** Returns service definitions for use with {@code ConnectProtocolConfig.builder()}. */
    public Map<String, ConnectServiceDefinition> serviceDefinitions() {
        return serviceDefinitions;
    }

    /**
     * Returns the gRPC method definition for the given service and method names.
     *
     * <p>{@code GrpcBridgeHandler} catches the failure and replies with a Connect
     * {@code unimplemented} error rather than letting it escape the pipeline.
     *
     * @throws IllegalArgumentException if the service or method is unknown
     */
    ServerMethodDefinition<?, ?> method(String serviceName, String methodName) {
        Map<String, ServerMethodDefinition<?, ?>> serviceMethods = methods.get(serviceName);
        if (serviceMethods == null) {
            throw new IllegalArgumentException("Unknown service: " + serviceName);
        }
        ServerMethodDefinition<?, ?> method = serviceMethods.get(methodName);
        if (method == null) {
            throw new IllegalArgumentException(
                "Unknown method: " + methodName + " on service: " + serviceName);
        }
        return method;
    }

    /** Builder for {@link ConnectGrpcBridge}. */
    public static final class Builder {
        private final Map<String, ConnectServiceDefinition> serviceDefinitions =
            new LinkedHashMap<>();
        private final Map<String, Map<String, ServerMethodDefinition<?, ?>>> methods =
            new LinkedHashMap<>();

        /**
         * Where calls run; {@code null} selects direct mode. Virtual threads by default: service
         * code may block, and one thread per call costs almost nothing.
         */
        private @Nullable Executor serviceExecutor = Executors.newVirtualThreadPerTaskExecutor();

        private Builder() {}

        /** Adds a gRPC service to the bridge. */
        public Builder addService(BindableService service) {
            GrpcServiceAdapter.AdaptedService adapted = GrpcServiceAdapter.adapt(service);
            String serviceName = adapted.definition().serviceName();
            if (serviceDefinitions.containsKey(serviceName)) {
                throw new IllegalArgumentException("Duplicate service: " + serviceName);
            }
            serviceDefinitions.put(serviceName, adapted.definition());
            methods.put(serviceName, adapted.methods());
            return this;
        }

        /**
         * Runs calls directly on the Netty event loop, with no executor in between.
         *
         * <p>Saves a thread hand-off per call, at the price of the loop: listener callbacks, and
         * therefore all service and interceptor code, run on it. A single blocking call - a database
         * round trip, a lock, {@code Thread.sleep} - stalls every other call sharing that loop.
         *
         * <p>Suitable for services that are genuinely non-blocking, and for tests, where it also
         * makes a call complete synchronously within {@code writeInbound}.
         */
        public Builder withDirectExecutor() {
            log.warn("Switching to direct Netty EventLoop executor: " +
                "interceptors and services ARE NOT ALLOWED to use any blocking operations.");
            serviceExecutor = null;
            return this;
        }

        /**
         * Runs calls on {@code serviceExecutor} instead of the default virtual threads, so service
         * code may block without stalling the event loop.
         *
         * <p>The executor is shared by every call; each call wraps it in its own serializer, so it
         * need not order anything itself, and it may run consecutive tasks of one call on different
         * threads. One requirement it does have: <b>capacity to spare while calls are in flight.</b>
         * Cancellation is delivered through this executor rather than the per-call serializer,
         * precisely so that it cannot queue behind blocked service code - the same split gRPC makes
         * at {@code ServerImpl:896}. A bounded pool with every thread sitting in service code
         * therefore starves the cancellation instead: the client is already gone and nothing tells
         * the service. Prefer an unbounded executor; gRPC's own default is a cached thread pool for
         * this reason.
         *
         * <p>An executor that runs tasks on the event loop of a channel it serves -
         * {@code Runnable::run}, or an {@code EventLoopGroup} those loops belong to - is safe but
         * pointless: the bridge cannot tell such an executor apart from any other, so it posts every
         * write rather than risk mixing inline and posted ones, and the hand-off it was meant to
         * avoid happens anyway. Use {@link #withDirectExecutor()} instead, which is built for it.
         */
        public Builder withServiceExecutor(Executor serviceExecutor) {
            this.serviceExecutor = serviceExecutor;
            return this;
        }

        /** Builds an immutable {@link ConnectGrpcBridge}. */
        public ConnectGrpcBridge build() {
            if (serviceDefinitions.isEmpty()) {
                throw new IllegalStateException("At least one service must be added");
            }
            return new ConnectGrpcBridge(serviceDefinitions, methods, serviceExecutor);
        }
    }
}
