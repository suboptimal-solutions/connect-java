package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;

import java.util.List;
import java.util.Map;

/**
 * Immutable configuration for a {@link ConnectProtocol} instance.
 *
 * <p>Create an instance via {@link #builder(Map, ConnectCallHandlerFactory, ConnectProtocolParameters, ConnectCodecRegistry)}:
 * <pre>{@code
 * ConnectProtocolConfig config = ConnectProtocolConfig
 *     .builder(services, callHandlerFactory,
 *         new ConnectProtocolParameters(4 * 1024 * 1024, 1024 * 1024),
 *         myCodecRegistry)
 *     .interceptors(List.of(myInterceptor))
 *     .build();
 * registry.register("/connect/*", new ConnectProtocol(config));
 * }</pre>
 *
 * @param services            map of exact Connect service name to service definition
 * @param callHandlerFactory  creates a fresh terminal handler for each Connect RPC call
 * @param parameters          Connect request size limits
 * @param codecRegistry       registry of supported payload codecs
 * @param compressionRegistry registry of supported message compression algorithms
 * @param jsonSerializer      JSON serializer for Connect error bodies and end-stream envelopes
 * @param interceptors        Connect protocol interceptors, invoked in registration order
 */
public record ConnectProtocolConfig(
    Map<String, ConnectServiceDefinition> services,
    ConnectCallHandlerFactory callHandlerFactory,
    ConnectProtocolParameters parameters,
    ConnectCodecRegistry codecRegistry,
    ConnectCompressionRegistry compressionRegistry,
    ConnectJsonSerializer jsonSerializer,
    List<ConnectInterceptor> interceptors
) {
    /** Validates and makes all collection components immutable. */
    public ConnectProtocolConfig {
        services = Map.copyOf(services);
        interceptors = List.copyOf(interceptors);
    }

    /**
     * Returns a new builder with the required fields pre-populated.
     *
     * @param services map of exact Connect service name to service definition;
     *                 used for route and method metadata lookup
     * @param callHandlerFactory creates a fresh terminal handler for each Connect RPC call
     * @param parameters    Connect request size limits
     * @param codecRegistry registry of supported payload codecs
     */
    public static Builder builder(Map<String, ConnectServiceDefinition> services,
                                  ConnectCallHandlerFactory callHandlerFactory,
                                  ConnectProtocolParameters parameters,
                                  ConnectCodecRegistry codecRegistry) {
        return new Builder(services, callHandlerFactory, parameters, codecRegistry);
    }

    /** Builder for {@link ConnectProtocolConfig}. */
    public static final class Builder {

        private final Map<String, ConnectServiceDefinition> services;
        private final ConnectCallHandlerFactory callHandlerFactory;
        private final ConnectProtocolParameters parameters;
        private final ConnectCodecRegistry codecRegistry;
        private ConnectCompressionRegistry compressionRegistry = ConnectCompressionRegistry.standard();
        private ConnectJsonSerializer jsonSerializer = ConnectStringBuilderJsonSerializer.INSTANCE;
        private List<ConnectInterceptor> interceptors = List.of();

        private Builder(Map<String, ConnectServiceDefinition> services,
                        ConnectCallHandlerFactory callHandlerFactory,
                        ConnectProtocolParameters parameters,
                        ConnectCodecRegistry codecRegistry) {
            this.services = services;
            this.callHandlerFactory = callHandlerFactory;
            this.parameters = parameters;
            this.codecRegistry = codecRegistry;
        }

        /**
         * Overrides the message compression registry.
         * Default: {@link ConnectCompressionRegistry#standard()}.
         */
        public Builder compressionRegistry(ConnectCompressionRegistry compressionRegistry) {
            this.compressionRegistry = compressionRegistry;
            return this;
        }

        /**
         * Overrides the JSON serializer used for Connect error bodies and end-stream envelopes.
         * Default: {@link ConnectStringBuilderJsonSerializer#INSTANCE}.
         */
        public Builder jsonSerializer(ConnectJsonSerializer jsonSerializer) {
            this.jsonSerializer = jsonSerializer;
            return this;
        }

        /**
         * Sets the Connect protocol interceptors, invoked in registration order.
         * Default: none.
         */
        public Builder interceptors(List<ConnectInterceptor> interceptors) {
            this.interceptors = interceptors;
            return this;
        }

        /**
         * Returns an immutable {@link ConnectProtocolConfig}.
         *
         * @throws NullPointerException if any required component is {@code null}
         */
        public ConnectProtocolConfig build() {
            return new ConnectProtocolConfig(
                services, callHandlerFactory, parameters,
                codecRegistry, compressionRegistry, jsonSerializer, interceptors);
        }
    }
}
