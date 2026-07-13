package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ConnectCallHandlerFactory;

import java.util.List;
import java.util.Map;

/**
 * Immutable configuration for a {@link ConnectServerProtocol} instance.
 *
 * <p>Create an instance via {@link #builder(Map, ConnectCallHandlerFactory, ConnectServerProtocolParameters, ConnectCodecRegistry)}:
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
public record ConnectServerProtocolConfig(
    Map<String, ConnectServiceDefinition> services,
    ConnectCallHandlerFactory callHandlerFactory,
    ConnectServerProtocolParameters parameters,
    ConnectCodecRegistry codecRegistry,
    ConnectCompressionRegistry compressionRegistry,
    ConnectJsonSerializer jsonSerializer,
    List<ConnectServerInterceptor> interceptors
) {
    /** Validates and makes all collection components immutable. */
    public ConnectServerProtocolConfig {
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
                                  ConnectServerProtocolParameters parameters,
                                  ConnectCodecRegistry codecRegistry) {
        return new Builder(services, callHandlerFactory, parameters, codecRegistry);
    }

    /** Builder for {@link ConnectServerProtocolConfig}. */
    public static final class Builder {

        private final Map<String, ConnectServiceDefinition> services;
        private final ConnectCallHandlerFactory callHandlerFactory;
        private final ConnectServerProtocolParameters parameters;
        private final ConnectCodecRegistry codecRegistry;
        private ConnectCompressionRegistry compressionRegistry = ConnectCompressionRegistry.standard();
        private ConnectJsonSerializer jsonSerializer = ConnectStringBuilderJsonSerializer.INSTANCE;
        private List<ConnectServerInterceptor> interceptors = List.of();

        private Builder(Map<String, ConnectServiceDefinition> services,
                        ConnectCallHandlerFactory callHandlerFactory,
                        ConnectServerProtocolParameters parameters,
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
        public Builder interceptors(List<ConnectServerInterceptor> interceptors) {
            this.interceptors = interceptors;
            return this;
        }

        /**
         * Returns an immutable {@link ConnectServerProtocolConfig}.
         *
         * @throws NullPointerException if any required component is {@code null}
         */
        public ConnectServerProtocolConfig build() {
            return new ConnectServerProtocolConfig(
                services, callHandlerFactory, parameters,
                codecRegistry, compressionRegistry, jsonSerializer, interceptors);
        }
    }
}
