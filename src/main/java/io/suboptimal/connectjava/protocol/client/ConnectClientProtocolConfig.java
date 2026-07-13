package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.protocol.ConnectCallHandlerFactory;

import java.util.List;

public record ConnectClientProtocolConfig(
    ConnectCallHandlerFactory callHandlerFactory,
    ConnectClientProtocolParameters parameters,
    ConnectCodecRegistry codecRegistry,
    ConnectCompressionRegistry compressionRegistry,
    ConnectJsonDeserializer jsonDeserializer,
    List<ConnectClientInterceptor> interceptors
) {
    public ConnectClientProtocolConfig {
        interceptors = List.copyOf(interceptors);
    }

    public static Builder builder(ConnectCallHandlerFactory callHandlerFactory,
                                  ConnectClientProtocolParameters parameters,
                                  ConnectCodecRegistry codecRegistry) {
        return new Builder(callHandlerFactory, parameters, codecRegistry);
    }

    public static final class Builder {
        private final ConnectCallHandlerFactory callHandlerFactory;
        private final ConnectClientProtocolParameters parameters;
        private final ConnectCodecRegistry codecRegistry;
        private ConnectCompressionRegistry compressionRegistry = ConnectCompressionRegistry.standard();
        private ConnectJsonDeserializer jsonDeserializer = ConnectStringBuilderJsonDeserializer.INSTANCE;
        private List<ConnectClientInterceptor> interceptors = List.of();

        private Builder(ConnectCallHandlerFactory callHandlerFactory,
                        ConnectClientProtocolParameters parameters,
                        ConnectCodecRegistry codecRegistry) {
            this.callHandlerFactory = callHandlerFactory;
            this.parameters = parameters;
            this.codecRegistry = codecRegistry;
        }

        public Builder compressionRegistry(ConnectCompressionRegistry compressionRegistry) {
            this.compressionRegistry = compressionRegistry;
            return this;
        }

        public Builder jsonDeserializer(ConnectJsonDeserializer jsonDeserializer) {
            this.jsonDeserializer = jsonDeserializer;
            return this;
        }

        public Builder interceptors(List<ConnectClientInterceptor> interceptors) {
            this.interceptors = interceptors;
            return this;
        }

        public ConnectClientProtocolConfig build() {
            return new ConnectClientProtocolConfig(
                callHandlerFactory, parameters, codecRegistry,
                compressionRegistry, jsonDeserializer, interceptors);
        }
    }
}
