package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.protocol.ConnectCallHandlerFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectClientProtocolConfigTest {
    private static final ConnectCallHandlerFactory FACTORY = ChannelInboundHandlerAdapter::new;
    private static final ConnectClientProtocolParameters PARAMS =
        new ConnectClientProtocolParameters(1024, 1024);

    @Test
    void appliesDefaultsForOptionalFields() {
        ConnectClientProtocolConfig config = ConnectClientProtocolConfig
            .builder(FACTORY, PARAMS, ConnectProtobufCodecs.defaults())
            .build();

        assertThat(config.jsonDeserializer()).isSameAs(ConnectStringBuilderJsonDeserializer.INSTANCE);
        assertThat(config.compressionRegistry().supportedNames()).contains("gzip", "identity");
        assertThat(config.interceptors()).isEmpty();
    }

    @Test
    void overridesOptionalFields() {
        ConnectCompressionRegistry customCompression = ConnectCompressionRegistry.standard();
        ConnectClientInterceptor interceptor = callStart -> ConnectClientInterceptor.continueCall();

        ConnectClientProtocolConfig config = ConnectClientProtocolConfig
            .builder(FACTORY, PARAMS, ConnectProtobufCodecs.defaults())
            .compressionRegistry(customCompression)
            .jsonDeserializer(ConnectStringBuilderJsonDeserializer.INSTANCE)
            .interceptors(List.of(interceptor))
            .build();

        assertThat(config.compressionRegistry()).isSameAs(customCompression);
        assertThat(config.interceptors()).containsExactly(interceptor);
    }

    @Test
    void interceptorListIsImmutableCopy() {
        List<ConnectClientInterceptor> source = new ArrayList<>();
        source.add(callStart -> ConnectClientInterceptor.continueCall());

        ConnectClientProtocolConfig config = ConnectClientProtocolConfig
            .builder(FACTORY, PARAMS, ConnectProtobufCodecs.defaults())
            .interceptors(source)
            .build();

        source.add(callStart -> ConnectClientInterceptor.continueCall());
        assertThat(config.interceptors()).hasSize(1);
        assertThatThrownBy(() -> config.interceptors().add(callStart -> ConnectClientInterceptor.continueCall()))
            .isInstanceOf(UnsupportedOperationException.class);
    }
}
