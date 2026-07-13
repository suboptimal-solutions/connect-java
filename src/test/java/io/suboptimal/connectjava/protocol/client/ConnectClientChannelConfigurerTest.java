package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientChannelConfigurerTest {
    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private static ConnectClientProtocolConfig freshConfig() {
        return ConnectClientProtocolConfig.builder(
            ChannelInboundHandlerAdapter::new,
            new ConnectClientProtocolParameters(4 * 1024 * 1024, 1024 * 1024),
            ConnectProtobufCodecs.defaults()).build();
    }

    @Test
    void installsCallDispatcher() {
        new ConnectClientChannelConfigurer(freshConfig()).configure(channel);

        assertThat(channel.pipeline().get(ConnectClientPipeline.CALL_DISPATCHER)).isNotNull();
    }

    @Test
    void installsTerminalHandlerAfterDispatcher() {
        new ConnectClientChannelConfigurer(freshConfig()).configure(channel);

        assertThat(channel.pipeline().names()).contains(ConnectClientPipeline.CALL_DISPATCHER);
        assertThat(channel.pipeline().last()).isNotInstanceOf(ConnectClientCallDispatcher.class);
    }

    @Test
    void dispatcherIsReusedAcrossChannels() {
        ConnectClientChannelConfigurer cfg = new ConnectClientChannelConfigurer(freshConfig());

        EmbeddedChannel chA = new EmbeddedChannel();
        EmbeddedChannel chB = new EmbeddedChannel();
        try {
            cfg.configure(chA);
            cfg.configure(chB);

            assertThat(chA.pipeline().get(ConnectClientPipeline.CALL_DISPATCHER))
                .isSameAs(chB.pipeline().get(ConnectClientPipeline.CALL_DISPATCHER));
        } finally {
            chA.finishAndReleaseAll();
            chB.finishAndReleaseAll();
        }
    }
}
