package io.suboptimal.connectjava.protocol.client;

import io.netty.channel.Channel;

public class ConnectClientChannelConfigurer {
    private final ConnectClientProtocolConfig config;
    private final ConnectClientCallDispatcher callDispatcher;

    ConnectClientChannelConfigurer(ConnectClientProtocolConfig config) {
        this.config = config;
        this.callDispatcher = new ConnectClientCallDispatcher(config);
    }

    public void configure(Channel channel) {
        channel.pipeline()
            .addLast(ConnectClientPipeline.CALL_DISPATCHER, callDispatcher)
            .addLast(config.callHandlerFactory().create());
    }
}
