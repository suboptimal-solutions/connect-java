package io.suboptimal.connectjava.protocol.client;

public class ConnectClientProtocol {
    private final ConnectClientChannelConfigurer http1Configurer;

    public ConnectClientProtocol(ConnectClientProtocolConfig config) {
        this.http1Configurer = new ConnectClientChannelConfigurer(config);
    }

    public ConnectClientChannelConfigurer http1() {
        return http1Configurer;
    }
}
