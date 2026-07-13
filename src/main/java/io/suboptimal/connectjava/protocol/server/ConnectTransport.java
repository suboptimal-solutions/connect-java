package io.suboptimal.connectjava.protocol.server;

/**
 * HTTP transport version a {@link ConnectServerChannelConfigurer} and its
 * {@link RoutingServerHandler} are wired for.
 */
enum ConnectTransport {
    HTTP_1_1,
    HTTP_2
}
