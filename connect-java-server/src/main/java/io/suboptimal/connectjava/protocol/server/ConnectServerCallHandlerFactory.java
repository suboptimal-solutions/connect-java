package io.suboptimal.connectjava.protocol.server;

import io.netty.channel.ChannelHandler;

/** Creates a fresh terminal handler for each routed Connect RPC call. */
@FunctionalInterface
public interface ConnectServerCallHandlerFactory {
    ChannelHandler create();
}
