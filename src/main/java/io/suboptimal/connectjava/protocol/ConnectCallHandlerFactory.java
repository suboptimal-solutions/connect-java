package io.suboptimal.connectjava.protocol;

import io.netty.channel.ChannelHandler;

/** Creates a fresh terminal handler for each routed Connect RPC call. */
@FunctionalInterface
public interface ConnectCallHandlerFactory {
    ChannelHandler create();
}
