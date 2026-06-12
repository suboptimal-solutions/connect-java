package io.suboptimal.connectjava.protocol.client;

public record ConnectClientProtocolParameters(
    int maxResponseBytes,
    int maxFrameBytes
) {
    public ConnectClientProtocolParameters {
        if (maxResponseBytes <= 0) throw new IllegalArgumentException("maxResponseBytes must be > 0");
        if (maxFrameBytes <= 0) throw new IllegalArgumentException("maxFrameBytes must be > 0");
    }
}
