package io.suboptimal.connectjava.api;

/**
 * Terminal signal indicating the successful end of one side of an RPC payload stream.
 */
public record ConnectEndOfStream() implements ConnectMessage {
    public static final ConnectEndOfStream INSTANCE = new ConnectEndOfStream();
}
