package io.suboptimal.connectjava.api;

public sealed interface ConnectMessage permits
    ConnectCallExchange,
    ConnectPayload,
    ConnectEndOfStream,
    ConnectError
{}
