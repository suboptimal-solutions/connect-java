package io.suboptimal.connectjava.api;

public sealed interface ConnectMessage permits
    ConnectCallExchange,
    ConnectClientResponseStart,
    ConnectPayload,
    ConnectEndOfStream,
    ConnectError
{}
