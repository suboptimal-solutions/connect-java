package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;

public record ConnectClientResponseStart (
    ConnectServiceDefinition serviceDefinition,
    ConnectMethodDefinition methodDefinition,
    ConnectResponseMeta responseMeta
) implements ConnectMessage {}
