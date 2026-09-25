package io.suboptimal.connectjava.grpcbridge;

import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;

import java.util.List;
import java.util.Map;

/**
 * Builds the {@link ConnectCallExchange} that starts a call, the way connect-java would.
 *
 * <p>Most tests reach this through {@link GrpcCallHarness#exchange}, which supplies the bridge.
 * The factories here take one explicitly, for the few tests that drive a channel without a harness.
 */
class TestExchanges {
    private TestExchanges() {}

    /** The single service the bridge was built with. */
    static ConnectServiceDefinition serviceOf(ConnectGrpcBridge bridge) {
        return bridge.serviceDefinitions().values().iterator().next();
    }

    static ConnectCallExchange create(
        ConnectGrpcBridge bridge, String methodName, Map<String, List<String>> headers)
    {
        return create(bridge, methodName, headers,
            new TestResponseHeadersBuilder(), new TestResponseTrailersBuilder());
    }

    static ConnectCallExchange create(
        ConnectGrpcBridge bridge, String methodName, Map<String, List<String>> headers,
        ConnectResponseHeadersBuilder headersBuilder,
        ConnectResponseTrailersBuilder trailersBuilder)
    {
        ConnectServiceDefinition sd = serviceOf(bridge);
        ConnectMethodDefinition md = sd.methods().get(methodName);
        return new ConnectCallExchange(sd, md,
            new ConnectRequestMeta(headers), headersBuilder, trailersBuilder);
    }
}
