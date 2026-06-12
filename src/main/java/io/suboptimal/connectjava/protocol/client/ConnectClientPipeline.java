package io.suboptimal.connectjava.protocol.client;

public final class ConnectClientPipeline {
    public static final String CALL_DISPATCHER        = "connectClientDispatcher";
    public static final String AGGREGATOR_HANDLER     = "connectClientAggregator";
    public static final String UNARY_POST_HANDLER     = "connectClientUnaryPost";
    public static final String UNARY_GET_HANDLER      = "connectClientUnaryGet";
    public static final String UNARY_RESPONSE_HANDLER = "connectClientUnaryResponse";
    public static final String STREAMING_HANDLER      = "connectClientStreaming";

    private ConnectClientPipeline() {}
}
