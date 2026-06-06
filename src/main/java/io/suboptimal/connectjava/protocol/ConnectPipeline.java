package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectPayload;

/**
 * Netty pipeline handler names used by the Connect protocol implementation.
 * Exposed so that {@link io.netty.channel.ChannelPipeline#addAfter} (and friends)
 * can reference handlers by name without holding direct references.
 */
public final class ConnectPipeline {
    /** Name of the {@link CorsHandler} optionally installed by {@link ConnectChannelConfigurer}. */
    public static final String CORS_HANDLER = "connectCors";

    /** Name of the {@link RoutingHandler} installed by {@link ConnectChannelConfigurer}. */
    public static final String ROUTING_HANDLER = "connectRouting";

    /**
     * Name of the standard Netty {@link HttpObjectAggregator} that {@link RoutingHandler}
     * installs ahead of unary handlers. Connect itself does not
     * provide a custom aggregator — only this name slot in the pipeline.
     */
    public static final String AGGREGATOR_HANDLER = "connectAggregator";

    /**
     * Name of the unary GET request-processing handler installed after {@link #AGGREGATOR_HANDLER}.
     *
     * <p>The handler validates the Connect Unary-Get query, decodes one request payload,
     * fires inbound RPC messages, and installs {@link #UNARY_RESPONSE_HANDLER} for the
     * outbound response.
     */
    public static final String UNARY_GET_REQUEST_HANDLER = "connectUnaryGetRequest";

    /**
     * Name of the unary POST request-processing handler installed after {@link #AGGREGATOR_HANDLER}.
     *
     * <p>The handler validates the Connect unary POST headers and body, decodes one
     * request payload, fires inbound RPC messages, and installs {@link #UNARY_RESPONSE_HANDLER}
     * for the outbound response.
     */
    public static final String UNARY_POST_REQUEST_HANDLER = "connectUnaryPostRequest";

    /**
     * Name of the unary outbound response handler installed by successful unary request handlers.
     *
     * <p>This handler consumes outbound {@link ConnectPayload}, {@link ConnectError}, and
     * {@link ConnectEndOfStream} messages from the terminal Connect call handler
     * and serializes exactly one Connect unary HTTP response.
     */
    public static final String UNARY_RESPONSE_HANDLER = "connectUnaryResponse";

    /** Name of the {@link StreamingHandler} installed on the streaming path. */
    public static final String STREAMING_HANDLER = "connectStreaming";

    private ConnectPipeline() {}
}
