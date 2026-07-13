package io.suboptimal.connectjava.protocol.client;

import org.jetbrains.annotations.ApiStatus;

/**
 * Netty handler names for the client-side Connect pipeline.
 *
 * <p>Only {@link #CALL_DISPATCHER} is permanent: {@link ConnectClientChannelConfigurer} installs it
 * (and the user's HTTP client codec ahead of it) once per channel. All other handlers are per-call
 * and are (re)installed by {@link ConnectClientCallDispatcher} when a {@link
 * io.suboptimal.connectjava.api.ConnectClientCallStart} is written, then torn down at the start of
 * the next call. Because Netty processes outbound writes from tail toward head, the dispatcher sits
 * tail-most among the Connect handlers and inserts per-call handlers on its head side via {@code
 * addBefore(CALL_DISPATCHER, …)} — i.e. between the HTTP codec and the dispatcher.
 *
 * <p>Pipeline for one call (head at the top, tail at the bottom):
 * <pre>{@code
 *   HttpClientCodec                     (user-provided, permanent)
 *   AGGREGATOR_HANDLER                  (unary only: aggregates the full response)
 *   UNARY_POST_HANDLER | UNARY_GET_HANDLER | STREAMING_HANDLER   (outbound request half)
 *   UNARY_RESPONSE_HANDLER              (unary only: inbound response half)
 *   CALL_DISPATCHER                     (permanent; owns per-call pipeline mutation)
 *   <application call handler>          (permanent; from callHandlerFactory)
 * }</pre>
 */
@ApiStatus.Internal
public final class ConnectClientPipeline {
    /**
     * Permanent, tail-most Connect handler, installed once by {@link ConnectClientChannelConfigurer}.
     * Intercepts each outbound {@code ConnectClientCallStart}, removes the previous call's per-call
     * handlers, and installs the ones for the new call.
     */
    public static final String CALL_DISPATCHER        = "connectClientDispatcher";

    /**
     * {@code HttpObjectAggregator} for unary calls only, added by the dispatcher so the response
     * arrives as a single {@code FullHttpResponse}. Streaming calls consume frames incrementally
     * and do not use it. Removed at the next call start.
     */
    public static final String AGGREGATOR_HANDLER     = "connectClientAggregator";

    /**
     * Outbound request handler for unary POST, added by the dispatcher. Turns the {@code
     * ConnectClientCallStart} into an {@code HttpRequest} and then installs {@link
     * #UNARY_RESPONSE_HANDLER} right after itself to read the response.
     */
    public static final String UNARY_POST_HANDLER     = "connectClientUnaryPost";

    /**
     * Outbound request handler for unary GET, used instead of {@link #UNARY_POST_HANDLER} when the
     * call opts into GET and the method is idempotent. Otherwise behaves like the POST handler.
     */
    public static final String UNARY_GET_HANDLER      = "connectClientUnaryGet";

    /**
     * Inbound response handler for unary calls. Not installed by the dispatcher: the unary request
     * handler ({@link #UNARY_POST_HANDLER}/{@link #UNARY_GET_HANDLER}) adds it right after itself
     * once the request has been written.
     */
    public static final String UNARY_RESPONSE_HANDLER = "connectClientUnaryResponse";

    /**
     * Duplex handler for server- and client-streaming calls, added by the dispatcher. Owns both the
     * outbound request framing and the inbound envelope decoding for the streaming call.
     */
    public static final String STREAMING_HANDLER      = "connectClientStreaming";

    private ConnectClientPipeline() {}
}
