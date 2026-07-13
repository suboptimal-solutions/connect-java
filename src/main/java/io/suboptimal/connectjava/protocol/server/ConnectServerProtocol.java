package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.protocol.ConnectCallHandlerFactory;

/**
 * Buf Connect protocol over HTTP/1.1 and HTTP/2.
 *
 * <p>Supports:
 * <ul>
 *   <li>Unary: {@code application/proto} and {@code application/json}.</li>
 *   <li>Streaming: {@code application/connect+proto} and {@code application/connect+json}
 *       with per-envelope compression.</li>
 * </ul>
 *
 * <p>Bidirectional streaming ({@code STREAM_STREAM}) requires HTTP/2; bidi requests over
 * HTTP/1.1 are rejected with {@code 505 HTTP Version Not Supported} and
 * {@code Connection: close}.
 *
 * <p>Connect service and method names are case-sensitive. The service-definition map must
 * therefore be keyed by the exact Connect service names accepted on the wire.
 *
 * <p>Wire it into Netty channel initializers via {@link #http1()} and {@link #http2()}:
 * <pre>{@code
 * ConnectProtocol protocol = new ConnectProtocol(config);
 *
 * // HTTP/1.1 channel initializer — channel must have an HTTP/1.1 codec installed:
 * protocol.http1().configure(channel);
 *
 * // HTTP/2 stream channel initializer — channel must be a child of Http2MultiplexHandler:
 * protocol.http2().configure(channel);
 * }</pre>
 *
 * @see ConnectServerProtocolConfig
 */
public class ConnectServerProtocol {
    private final ConnectServerChannelConfigurer http1Configurer;
    private final ConnectServerChannelConfigurer http2Configurer;

    /**
     * Creates a new {@link ConnectServerProtocol} from the given configuration.
     *
     * <p>Use {@link ConnectServerProtocolConfig#builder(java.util.Map, ConnectCallHandlerFactory,
     * ConnectServerProtocolParameters, io.suboptimal.connectjava.codec.ConnectCodecRegistry)}
     * to build a configuration with the desired options. ConnectCompression, JSON serializer,
     * and interceptors carry defaults and only need to be specified when overriding
     * those defaults.
     *
     * @param config protocol configuration; must not be {@code null}
     * @see ConnectServerProtocolConfig
     */
    public ConnectServerProtocol(ConnectServerProtocolConfig config) {
        this.http1Configurer = new ConnectServerChannelConfigurer(ConnectTransport.HTTP_1_1, config);
        this.http2Configurer = new ConnectServerChannelConfigurer(ConnectTransport.HTTP_2, config);
    }

    /**
     * Returns the configurer for HTTP/1.1 channels.
     *
     * <p>The channel passed to {@link ConnectServerChannelConfigurer#configure(io.netty.channel.Channel)}
     * must already have an HTTP/1.1 codec installed (e.g. {@code HttpServerCodec}).
     * The configurer appends an optional CORS handler, the routing handler, and the terminal
     * call handler to the pipeline.
     */
    public ConnectServerChannelConfigurer http1() {
        return http1Configurer;
    }

    /**
     * Returns the configurer for HTTP/2 stream channels.
     *
     * <p>The channel passed to {@link ConnectServerChannelConfigurer#configure(io.netty.channel.Channel)}
     * must be a child channel produced by {@code Http2MultiplexHandler}. The configurer prepends
     * {@code Http2StreamFrameToHttpObjectCodec}, then adds an optional CORS handler, the routing
     * handler, and the terminal call handler.
     */
    public ConnectServerChannelConfigurer http2() {
        return http2Configurer;
    }
}
