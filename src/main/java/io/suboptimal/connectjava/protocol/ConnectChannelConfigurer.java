package io.suboptimal.connectjava.protocol;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;

public class ConnectChannelConfigurer {
    private final ConnectTransport transport;
    private final ConnectProtocolConfig config;
    private final RoutingHandler routingHandler;

    ConnectChannelConfigurer(ConnectTransport transport, ConnectProtocolConfig config) {
        this.transport = transport;
        this.config = config;
        this.routingHandler = new RoutingHandler(transport, config);
    }

    public void configure(Channel channel) {
        if (transport == ConnectTransport.HTTP_2) {
            channel.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(true));
        }
        if (config.parameters().cors().enabled()) {
            channel.pipeline().addLast(ConnectPipeline.CORS_HANDLER, buildCorsHandler());
        }
        channel.pipeline()
            .addLast(ConnectPipeline.ROUTING_HANDLER, routingHandler)
            .addLast(config.callHandlerFactory().create());
    }

    private CorsHandler buildCorsHandler() {
        CorsConfigBuilder builder = config.parameters().cors().anyOrigin()
            ? CorsConfigBuilder.forAnyOrigin()
            : CorsConfigBuilder.forOrigins(config.parameters().cors().allowedOrigins().toArray(String[]::new));

        HttpMethod[] methods = config.parameters().cors().allowedMethods().stream()
            .map(HttpMethod::valueOf)
            .toArray(HttpMethod[]::new);
        builder.allowedRequestMethods(methods);

        if (!config.parameters().cors().allowedHeaders().isEmpty()) {
            builder.allowedRequestHeaders(config.parameters().cors().allowedHeaders().toArray(String[]::new));
        }
        if (!config.parameters().cors().exposedHeaders().isEmpty()) {
            builder.exposeHeaders(config.parameters().cors().exposedHeaders().toArray(String[]::new));
        }
        builder.maxAge(config.parameters().cors().maxAgeSeconds());
        if (config.parameters().cors().allowCredentials()) {
            builder.allowCredentials();
        }
        if (config.parameters().cors().allowPrivateNetwork()) {
            builder.allowPrivateNetwork();
        }

        CorsConfig config = builder.build();
        return new CorsHandler(config);
    }
}
