package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectChannelConfigurerTest {
    private static final String EXAMPLE_URL = "https://app.example";
    private static final ConnectProtocolParameters PARAMETERS =
        new ConnectProtocolParameters(1024, 64,
            ConnectCorsParameters.defaultsForOrigins(Set.of(EXAMPLE_URL)));

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Nested
    class Http1 {
        @Test
        void installsExpectedPipeline() {
            configureChannel(ConnectTransport.HTTP_1_1);

            assertThat(channel.pipeline().get(ConnectPipeline.CORS_HANDLER))
                .isInstanceOf(CorsHandler.class);
            String terminalName = channel.pipeline().context(TerminalHandler.class).name();
            assertThat(channel.pipeline().names())
                .containsSequence(
                    ConnectPipeline.CORS_HANDLER,
                    ConnectPipeline.ROUTING_HANDLER,
                    terminalName);
        }
    }

    @Nested
    class Http2 {
        @Test
        void installsExpectedPipeline() {
            configureChannel(ConnectTransport.HTTP_2);

            assertThat(channel.pipeline().get(ConnectPipeline.CORS_HANDLER))
                .isInstanceOf(CorsHandler.class);
            String adapterName = channel.pipeline().context(Http2StreamFrameToHttpObjectCodec.class).name();
            String terminalName = channel.pipeline().context(TerminalHandler.class).name();
            assertThat(channel.pipeline().names())
                .containsSequence(
                    adapterName,
                    ConnectPipeline.CORS_HANDLER,
                    ConnectPipeline.ROUTING_HANDLER,
                    terminalName);
        }
    }

    @Nested
    class Cors {
        @BeforeEach
        void setUpChannel() {
            configureChannel(ConnectTransport.HTTP_1_1);
        }

        @Test
        void acceptsPreflightFromAllowedOrigin() {
            DefaultHttpRequest preflight = new DefaultHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/missing.Service/Missing");
            preflight.headers()
                .set(HttpHeaderNames.ORIGIN, EXAMPLE_URL)
                .set(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD, HttpMethod.POST.name())
                .set(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS,
                    "content-type,connect-protocol-version");

            channel.writeInbound(preflight);
            Object response = channel.readOutbound();

            assertThat(response)
                .isNotNull()
                .isInstanceOf(HttpResponse.class);

            HttpResponse httpResponse = (HttpResponse) response;
            assertThat(httpResponse.status()).isEqualTo(HttpResponseStatus.OK);
            assertThat(httpResponse.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN))
                .isEqualTo(EXAMPLE_URL);
            assertThat(httpResponse.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS))
                .isNotNull();
            assertThat(httpResponse.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS))
                .isNotNull();
        }

        @Test
        void deniesToOriginNotInAllowlist() {
            DefaultHttpRequest preflight = new DefaultHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/missing.Service/Missing");
            preflight.headers()
                .set(HttpHeaderNames.ORIGIN, "https://evil.example")
                .set(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD, HttpMethod.POST.name())
                .set(HttpHeaderNames.ACCESS_CONTROL_REQUEST_HEADERS, HttpHeaderNames.CONTENT_TYPE);

            channel.writeInbound(preflight);
            var response = channel.readOutbound();
            assertThat(response).isNotNull();

            HttpResponse httpResponse =
                (HttpResponse) response;
            assertThat(httpResponse.status()).isEqualTo(HttpResponseStatus.OK);
            assertThat(httpResponse.headers().contains(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN))
                .isFalse();
        }
    }

    void configureChannel(ConnectTransport transport) {
        new ConnectChannelConfigurer(transport,
            ConnectProtocolConfig.builder(Map.of(), TerminalHandler::new, PARAMETERS, ConnectCodecRegistry.builder().build()).build())
            .configure(channel);
    }

    private static final class TerminalHandler extends ChannelInboundHandlerAdapter {}
}
