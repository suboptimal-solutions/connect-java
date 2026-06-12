package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UnaryResponseClientHandlerTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "connectjava.test.v1.UnaryPostFixtureService", List.of(METHOD), null);
    private static final UnaryPostResponse RESPONSE =
        UnaryPostResponse.newBuilder().setText("pong").build();

    private final ConnectClientProtocolConfig config = ClientTestSupport.config();
    private final ConnectCodec proto = ClientTestSupport.protoCodec();

    private ConnectClientCallStart callStart() {
        return new ConnectClientCallStart(SERVICE, METHOD, Map.of(), false, "proto");
    }

    private EmbeddedChannel newChannel(ClientTestSupport.RecordingObserver observer) {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(new UnaryResponseClientHandler(callStart(), config, observer));
        return channel;
    }

    private FullHttpResponse response(int status, byte[] body, String contentType, String contentEncoding) {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status), Unpooled.wrappedBuffer(body));
        if (contentType != null) {
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        }
        if (contentEncoding != null) {
            response.headers().set(HttpHeaderNames.CONTENT_ENCODING, contentEncoding);
        }
        return response;
    }

    @Test
    void deliversExchangePayloadEndOfStreamOnSuccess() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(200, ClientTestSupport.encode(proto, RESPONSE),
            "application/proto", null));

        Object exchange = channel.readInbound();
        assertThat(exchange).isInstanceOf(ConnectClientResponseStart.class);
        assertThat(((ConnectClientResponseStart) exchange).responseMeta().statusCode()).isEqualTo(200);

        Object payload = channel.readInbound();
        assertThat(payload).isInstanceOf(ConnectPayload.class);
        assertThat(((ConnectPayload) payload).data()).isEqualTo(RESPONSE);

        Object endOfStream = channel.readInbound();
        assertThat(endOfStream).isSameAs(ConnectEndOfStream.INSTANCE);

        assertThat(observer.events)
            .containsExactly("onResponseHeaders", "onResponsePayload", "onCallComplete");
        assertThat(observer.completeError).isNull();

        channel.finishAndReleaseAll();
    }

    @Test
    void decompressesGzipResponse() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        byte[] gzipped = ClientTestSupport.gzipCompress(ClientTestSupport.encode(proto, RESPONSE));
        channel.writeInbound(response(200, gzipped, "application/proto", "gzip"));

        channel.readInbound(); // exchange
        ConnectPayload payload = channel.readInbound();
        assertThat(payload.data()).isEqualTo(RESPONSE);

        channel.finishAndReleaseAll();
    }

    @Test
    void mapsEmptyBody404ToUnimplemented() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(404, new byte[0], null, null));

        Object first = channel.readInbound();
        assertThat(first).isInstanceOf(ConnectClientResponseStart.class);
        Object second = channel.readInbound();
        assertThat(second).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) second).code()).isEqualTo(ConnectErrorCode.UNIMPLEMENTED);
        assertThat(observer.completeCount).isEqualTo(1);
        Object next = channel.readInbound();
        assertThat(next).isNull();

        channel.finishAndReleaseAll();
    }

    @Test
    void mapsHttpStatusesToErrorCodes() {
        Map<Integer, ConnectErrorCode> cases = Map.of(
            400, ConnectErrorCode.INTERNAL,
            401, ConnectErrorCode.UNAUTHENTICATED,
            403, ConnectErrorCode.PERMISSION_DENIED,
            429, ConnectErrorCode.UNAVAILABLE,
            500, ConnectErrorCode.UNKNOWN,
            503, ConnectErrorCode.UNAVAILABLE,
            418, ConnectErrorCode.UNKNOWN);

        cases.forEach((status, expected) -> {
            var observer = new ClientTestSupport.RecordingObserver();
            EmbeddedChannel channel = newChannel(observer);
            channel.writeInbound(response(status, new byte[0], null, null));
            Object first = channel.readInbound();
            assertThat(first).isInstanceOf(ConnectClientResponseStart.class);
            Object second = channel.readInbound();
            assertThat(second).isInstanceOf(ConnectError.class);
            assertThat(((ConnectError) second).code())
                .as("status %d", status)
                .isEqualTo(expected);
            channel.finishAndReleaseAll();
        });
    }

    @Test
    void prefersConnectErrorBodyOverHttpStatus() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        byte[] body = "{\"code\":\"not_found\",\"message\":\"nope\"}".getBytes(StandardCharsets.UTF_8);
        channel.writeInbound(response(400, body, "application/json", null));

        assertThat((Object) channel.readInbound()).isInstanceOf(ConnectClientResponseStart.class);
        ConnectError error = channel.readInbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(error.message()).isEqualTo("nope");

        channel.finishAndReleaseAll();
    }

    @Test
    void rejectsResponseWithMismatchedCodec() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer); // callStart has codecName="proto"

        // server responds with JSON codec while client requested proto
        channel.writeInbound(response(200, new byte[0], "application/json", null));

        ConnectClientResponseStart responseStart = channel.readInbound();
        assertThat(responseStart).isNotNull();

        ConnectError error = channel.readInbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.message()).contains("json");
        assertThat(error.message()).contains("proto");
        assertThat(observer.completeCount).isEqualTo(1);

        channel.finishAndReleaseAll();
    }

    @Test
    void failsOnMissingContentTypeForSuccess() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(200, ClientTestSupport.encode(proto, RESPONSE), null, null));

        ConnectClientResponseStart responseStart = channel.readInbound();
        assertThat(responseStart).isNotNull();

        ConnectError error = channel.readInbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(error.message()).contains("Content-Type");

        channel.finishAndReleaseAll();
    }

    @Test
    void failsCleanlyOnUndecodableBody() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        // Invalid protobuf payload — decode throws; must surface as a clean ConnectError,
        // never an IllegalReferenceCountException from a double release (regression: BUG 1).
        channel.writeInbound(response(200, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
            "application/proto", null));

        ConnectClientResponseStart responseStart = channel.readInbound();
        assertThat(responseStart).isNotNull();

        ConnectError error = channel.readInbound();
        assertThat((error).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat((error).message()).contains("Deserialization failed");
        assertThat(observer.completeCount).isEqualTo(1);

        channel.finishAndReleaseAll();
    }

    @Test
    void failsCleanlyOnCorruptGzip() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(200, new byte[] {1, 2, 3, 4, 5}, "application/proto", "gzip"));

        ConnectClientResponseStart responseStart = channel.readInbound();
        assertThat(responseStart).isNotNull();

        ConnectError error = channel.readInbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.message()).contains("Decompression failed");

        channel.finishAndReleaseAll();
    }

    @Test
    void channelInactiveBeforeResponseCancelsCall() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.pipeline().fireChannelInactive();

        ConnectError error = channel.readInbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.CANCELED);
        assertThat(observer.completeCount).isEqualTo(1);

        channel.finishAndReleaseAll();
    }

    @Test
    void deliversTrailersFromTrailerPrefixedHeaders() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        byte[] body = ClientTestSupport.encode(proto, RESPONSE);
        FullHttpResponse resp = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(body));
        resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/proto");
        resp.headers().set("trailer-x-foo", "bar");
        resp.headers().set("x-plain", "v");

        channel.writeInbound(resp);

        ConnectClientResponseStart start = channel.readInbound();
        ConnectResponseMeta meta = start.responseMeta();

        assertThat(meta.trailers().get("x-foo")).containsExactly("bar");
        assertThat(meta.headers()).containsKey("x-plain");
        assertThat(meta.headers()).doesNotContainKey("x-foo");

        channel.readInbound(); // payload
        channel.readInbound(); // EOS
        channel.finishAndReleaseAll();
    }

    @Test
    void parsesErrorDetailsFromUnaryErrorBody() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        byte[] detailBytes = new byte[]{1, 2, 3};
        String b64 = Base64.getEncoder().encodeToString(detailBytes);
        byte[] body = ("{\"code\":\"not_found\",\"message\":\"nope\",\"details\":"
            + "[{\"type\":\"google.rpc.RetryInfo\",\"value\":\"" + b64 + "\"}]}")
            .getBytes(StandardCharsets.UTF_8);

        channel.writeInbound(response(400, body, "application/json", null));

        channel.readInbound(); // ConnectClientResponseStart
        ConnectError error = channel.readInbound();

        assertThat(error.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(error.message()).isEqualTo("nope");
        assertThat(error.details()).hasSize(1);
        assertThat(error.details().get(0).type()).isEqualTo("google.rpc.RetryInfo");
        assertThat(error.details().get(0).value()).isEqualTo(detailBytes);

        channel.finishAndReleaseAll();
    }

    @Test
    void decompressesGzipErrorBody() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        byte[] json = "{\"code\":\"out_of_range\",\"message\":\"oops\"}"
            .getBytes(StandardCharsets.UTF_8);
        channel.writeInbound(response(422, ClientTestSupport.gzipCompress(json),
            "application/json", "gzip"));

        channel.readInbound(); // ConnectClientResponseStart
        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.OUT_OF_RANGE);
        assertThat(((ConnectError) inbound).message()).isEqualTo("oops");
    }

    @Test
    void fallsBackToHttpStatusWhenErrorBodyIsCorruptGzip() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(503, new byte[] {1, 2, 3, 4, 5},
            "application/json", "gzip"));

        channel.readInbound(); // ConnectClientResponseStart
        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNAVAILABLE);
    }

    @Test
    void channelInactiveAfterSuccessDoesNotCompleteTwice() {
        var observer = new ClientTestSupport.RecordingObserver();
        EmbeddedChannel channel = newChannel(observer);

        channel.writeInbound(response(200, ClientTestSupport.encode(proto, RESPONSE),
            "application/proto", null));
        channel.pipeline().fireChannelInactive();

        assertThat(observer.completeCount).isEqualTo(1);

        channel.finishAndReleaseAll();
    }
}
