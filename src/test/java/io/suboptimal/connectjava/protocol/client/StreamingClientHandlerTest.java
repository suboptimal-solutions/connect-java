package io.suboptimal.connectjava.protocol.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.suboptimal.connectjava.api.ConnectClientResponseStart;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.protocol.ConnectEnvelope;
import io.suboptimal.connectjava.testfixtures.StreamingRequest;
import io.suboptimal.connectjava.testfixtures.StreamingResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingClientHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.StreamingFixtureService";
    private static final ConnectMethodDefinition SERVER_STREAMING = new ConnectMethodDefinition(
        "ServerStreaming", ConnectMethodType.SERVER_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectMethodDefinition CLIENT_STREAMING = new ConnectMethodDefinition(
        "ClientStreaming", ConnectMethodType.CLIENT_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectMethodDefinition BIDI_STREAMING = new ConnectMethodDefinition(
        "Bidi", ConnectMethodType.BIDI_STREAMING, StreamingRequest.class, StreamingResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        SERVICE_NAME, List.of(SERVER_STREAMING, CLIENT_STREAMING, BIDI_STREAMING), null);

    private final ConnectCodec proto = ClientTestSupport.protoCodec();
    private EmbeddedChannel channel;
    private ClientTestSupport.RecordingObserver observer;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel();
        observer = new ClientTestSupport.RecordingObserver();
    }

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    private void install(ConnectMethodDefinition method, Map<String, List<String>> headers,
                         ConnectClientProtocolConfig config) {
        ConnectClientCallStart callStart =
            new ConnectClientCallStart(SERVICE, method, headers, false, "proto");
        channel.pipeline().addLast(new StreamingClientHandler(callStart, config, observer));
    }

    private void install(ConnectMethodDefinition method) {
        install(method, Map.of(), ClientTestSupport.config());
    }

    private void start(ConnectMethodDefinition method) {
        ConnectClientCallStart callStart =
            new ConnectClientCallStart(SERVICE, method, Map.of(), false, "proto");
        channel.writeOutbound(callStart);
    }

    private HttpResponse okStreamingResponse() {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/connect+proto");
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        return response;
    }

    private HttpContent dataFrame(StreamingResponse message) {
        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(), (byte) 0,
            ClientTestSupport.encode(proto, message));
        return new DefaultHttpContent(buf);
    }

    private HttpContent endStreamFrame(String json) {
        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(), ConnectEnvelope.FLAG_END_STREAM,
            json.getBytes(StandardCharsets.UTF_8));
        return new DefaultHttpContent(buf);
    }

    // ---- outbound ----

    @Test
    void sendsRequestHeadersOnCallStart() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);

        HttpRequest request = channel.readOutbound();
        assertThat(request.method()).isEqualTo(HttpMethod.POST);
        assertThat(request.uri()).isEqualTo("/" + SERVICE_NAME + "/ServerStreaming");
        assertThat(request.headers().get(HttpHeaderNames.CONTENT_TYPE)).isEqualTo("application/connect+proto");
        assertThat(request.headers().get(HttpHeaderNames.TRANSFER_ENCODING)).isEqualTo("chunked");
        assertThat(request.headers().get("connect-protocol-version")).isEqualTo("1");
        assertThat(request.headers().get("connect-accept-encoding")).contains("gzip");
    }

    @Test
    void encodesPayloadAsEnvelopeFrame() throws IOException {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound(); // request headers

        StreamingRequest req = StreamingRequest.newBuilder().setText("one").build();
        channel.writeOutbound(new ConnectPayload(req));

        HttpContent content = channel.readOutbound();
        ByteBuf buf = content.content();
        byte flags = buf.readByte();
        int length = buf.readInt();
        assertThat(flags).isEqualTo((byte) 0);
        byte[] payload = new byte[length];
        buf.readBytes(payload);
        assertThat(proto.decode(Unpooled.wrappedBuffer(payload), StreamingRequest.class)).isEqualTo(req);
        content.release();

        assertThat(observer.events).containsExactly("onRequestPayload");
    }

    @Test
    void endOfStreamSendsLastContent() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeOutbound(new ConnectPayload(StreamingRequest.newBuilder().setText("x").build()));
        channel.readOutbound();

        channel.writeOutbound(ConnectEndOfStream.INSTANCE);
        Object last = channel.readOutbound();
        assertThat(last).isInstanceOf(LastHttpContent.class);
        assertThat(observer.events).contains("onRequestFinished");
    }

    @Test
    void rejectsBidiStreaming() {
        install(BIDI_STREAMING);
        start(BIDI_STREAMING);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNIMPLEMENTED);
        Object outbound = channel.readOutbound();
        assertThat(outbound).isNull(); // no request sent
    }

    @Test
    void serverStreamingRejectsSecondRequest() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeOutbound(new ConnectPayload(StreamingRequest.newBuilder().setText("1").build()));
        channel.readOutbound();

        channel.writeOutbound(new ConnectPayload(StreamingRequest.newBuilder().setText("2").build()));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNIMPLEMENTED);
    }

    // ---- inbound: server-streaming ----

    @Test
    void deliversExchangeBeforePayloads() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();

        channel.writeInbound(okStreamingResponse());

        // BUG 3 regression: CallExchange must arrive before any payload.
        Object first = channel.readInbound();
        assertThat(first).isInstanceOf(ConnectClientResponseStart.class);

        channel.writeInbound(dataFrame(StreamingResponse.newBuilder().setText("a").build()));
        channel.writeInbound(dataFrame(StreamingResponse.newBuilder().setText("b").build()));
        channel.writeInbound(endStreamFrame("{}"));

        Object p1 = channel.readInbound();
        Object p2 = channel.readInbound();
        Object eos = channel.readInbound();
        assertThat(p1).isInstanceOf(ConnectPayload.class);
        assertThat(((ConnectPayload) p1).data())
            .isEqualTo(StreamingResponse.newBuilder().setText("a").build());
        assertThat(p2).isInstanceOf(ConnectPayload.class);
        assertThat(eos).isInstanceOf(ConnectEndOfStream.class);

        assertThat(observer.events)
            .containsExactly("onResponseHeaders", "onResponsePayload", "onResponsePayload", "onCallComplete");
        assertThat(observer.completeError).isNull();
    }

    @Test
    void reassemblesFrameSplitAcrossChunks() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        ByteBuf frame = ConnectEnvelope.encode(channel.alloc(), (byte) 0,
            ClientTestSupport.encode(proto, StreamingResponse.newBuilder().setText("split").build()));
        int mid = frame.readableBytes() / 2;
        ByteBuf part1 = frame.readSlice(mid).retain();
        ByteBuf part2 = frame.readSlice(frame.readableBytes()).retain();
        frame.release();

        channel.writeInbound(new DefaultHttpContent(part1));
        Object partial = channel.readInbound();
        assertThat(partial).isNull(); // not yet complete
        channel.writeInbound(new DefaultHttpContent(part2));

        Object payload = channel.readInbound();
        assertThat(payload).isInstanceOf(ConnectPayload.class);
        assertThat(((ConnectPayload) payload).data())
            .isEqualTo(StreamingResponse.newBuilder().setText("split").build());
    }

    // ---- inbound: client-streaming ----

    @Test
    void clientStreamingRejectsSecondResponse() {
        install(CLIENT_STREAMING);
        start(CLIENT_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(dataFrame(StreamingResponse.newBuilder().setText("1").build()));
        channel.readInbound(); // first payload
        channel.writeInbound(dataFrame(StreamingResponse.newBuilder().setText("2").build()));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNIMPLEMENTED);
        assertThat(((ConnectError) inbound).message()).contains("more than one response message");
    }

    // ---- inbound: errors ----

    @Test
    void rejectsResponseWithMismatchedCodec() {
        install(SERVER_STREAMING); // callStart has codecName="proto"
        start(SERVER_STREAMING);
        channel.readOutbound();

        // server responds with JSON codec while client requested proto
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/connect+json");
        response.headers().set(HttpHeaderNames.TRANSFER_ENCODING, HttpHeaderValues.CHUNKED);
        channel.writeInbound(response);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(((ConnectError) inbound).message()).contains("json");
        assertThat(observer.completeCount).isEqualTo(1);
        // no exchange must be fired — check happened before onResponseHeaders
        assertThat(observer.events).doesNotContain("onResponseHeaders");
    }

    @Test
    void rejectsCompressedDataFrameWhenCompressionNotNegotiated() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        // response without connect-content-encoding — responseEncoding stays identity
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        // data frame with FLAG_COMPRESSED, but no compression was negotiated
        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(), ConnectEnvelope.FLAG_COMPRESSED,
            new byte[] {1, 2, 3, 4});
        channel.writeInbound(new DefaultHttpContent(buf));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(((ConnectError) inbound).message()).contains("compression");
        assertThat(observer.completeCount).isEqualTo(1);
    }

    @Test
    void endStreamErrorPropagated() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(endStreamFrame("{\"error\":{\"code\":\"not_found\",\"message\":\"gone\"}}"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectEndOfStream.class);
        ConnectEndOfStream eos = (ConnectEndOfStream) inbound;
        assertThat(eos.error()).isNotNull();
        assertThat(eos.error().code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(observer.completeError).isNotNull();
    }

    @Test
    void endStreamErrorCarriesTrailers() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(endStreamFrame(
            "{\"error\":{\"code\":\"not_found\",\"message\":\"gone\"},"
                + "\"metadata\":{\"x-custom-trailer\":[\"a\",\"b\"]}}"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectEndOfStream.class);
        ConnectEndOfStream eos = (ConnectEndOfStream) inbound;
        assertThat(eos.error()).isNotNull();
        assertThat(eos.error().code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(eos.trailers().get("x-custom-trailer")).containsExactly("a", "b");
        assertThat(observer.completeError).isNotNull();

        // ровно одно терминальное сообщение, отдельного ConnectError быть не должно
        assertThat((Object) channel.readInbound()).isNull();
    }

    @Test
    void nonOkResponseFailsCall() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();

        HttpResponse response = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
        channel.writeInbound(response);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNAVAILABLE);
    }

    @Test
    void missingContentTypeOnOkFailsCall() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        channel.writeInbound(response);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(((ConnectError) inbound).message()).contains("Content-Type");
    }

    @Test
    void truncatedStreamFails() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).message()).contains("Truncated stream");
    }

    @Test
    void frameLargerThanLimitFails() {
        install(SERVER_STREAMING, Map.of(), ClientTestSupport.configWithMaxFrameBytes(4));
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(dataFrame(StreamingResponse.newBuilder().setText("way too long").build()));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.RESOURCE_EXHAUSTED);
    }

    private HttpContent compressedEndStreamFrame(String json) {
        byte[] compressed = ClientTestSupport.gzipCompress(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(),
            (byte) (ConnectEnvelope.FLAG_END_STREAM | ConnectEnvelope.FLAG_COMPRESSED), compressed);
        return new DefaultHttpContent(buf);
    }

    private HttpResponse gzipStreamingResponse() {
        HttpResponse response = okStreamingResponse();
        response.headers().set("connect-content-encoding", "gzip");
        return response;
    }

    @Test
    void compressedEndStreamSuccessIsDecompressedAndParsed() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(gzipStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(compressedEndStreamFrame("{}"));

        Object eos = channel.readInbound();
        assertThat(eos).isInstanceOf(ConnectEndOfStream.class);
        assertThat(observer.completeError).isNull();
        assertThat(observer.completeCount).isEqualTo(1);
    }

    @Test
    void compressedEndStreamErrorIsDecompressedAndParsed() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(gzipStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(compressedEndStreamFrame(
            "{\"error\":{\"code\":\"not_found\",\"message\":\"gone\"}}"));

        Object err = channel.readInbound();
        assertThat(err).isInstanceOf(ConnectEndOfStream.class);
        ConnectEndOfStream eos = (ConnectEndOfStream) err;
        assertThat(eos.error()).isNotNull();
        assertThat(eos.error().code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(observer.completeError).isNotNull();
    }

    @Test
    void corruptCompressedEndStreamFrameFails() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(gzipStreamingResponse());
        channel.readInbound(); // exchange

        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(),
            (byte) (ConnectEnvelope.FLAG_END_STREAM | ConnectEnvelope.FLAG_COMPRESSED),
            new byte[] {1, 2, 3, 4});
        channel.writeInbound(new DefaultHttpContent(buf));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).message()).contains("Decompression failed");
        assertThat(observer.completeCount).isEqualTo(1);
    }

    @Test
    void endStreamWithMissingErrorCodeIsUnknown() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(endStreamFrame("{\"error\":{\"message\":\"oops\"}}"));

        Object err = channel.readInbound();
        assertThat(err).isInstanceOf(ConnectEndOfStream.class);
        ConnectEndOfStream eos = (ConnectEndOfStream) err;
        assertThat(eos.error()).isNotNull();
        assertThat(eos.error().code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(eos.error().message()).isEqualTo("oops");
    }

    @Test
    void corruptCompressedFrameFails() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();

        HttpResponse response = okStreamingResponse();
        response.headers().set("connect-content-encoding", "gzip");
        channel.writeInbound(response);
        channel.readInbound(); // exchange

        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(), ConnectEnvelope.FLAG_COMPRESSED,
            new byte[] {1, 2, 3, 4});
        channel.writeInbound(new DefaultHttpContent(buf));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).message()).contains("Decompression failed");
    }

    // ---- lifecycle ----

    @Test
    void channelInactiveMidStreamCancels() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.pipeline().fireChannelInactive();

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.CANCELED);
        assertThat(observer.completeCount).isEqualTo(1);
    }

    @Test
    void channelInactiveAfterCompletionDoesNotCompleteTwice() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange
        channel.writeInbound(endStreamFrame("{}"));

        channel.pipeline().fireChannelInactive();

        assertThat(observer.completeCount).isEqualTo(1);
    }

    // ---- §5.1 end-stream trailers ----

    @Test
    void endStreamMetadataBecomesEndOfStreamTrailers() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.writeInbound(endStreamFrame("{\"metadata\":{\"x-foo\":[\"a\",\"b\"]}}"));

        Object eos = channel.readInbound();
        assertThat(eos).isInstanceOf(ConnectEndOfStream.class);
        assertThat(((ConnectEndOfStream) eos).trailers().get("x-foo")).containsExactly("a", "b");
        assertThat(observer.completeError).isNull();
        assertThat(observer.completeCount).isEqualTo(1);
    }

    // ---- §5.2 outbound compression ----

    @Test
    void compressesOutboundDataFrameWhenContentEncodingGzip() {
        install(SERVER_STREAMING, Map.of("content-encoding", List.of("gzip")), ClientTestSupport.config());
        start(SERVER_STREAMING);

        HttpRequest req = channel.readOutbound();
        assertThat(req.headers().get("connect-content-encoding")).isEqualTo("gzip");

        StreamingRequest msg = StreamingRequest.newBuilder().setText("zip").build();
        channel.writeOutbound(new ConnectPayload(msg));

        HttpContent content = channel.readOutbound();
        ByteBuf buf = content.content();
        byte flags = buf.readByte();
        int len = buf.readInt();

        boolean compressed = (flags & ConnectEnvelope.FLAG_COMPRESSED) != 0;
        assertThat(compressed).isTrue();

        byte[] payload = new byte[len];
        buf.readBytes(payload);
        byte[] decompressed = ClientTestSupport.gzipDecompress(payload);
        assertThat(decompressed).isEqualTo(ClientTestSupport.encode(proto, msg));
        content.release();
    }

    // ---- §5.3 client-streaming multiple requests ----

    @Test
    void clientStreamingSendsMultipleRequestFrames() throws Exception {
        install(CLIENT_STREAMING);
        start(CLIENT_STREAMING);
        channel.readOutbound(); // headers

        channel.writeOutbound(new ConnectPayload(StreamingRequest.newBuilder().setText("1").build()));
        channel.writeOutbound(new ConnectPayload(StreamingRequest.newBuilder().setText("2").build()));

        HttpContent c1 = channel.readOutbound();
        HttpContent c2 = channel.readOutbound();

        Object noError = channel.readInbound();
        assertThat(noError).isNull();

        ByteBuf buf1 = c1.content();
        buf1.readByte(); // flags
        int len1 = buf1.readInt();
        byte[] payload1 = new byte[len1];
        buf1.readBytes(payload1);
        assertThat(proto.decode(Unpooled.wrappedBuffer(payload1), StreamingRequest.class).getText()).isEqualTo("1");

        ByteBuf buf2 = c2.content();
        buf2.readByte(); // flags
        int len2 = buf2.readInt();
        byte[] payload2 = new byte[len2];
        buf2.readBytes(payload2);
        assertThat(proto.decode(Unpooled.wrappedBuffer(payload2), StreamingRequest.class).getText()).isEqualTo("2");

        assertThat(observer.events).contains("onRequestPayload", "onRequestPayload");
        c1.release();
        c2.release();
    }

    // ---- §5.4 serialization failure ----

    @Test
    void serializationFailureOnOutboundPayloadFailsCall() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();

        channel.writeOutbound(new ConnectPayload("not a protobuf message"));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(((ConnectError) inbound).message()).contains("Serialization failed");
        assertThat(observer.completeCount).isEqualTo(1);
    }

    // ---- §5.5 undecodable data frame ----

    @Test
    void undecodableDataFrameFailsCleanly() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        ByteBuf buf = ConnectEnvelope.encode(channel.alloc(), (byte) 0,
            new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        channel.writeInbound(new DefaultHttpContent(buf));

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(((ConnectError) inbound).message()).contains("Deserialization failed");
        assertThat(observer.completeCount).isEqualTo(1);
    }

    // ---- §5.6 handlerRemoved mid-stream ----

    @Test
    void handlerRemovedMidStreamCancels() {
        install(SERVER_STREAMING);
        start(SERVER_STREAMING);
        channel.readOutbound();
        channel.writeInbound(okStreamingResponse());
        channel.readInbound(); // exchange

        channel.pipeline().remove(StreamingClientHandler.class);

        Object inbound = channel.readInbound();
        assertThat(inbound).isInstanceOf(ConnectError.class);
        assertThat(((ConnectError) inbound).code()).isEqualTo(ConnectErrorCode.CANCELED);
        assertThat(observer.completeCount).isEqualTo(1);
    }

    // ---- §6.2 streaming timeout ----

    @Test
    void sendsConnectTimeoutHeaderWhenTimeoutSet() {
        ConnectClientCallStart cs = new ConnectClientCallStart(
            SERVICE, SERVER_STREAMING, Map.of(), false, "proto", 1500L);
        channel.pipeline().addLast(new StreamingClientHandler(cs, ClientTestSupport.config(), observer));
        channel.writeOutbound(cs);

        HttpRequest request = channel.readOutbound();
        assertThat(request.headers().get("connect-timeout-ms")).isEqualTo("1500");
    }
}
