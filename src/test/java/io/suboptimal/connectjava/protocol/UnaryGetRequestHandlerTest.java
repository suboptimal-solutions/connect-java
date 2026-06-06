package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.codec.ConnectCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodec;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.testfixtures.UnaryGetRequest;
import io.suboptimal.connectjava.testfixtures.UnaryGetResponse;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class UnaryGetRequestHandlerTest {
    private static final String SERVICE_NAME = "connectjava.test.v1.UnaryGetFixtureService";

    private static final ConnectMethodDefinition UNARY_METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryGetRequest.class, UnaryGetResponse.class, true);

    private static final ConnectMethodDefinition MUTATING_UNARY_METHOD = new ConnectMethodDefinition(
        "MutatingUnary", ConnectMethodType.UNARY, UnaryGetRequest.class, UnaryGetResponse.class, false);

    private static final ConnectServiceDefinition SERVICE_DEF = new ConnectServiceDefinition(
        SERVICE_NAME,
        List.of(UNARY_METHOD, MUTATING_UNARY_METHOD),
        null);

    private static final UnaryGetRequest REQUEST =
        UnaryGetRequest.newBuilder().setText("hello").build();

    private static final int DEFAULT_MAX_REQUEST_BYTES = 4096;
    private static final int SMALL_MAX_REQUEST_BYTES = 64;

    private EmbeddedChannel channel;
    private @Nullable ConnectCallExchange exchange;

    @BeforeEach
    void setUpChannel() {
        channel = new EmbeddedChannel();
    }

    @AfterEach
    void tearDownChannel() {
        channel.finishAndReleaseAll();
    }

    void setUpHandler(String methodName) {
        setUpHandler(methodName, DEFAULT_MAX_REQUEST_BYTES);
    }

    void setUpHandler(String methodName, int maxRequestBytes) {
        setUpHandler(methodName, maxRequestBytes, ConnectInterceptorPipeline.EMPTY);
    }

    void setUpHandler(String methodName, int maxRequestBytes,
        ConnectInterceptorPipeline interceptorPipeline)
    {
        ConnectMethodDefinition method = SERVICE_DEF.methods().get(methodName);
        exchange = new ConnectCallExchange(SERVICE_DEF, method, new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());

        channel.pipeline().addLast(ConnectPipeline.UNARY_GET_REQUEST_HANDLER,
            new UnaryGetRequestHandler(
                exchange,
                ConnectProtobufCodecs.defaults(),
                ConnectCompressionRegistry.standard(),
                maxRequestBytes,
                ConnectStringBuilderJsonSerializer.INSTANCE,
                interceptorPipeline));
    }

    @Nested
    class RequestRouting {
        @Test
        void acceptsMinimalValidJsonGet() {
            setUpHandler("Unary");
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsMinimalValidProtoGet() throws IOException {
            setUpHandler("Unary");
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String methodUri = "/Unary?encoding=proto&base64=1&message=" + base64Url(protoBytes);
            DefaultFullHttpRequest request = getRequest(methodUri);

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertThat((Object) channel.readOutbound()).isNull();
            assertThat(channel.isOpen()).isTrue();
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsQueryParamsInAnyOrder() {
            setUpHandler("Unary");
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?message=" + urlEncode(messageJson) + "&encoding=json";

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void allowsUnknownQueryParams() {
            setUpHandler("Unary");
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message="
                + urlEncode(messageJson) + "&foo=bar&baz=qux";

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsMutatingMethodAs405() {
            setUpHandler("MutatingUnary");
            String methodUri = "/MutatingUnary?encoding=json&message=%7B%7D";

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertMethodNotAllowedWithAllowPost(response);
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }

        @Test
        void rejectsMutatingMethodBeforeValidatingQueryParams() {
            setUpHandler("MutatingUnary");
            String methodUri = "/MutatingUnary?encoding=json";

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertMethodNotAllowedWithAllowPost(response);
            } finally {
                response.release();
            }
        }
    }

    @Nested
    class CodecSelection {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void selectsJsonFromEncodingParam() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void selectsProtoFromEncodingParam() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String methodUri = "/Unary?encoding=proto&base64=1&message=" + base64Url(protoBytes);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsMissingEncodingAsPlain415() {
            DefaultFullHttpRequest request = getRequest("/Unary?message=%7B%7D");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                HttpAssertions.assertThat(response).unsupportedMediaTypeError();
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }

        @Test
        void rejectsEmptyEncodingAsPlain415() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=&message=%7B%7D");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                HttpAssertions.assertThat(response).unsupportedMediaTypeError();
            } finally {
                response.release();
            }
        }

        @Test
        void rejectsUnsupportedEncodingAsPlain415() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=xml&message=%7B%7D");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                HttpAssertions.assertThat(response).unsupportedMediaTypeError();
            } finally {
                response.release();
            }
        }

        @Test
        void ignoresContentTypeForCodecSelection() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/proto");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void ignoresStreamingContentType() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/connect+json");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }
    }

    @Nested
    class MessageParam {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void requiresMessageParam() {
            DefaultFullHttpRequest request = getRequest("/Unary?encoding=json");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST,
                    "{\"code\":\"invalid_argument\",\"message\":\"Missing message query parameter\"}");
            } finally {
                response.release();
            }
        }

        @Test
        void acceptsEmptyMessageValue() {
            String methodUri = "/Unary?encoding=proto&message=";

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(UnaryGetRequest.getDefaultInstance());
            assertResponseHandlerInstalled();
        }

        @Test
        void percentDecodesJsonMessage() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void base64DecodesWhenFlagIsOne() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String methodUri = "/Unary?encoding=proto&base64=1&message=" + base64Url(protoBytes);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsPaddedBase64() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String padded = Base64.getUrlEncoder().encodeToString(protoBytes);
            String methodUri = "/Unary?encoding=proto&base64=1&message="
                + urlEncode(padded);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsUnpaddedBase64() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String unpadded = Base64.getUrlEncoder().withoutPadding().encodeToString(protoBytes);
            String methodUri = "/Unary?encoding=proto&base64=1&message=" + unpadded;

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @ParameterizedTest
        @ValueSource(strings = {"0", "true", ""})
        void ignoresBase64FlagOtherThanOne(String base64Value) {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&base64=" + base64Value
                + "&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsInvalidBase64() {
            String methodUri = "/Unary?encoding=proto&base64=1&message=not***base64";

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST,
                    "{\"code\":\"invalid_argument\",\"message\":\"Invalid base64 message query parameter\"}");
            } finally {
                response.release();
            }
        }

        @Test
        void rejectsMalformedProtoPayload() {
            String methodUri = "/Unary?encoding=proto&base64=1&message="
                + base64Url("not proto".getBytes(StandardCharsets.UTF_8));

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
                assertThat(json).contains("\"message\":\"Deserialization failed:");
            } finally {
                response.release();
            }
        }

        @Test
        void rejectsMalformedJsonPayload() {
            String methodUri = "/Unary?encoding=json&message=" + urlEncode("{");

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
                assertThat(json).contains("\"message\":\"Deserialization failed:");
            } finally {
                response.release();
            }
        }

        @Test
        void base64EncodedJsonWorks() {
            String messageJson = "{\"text\":\"hello\"}";
            byte[] jsonBytes = messageJson.getBytes(StandardCharsets.UTF_8);
            String methodUri = "/Unary?encoding=json&base64=1&message=" + base64Url(jsonBytes);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void base64ProtoWithJsonBytesFailsDecoding() {
            byte[] protoBytes;
            try {
                protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            String methodUri = "/Unary?encoding=json&base64=1&message=" + base64Url(protoBytes);

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
            } finally {
                response.release();
            }
        }
    }

    @Nested
    class CompressionParam {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void treatsAbsentCompressionAsIdentity() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void treatsEmptyCompressionAsIdentity() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&compression=&message="
                + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void decompressesGzipProtoMessage() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            byte[] compressed = gzipBytes(protoBytes);
            String methodUri = "/Unary?encoding=proto&compression=gzip&base64=1&message="
                + base64Url(compressed);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsCompressedJson() throws IOException {
            String messageJson = "{\"text\":\"hello\"}";
            byte[] jsonBytes = messageJson.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = gzipBytes(jsonBytes);
            String methodUri = "/Unary?encoding=json&compression=gzip&base64=1&message="
                + base64Url(compressed);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsUnsupportedCompression() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=json&compression=br&message=%7B%7D");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.NOT_IMPLEMENTED);
                assertThat(json).contains("\"code\":\"unimplemented\"");
                assertThat(json).contains("\"message\":\"Unsupported compression: br; supported: identity,gzip\"");
            } finally {
                response.release();
            }
        }

        @Test
        void rejectsUnsupportedCompressionEvenForEmptyMessage() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=json&compression=br&message=");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.NOT_IMPLEMENTED);
                assertThat(json).contains("\"code\":\"unimplemented\"");
            } finally {
                response.release();
            }
        }

        @Test
        void skipsDecompressionForZeroLengthMessage() {
            String methodUri = "/Unary?encoding=proto&compression=gzip&message=";

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(UnaryGetRequest.getDefaultInstance());
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsDecompressionFailure() {
            String methodUri = "/Unary?encoding=proto&compression=gzip&base64=1&message="
                + base64Url("not gzip".getBytes(StandardCharsets.UTF_8));

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
                assertThat(json).contains("\"message\":\"Decompression failed:");
            } finally {
                response.release();
            }
        }

    }

    @Nested
    class RequestSizeLimits {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary", SMALL_MAX_REQUEST_BYTES);
        }

        @Test
        void rejectsMessageExceedingMaxRequestBytes() {
            String largeText = "x".repeat(SMALL_MAX_REQUEST_BYTES + 1);
            String messageJson = "{\"text\":\"" + largeText + "\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.TOO_MANY_REQUESTS);
                assertThat(json).contains("\"code\":\"resource_exhausted\"");
            } finally {
                response.release();
            }
        }
    }

    @Nested
    class ContentEncodingPrecedence {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void ignoresContentEncodingForGetDecompression() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "gzip");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsGzipQueryWithConflictingContentEncoding() throws IOException {
            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            byte[] compressed = gzipBytes(protoBytes);
            String methodUri = "/Unary?encoding=proto&compression=gzip&base64=1&message="
                + base64Url(compressed);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "identity");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void doesNotRejectUnsupportedContentEncodingHeader() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_ENCODING, "br");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }
    }

    @Nested
    class ProtocolVersion {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void acceptsOmittedConnectParam() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsConnectV1() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message="
                + urlEncode(messageJson) + "&connect=v1";

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsInvalidConnectValue() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=json&message=%7B%7D&connect=v2");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST,
                    "{\"code\":\"invalid_argument\",\"message\":\"Unsupported Connect protocol version: v2\"}");
            } finally {
                response.release();
            }
        }

        @Test
        void ignoresConnectProtocolVersionHeader() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set("connect-protocol-version", "2");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsConnectV1WithConflictingHeader() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message="
                + urlEncode(messageJson) + "&connect=v1";
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set("connect-protocol-version", "2");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsConnectV2WithConflictingHeader() {
            DefaultFullHttpRequest request = getRequest(
                "/Unary?encoding=json&message=%7B%7D&connect=v2");
            request.headers().set("connect-protocol-version", "1");

            channel.writeInbound(request);

            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                String json = assertConnectErrorResponse(response, HttpResponseStatus.BAD_REQUEST);
                assertThat(json).contains("\"code\":\"invalid_argument\"");
            } finally {
                response.release();
            }
        }
    }

    @Nested
    class BodyHandling {
        @BeforeEach
        void setUp() {
            setUpHandler("Unary");
        }

        @Test
        void acceptsEmptyBody() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);

            channel.writeInbound(getRequest(methodUri));

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void acceptsContentLengthZero() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = getRequest(methodUri);
            request.headers().set(HttpHeaderNames.CONTENT_LENGTH, "0");

            channel.writeInbound(request);

            assertCallExchangeFired();
            assertInboundDataDecodesTo(REQUEST);
            assertResponseHandlerInstalled();
        }

        @Test
        void rejectsNonEmptyBodyAsPlain415() {
            String methodUri = "/Unary?encoding=json&message=%7B%7D";
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, methodUri,
                Unpooled.wrappedBuffer("body".getBytes(StandardCharsets.UTF_8)));

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                HttpAssertions.assertThat(response).unsupportedMediaTypeError();
            } finally {
                response.release();
            }
            assertThat(channel.isOpen()).isTrue();
        }

        @Test
        void rejectsNonEmptyBodyEvenWithValidQueryPayload() {
            String messageJson = "{\"text\":\"hello\"}";
            String methodUri = "/Unary?encoding=json&message=" + urlEncode(messageJson);
            DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.GET, methodUri,
                Unpooled.wrappedBuffer("body".getBytes(StandardCharsets.UTF_8)));

            channel.writeInbound(request);

            assertThat(request.refCnt()).isZero();
            assertThat((Object) channel.readInbound()).isNull();
            FullHttpResponse response = channel.readOutbound();
            try {
                HttpAssertions.assertThat(response).unsupportedMediaTypeError();
            } finally {
                response.release();
            }
        }
    }

    @Nested
    class InterceptorCallbacks {
        @Test
        void observerCallbacksAreInterleavedCorrectlyWithConnectMessages() throws IOException {
            List<String> events = new ArrayList<>();
            ConnectCallObserver observer = new ConnectCallObserver() {
                @Override
                public void onRequestPayload(Object payload) {
                    events.add("onRequestPayload");
                }

                @Override
                public void onRequestFinished() {
                    events.add("onRequestFinished");
                }
            };

            setUpHandler("Unary", DEFAULT_MAX_REQUEST_BYTES,
                new ConnectInterceptorPipeline(List.of(ctx -> ConnectInterceptor.continueWith(observer))));
            channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) {
                    switch (msg) {
                        case ConnectCallExchange ignore -> events.add("ConnectCallExchange");
                        case ConnectPayload ignore -> events.add("ConnectPayload");
                        case ConnectEndOfStream ignore -> events.add("ConnectEndOfStream");
                        case null, default -> {
                        }
                    }
                }
            });

            byte[] protoBytes = encoded(ConnectProtobufCodec.INSTANCE, REQUEST);
            String methodUri = "/Unary?encoding=proto&base64=1&message=" + base64Url(protoBytes)
                + "&connect=v1";
            channel.writeInbound(getRequest(methodUri));

            assertThat(events).containsExactly(
                "ConnectCallExchange",
                "onRequestPayload",
                "ConnectPayload",
                "onRequestFinished",
                "ConnectEndOfStream");
        }
    }

    private void assertResponseHandlerInstalled() {
        assertThat(channel.pipeline().get(ConnectPipeline.UNARY_RESPONSE_HANDLER))
            .isInstanceOf(UnaryResponseProcessingHandler.class);
    }

    private static DefaultFullHttpRequest getRequest(String methodUri) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/" + SERVICE_NAME + methodUri, Unpooled.buffer(0));
        request.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        return request;
    }

    private static byte[] encoded(ConnectCodec codec, Object value) throws IOException {
        ByteBuf buf = codec.encode(value, ByteBufAllocator.DEFAULT);
        try {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        } finally {
            buf.release();
        }
    }

    private static String base64Url(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static byte[] gzipBytes(byte[] input) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(input);
        }
        return bos.toByteArray();
    }

    private void assertCallExchangeFired() {
        assertThat((ConnectCallExchange) channel.readInbound()).isSameAs(exchange);
    }

    private void assertInboundDataDecodesTo(UnaryGetRequest expected) {
        Object inbound = channel.readInbound();
        assertThat(inbound)
            .isInstanceOfSatisfying(ConnectPayload.class, data ->
                assertThat(data.data()).isEqualTo(expected));
    }

    private static void assertConnectErrorResponse(
        FullHttpResponse response, HttpResponseStatus status, String expectedJson)
    {
        String actualJson = assertConnectErrorResponse(response, status);
        assertThat(actualJson).isEqualTo(expectedJson);
    }

    private static String assertConnectErrorResponse(FullHttpResponse response, HttpResponseStatus status) {
        assertThat(response.status()).isEqualTo(status);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE))
            .isEqualTo("application/json");
        String actualJson = response.content().toString(StandardCharsets.UTF_8);
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH))
            .isEqualTo(String.valueOf(actualJson.getBytes(StandardCharsets.UTF_8).length));
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        return actualJson;
    }

    private static void assertMethodNotAllowedWithAllowPost(FullHttpResponse response) {
        assertThat(response.status()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED);
        assertThat(response.headers().get("Allow")).isEqualTo("POST");
        assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE))
            .isEqualTo("text/plain; charset=utf-8");
        assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
    }
}
