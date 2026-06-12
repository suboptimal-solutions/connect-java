package io.suboptimal.connectjava.protocol.server;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.AsciiString;
import io.suboptimal.connectjava.api.ConnectError;

import java.nio.charset.StandardCharsets;

final class HttpResponses {
    public static final AsciiString TYPE_UTF8 = AsciiString.cached("text/plain; charset=utf-8");
    public static final AsciiString TYPE_JSON = AsciiString.cached("application/json");

    public static final byte[] UNSUPPORTED_MEDIA_TYPE_BODY = statusToBody(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE);
    public static final byte[] METHOD_NOT_ALLOWED_BODY = statusToBody(HttpResponseStatus.METHOD_NOT_ALLOWED);
    public static final byte[] NOT_FOUND_BODY = statusToBody(HttpResponseStatus.NOT_FOUND);

    private HttpResponses() {}

    static FullHttpResponse unsupportedMediaType() {
        return fullHttpResponse(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, TYPE_UTF8, UNSUPPORTED_MEDIA_TYPE_BODY);
    }

    static FullHttpResponse methodNotAllowedPostOnly() {
        FullHttpResponse response = fullHttpResponse(
            HttpResponseStatus.METHOD_NOT_ALLOWED, TYPE_UTF8, METHOD_NOT_ALLOWED_BODY);
        response.headers().set(HttpHeaderNames.ALLOW, HttpMethod.POST.asciiName());
        return response;
    }

    static FullHttpResponse methodNotAllowedGetPost() {
        FullHttpResponse response = fullHttpResponse(
            HttpResponseStatus.METHOD_NOT_ALLOWED, TYPE_UTF8, METHOD_NOT_ALLOWED_BODY);
        response.headers().set(HttpHeaderNames.ALLOW, "GET, POST");
        return response;
    }

    static FullHttpResponse notFound() {
        return fullHttpResponse(HttpResponseStatus.NOT_FOUND, TYPE_UTF8, NOT_FOUND_BODY);
    }

    static FullHttpResponse httpVersionNotSupported() {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED, Unpooled.EMPTY_BUFFER);
        response.headers()
            .set(HttpHeaderNames.CONTENT_LENGTH, 0)
            .set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        return response;
    }

    static FullHttpResponse protocolError(ConnectError error, ConnectJsonSerializer jsonSerializer) {
        return fullHttpResponse(error.code().httpStatus(), TYPE_JSON, jsonSerializer.error(error));
    }

    private static FullHttpResponse fullHttpResponse(
        HttpResponseStatus status, AsciiString contentType, byte[] body)
    {
        FullHttpResponse response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers()
            .set(HttpHeaderNames.CONTENT_TYPE, contentType)
            .set(HttpHeaderNames.CONTENT_LENGTH, body.length);
        return response;
    }

    private static byte[] statusToBody(HttpResponseStatus status) {
        return status.reasonPhrase().getBytes(StandardCharsets.UTF_8);
    }
}
