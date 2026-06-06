package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.assertj.core.api.Assertions;

class HttpAssertions {
    private final FullHttpResponse response;

    private HttpAssertions(FullHttpResponse response) {
        this.response = response;
    }

    public static HttpAssertions assertThat(FullHttpResponse response) {
        return new HttpAssertions(response);
    }

    void unsupportedMediaTypeError() {
        assertPlainHttpErrorResponse(response, HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE,
            HttpResponses.UNSUPPORTED_MEDIA_TYPE_BODY);
    }

    void methodNotAllowedError() {
        assertPlainHttpErrorResponse(response, HttpResponseStatus.METHOD_NOT_ALLOWED,
            HttpResponses.METHOD_NOT_ALLOWED_BODY);
    }

    void methodNotAllowedAllowing(String allowHeader) {
        methodNotAllowedError();
        Assertions.assertThat(response.headers().get(HttpHeaderNames.ALLOW)).isEqualTo(allowHeader);
    }

    void notFoundError() {
        assertPlainHttpErrorResponse(response, HttpResponseStatus.NOT_FOUND,
            HttpResponses.NOT_FOUND_BODY);
    }

    void httpVersionNotSupportedWithConnectionClose() {
        Assertions.assertThat(response.status()).isEqualTo(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED);
        Assertions.assertThat(response.headers().get(HttpHeaderNames.CONNECTION)).isEqualTo("close");
        Assertions.assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH)).isEqualTo("0");
        Assertions.assertThat(response.content().readableBytes()).isZero();
    }

    private static void assertPlainHttpErrorResponse(FullHttpResponse response, HttpResponseStatus status, byte[] body) {
        Assertions.assertThat(response.status()).isEqualTo(status);
        Assertions.assertThat(response.headers().get(HttpHeaderNames.CONTENT_TYPE))
            .isEqualTo("text/plain; charset=utf-8");
        Assertions.assertThat(response.headers().get(HttpHeaderNames.CONTENT_LENGTH))
            .isEqualTo(String.valueOf(body.length));
        Assertions.assertThat(response.headers().contains(HttpHeaderNames.CONNECTION)).isFalse();
        Assertions.assertThat(response.content().array()).isEqualTo(body);
    }
}
