package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectStringBuilderJsonSerializerTest {
    private static final ConnectStringBuilderJsonSerializer S = ConnectStringBuilderJsonSerializer.INSTANCE;

    @Nested
    class Error {
        @Test
        void wrapsErrorObjectInBraces() {
            byte[] body = S.error(ConnectError.internal("boom"));
            assertThat(utf8(body)).isEqualTo("{\"code\":\"internal\",\"message\":\"boom\"}");
        }

        @Test
        void noMessageOmitsMessageField() {
            byte[] body = S.error(ConnectError.unimplemented(""));
            assertThat(utf8(body)).isEqualTo("{\"code\":\"unimplemented\"}");
        }

        @Test
        void specialCharsInMessageEscaped() {
            byte[] body = S.error(ConnectError.internal("line1\nline2\"end"));
            assertThat(utf8(body)).isEqualTo("{\"code\":\"internal\",\"message\":\"line1\\nline2\\\"end\"}");
        }

        @Test
        void controlCharsEscapedAsUnicode() {
            String msg = String.valueOf((char) 0x00) + (char) 0x01 + (char) 0x1f;
            byte[] body = S.error(ConnectError.internal(msg));
            assertThat(utf8(body)).isEqualTo("{\"code\":\"internal\",\"message\":\"\\u0000\\u0001\\u001f\"}");
        }

        @Test
        void errorWithOneDetailIncludesDetailsArray() {
            ConnectErrorDetail detail = new ConnectErrorDetail(
                "google.rpc.RetryInfo", new byte[]{0x0a, 0x02, 0x08, 0x1e});
            ConnectError error = new ConnectError(ConnectErrorCode.UNAVAILABLE, "try again", List.of(detail));
            assertThat(utf8(S.error(error)))
                .isEqualTo("{\"code\":\"unavailable\",\"message\":\"try again\","
                    + "\"details\":[{\"type\":\"google.rpc.RetryInfo\",\"value\":\"CgIIHg\"}]}");
        }

        @Test
        void errorWithMultipleDetailsPreservesOrder() {
            ConnectErrorDetail d1 = new ConnectErrorDetail("foo.Bar", new byte[]{0x01});
            ConnectErrorDetail d2 = new ConnectErrorDetail("foo.Baz", new byte[]{0x02});
            ConnectError error = new ConnectError(ConnectErrorCode.INTERNAL, "oops", List.of(d1, d2));
            String json = utf8(S.error(error));
            assertThat(json).contains("\"type\":\"foo.Bar\"");
            assertThat(json).contains("\"type\":\"foo.Baz\"");
            assertThat(json.indexOf("foo.Bar")).isLessThan(json.indexOf("foo.Baz"));
        }

        @Test
        void errorWithEmptyDetailsOmitsDetailsField() {
            ConnectError error = new ConnectError(ConnectErrorCode.INTERNAL, "boom", List.of());
            assertThat(utf8(S.error(error))).isEqualTo("{\"code\":\"internal\",\"message\":\"boom\"}");
        }

        @Test
        void emptyMessageWithDetailsOmitsMessageFieldButIncludesDetails() {
            ConnectErrorDetail detail = new ConnectErrorDetail("google.rpc.DebugInfo", new byte[]{0x01});
            ConnectError error = new ConnectError(ConnectErrorCode.INTERNAL, "", List.of(detail));
            assertThat(utf8(S.error(error)))
                .isEqualTo("{\"code\":\"internal\","
                    + "\"details\":[{\"type\":\"google.rpc.DebugInfo\",\"value\":\"AQ\"}]}");
        }

        @Test
        void detailValueEncodedWithoutPadding() {
            // 1-byte value encodes to 2 base64 chars — padded would be "AQ==", unpadded "AQ"
            ConnectErrorDetail detail = new ConnectErrorDetail("t", new byte[]{0x01});
            ConnectError error = new ConnectError(ConnectErrorCode.INTERNAL, "", List.of(detail));
            String json = utf8(S.error(error));
            assertThat(json).contains("\"value\":\"AQ\"");
            assertThat(json).doesNotContain("=");
        }
    }

    @Nested
    class EndStream {
        @Test
        void noErrorNoMetadata() {
            byte[] body = S.endStream(new ConnectEndStreamResponse(null, ConnectEndStreamMeta.EMPTY));
            assertThat(utf8(body)).isEqualTo("{}");
        }

        @Test
        void errorNoMetadata() {
            byte[] body = S.endStream(new ConnectEndStreamResponse(
                ConnectError.unknown("oops"), ConnectEndStreamMeta.EMPTY));
            assertThat(utf8(body)).isEqualTo("{\"error\":{\"code\":\"unknown\",\"message\":\"oops\"}}");
        }

        @Test
        void noErrorWithMetadata() {
            ConnectEndStreamMeta metadata = meta().add("grpc-status", "0").build();
            byte[] body = S.endStream(new ConnectEndStreamResponse(null, metadata));
            assertThat(utf8(body)).isEqualTo("{\"metadata\":{\"grpc-status\":[\"0\"]}}");
        }

        @Test
        void errorWithMetadata() {
            ConnectEndStreamMeta metadata = meta().add("x-req-id", "abc").build();
            byte[] body = S.endStream(new ConnectEndStreamResponse(ConnectError.internal("fail"), metadata));
            assertThat(utf8(body))
                .isEqualTo("{\"error\":{\"code\":\"internal\",\"message\":\"fail\"},\"metadata\":{\"x-req-id\":[\"abc\"]}}");
        }

        @Test
        void multiValueHeader() {
            ConnectEndStreamMeta metadata = meta()
                .add("foo", "a")
                .add("foo", "b")
                .build();
            byte[] body = S.endStream(new ConnectEndStreamResponse(null, metadata));
            assertThat(utf8(body)).isEqualTo("{\"metadata\":{\"foo\":[\"a\",\"b\"]}}");
        }

        @Test
        void multipleHeaders() {
            ConnectEndStreamMeta metadata = meta()
                .add("alpha", "1")
                .add("beta", "2")
                .build();
            String result = utf8(S.endStream(new ConnectEndStreamResponse(null, metadata)));
            assertThat(result).startsWith("{\"metadata\":{");
            assertThat(result).contains("\"alpha\":[\"1\"]");
            assertThat(result).contains("\"beta\":[\"2\"]");
            assertThat(result).endsWith("}}");
        }

        @Test
        void specialCharsInMetadataValueEscaped() {
            ConnectEndStreamMeta metadata = meta().add("x-val", "he\"llo\nworld").build();
            byte[] body = S.endStream(new ConnectEndStreamResponse(null, metadata));
            assertThat(utf8(body)).isEqualTo("{\"metadata\":{\"x-val\":[\"he\\\"llo\\nworld\"]}}");
        }

        @Test
        void specialCharsInMetadataNameEscaped() {
            ConnectEndStreamMeta metadata = meta().add("x-\ttab", "v").build();
            byte[] body = S.endStream(new ConnectEndStreamResponse(null, metadata));
            assertThat(utf8(body)).isEqualTo("{\"metadata\":{\"x-\\ttab\":[\"v\"]}}");
        }

        @Test
        void endStreamErrorWithDetailsIncludesDetailsInErrorObject() {
            ConnectErrorDetail detail = new ConnectErrorDetail("google.rpc.RetryInfo", new byte[]{0x0a, 0x02, 0x08, 0x1e});
            ConnectError error = new ConnectError(ConnectErrorCode.UNAVAILABLE, "retry", List.of(detail));
            byte[] body = S.endStream(new ConnectEndStreamResponse(error, ConnectEndStreamMeta.EMPTY));
            assertThat(utf8(body))
                .isEqualTo("{\"error\":{\"code\":\"unavailable\",\"message\":\"retry\","
                    + "\"details\":[{\"type\":\"google.rpc.RetryInfo\",\"value\":\"CgIIHg\"}]}}");
        }
    }

    private static ConnectEndStreamMeta.Builder meta() {
        return ConnectEndStreamMeta.builder();
    }

    private static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
