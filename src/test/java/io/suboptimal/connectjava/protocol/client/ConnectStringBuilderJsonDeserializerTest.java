package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectStringBuilderJsonDeserializerTest {
    private static final ConnectStringBuilderJsonDeserializer D =
        ConnectStringBuilderJsonDeserializer.INSTANCE;

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    // ---- parseUnaryError ----

    @Test
    void parseUnaryErrorReturnsRawFields() {
        String json = """
                {"code":"permission_denied","message":"denied","details":[]}""";

        ConnectErrorBody b = D.parseUnaryError(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.codeName()).isEqualTo("permission_denied");
        assertThat(b.message()).isEqualTo("denied");
        assertThat(b.details()).isEmpty();
    }

    @Test
    void parseUnaryErrorReturnsNullForNonErrorJson() {
        String json = """
                {"foo":"bar"}""";
        ConnectErrorBody b = D.parseUnaryError(utf8(json));
        assertThat(b).isNull();
    }

    @Test
    void parseUnaryErrorSkipsMalformedBase64Detail() {
        String json = """
                {"code":"internal","details":[{"type":"t","value":"!!!not-base64!!!"}]}""";

        ConnectErrorBody b = D.parseUnaryError(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.details()).isEmpty();
    }

    @Test
    void parseUnaryErrorDecodesMultipleDetailsInOrder() {
        String v1 = Base64.getEncoder().encodeToString(new byte[]{1});
        String v2 = Base64.getEncoder().encodeToString(new byte[]{2});
        String json = "{\"code\":\"internal\",\"details\":["
            + "{\"type\":\"a\",\"value\":\"" + v1 + "\"},"
            + "{\"type\":\"b\",\"value\":\"" + v2 + "\"}"
            + "]}";

        ConnectErrorBody b = D.parseUnaryError(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.details()).hasSize(2);
        assertThat(b.details().get(0).value()).isEqualTo(new byte[]{1});
        assertThat(b.details().get(1).value()).isEqualTo(new byte[]{2});
    }

    // ---- parseStreamError ----

    @Test
    void parseStreamErrorReadsNestedError() {
        String json = """
                {"error":{"code":"not_found","message":"gone"}}""";
        ConnectError e = D.parseStreamError(utf8(json));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(e.message()).isEqualTo("gone");
    }

    @Test
    void parseStreamErrorReturnsNullWhenNoError() {
        assertThat(D.parseStreamError(utf8("{}"))).isNull();
        assertThat(D.parseStreamError(utf8("""
                {"metadata":{"a":["b"]}}"""))).isNull();
    }

    @Test
    void parseStreamErrorWithoutCodeIsUnknown() {
        ConnectError e = D.parseStreamError(utf8("""
                {"error":{"message":"oops"}}"""));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(e.message()).isEqualTo("oops");
    }

    @Test
    void parseStreamErrorHandlesNestedBracesInDetails() {
        String b64 = Base64.getEncoder().encodeToString(new byte[]{9});
        String json = "{\"error\":{\"code\":\"internal\",\"message\":\"m\",\"details\":[{\"type\":\"t\",\"value\":\"" + b64 + "\"}]}}";

        ConnectError e = D.parseStreamError(utf8(json));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(e.details()).hasSize(1);
        assertThat(e.details().get(0).value()).isEqualTo(new byte[]{9});
    }

    // ---- parseStreamMetadata ----

    @Test
    void parseStreamMetadataExtractsMultiValue() {
        String json = """
                {"metadata":{"foo":["a","b"],"bar":["c"]}}""";

        Map<String, List<String>> m = D.parseStreamMetadata(utf8(json));

        assertThat(m.get("foo")).containsExactly("a", "b");
        assertThat(m.get("bar")).containsExactly("c");
    }

    @Test
    void parseStreamMetadataAbsentReturnsEmpty() {
        assertThat(D.parseStreamMetadata(utf8("{}"))).isEmpty();
        assertThat(D.parseStreamMetadata(utf8("""
                {"error":{"code":"internal"}}"""))).isEmpty();
    }

    @Test
    void parseStreamMetadataUnescapesValues() {
        String json = """
                {"metadata":{"k":["line1
                line2"]}}""";

        Map<String, List<String>> m = D.parseStreamMetadata(utf8(json));

        assertThat(m.get("k").get(0)).isEqualTo("line1\nline2");
    }
}
