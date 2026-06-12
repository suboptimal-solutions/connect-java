package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
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

    // ---- parseError ----

    @Test
    void parseErrorReadsCodeMessageAndDetails() {
        String b64 = Base64.getEncoder().encodeToString(new byte[]{1, 2, 3});
        String json = "{\"code\":\"not_found\",\"message\":\"nope\",\"details\":[{\"type\":\"google.rpc.RetryInfo\",\"value\":\"" + b64 + "\"}]}";

        ConnectError e = D.parseError(utf8(json));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(e.message()).isEqualTo("nope");
        assertThat(e.details()).hasSize(1);
        assertThat(e.details().get(0).type()).isEqualTo("google.rpc.RetryInfo");
        assertThat(e.details().get(0).value()).isEqualTo(new byte[]{1, 2, 3});
    }

    @Test
    void parseErrorWithoutCodeReturnsNull() {
        ConnectError e = D.parseError(utf8("{\"message\":\"oops\"}"));
        assertThat(e).isNull();
    }

    @Test
    void parseErrorWithUnknownCodeNameFallsBackToUnknown() {
        ConnectError e = D.parseError(utf8("{\"code\":\"totally_made_up\",\"message\":\"x\"}"));
        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.UNKNOWN);
    }

    @Test
    void parseErrorUnescapesMessage() {
        String json = "{\"code\":\"internal\",\"message\":\"a \\\"quote\\\" and\\nnewline\"}";

        ConnectError e = D.parseError(utf8(json));

        assertThat(e).isNotNull();
        assertThat(e.message()).isEqualTo("a \"quote\" and\nnewline");
    }

    @Test
    void parseErrorMissingMessageGivesEmptyString() {
        ConnectError e = D.parseError(utf8("{\"code\":\"internal\"}"));
        assertThat(e).isNotNull();
        assertThat(e.message()).isEqualTo("");
    }

    // ---- parseErrorBody ----

    @Test
    void parseErrorBodyReturnsRawFields() {
        String json = "{\"code\":\"permission_denied\",\"message\":\"denied\",\"details\":[]}";

        ConnectErrorBody b = D.parseErrorBody(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.codeName()).isEqualTo("permission_denied");
        assertThat(b.message()).isEqualTo("denied");
        assertThat(b.details()).isEmpty();
    }

    @Test
    void parseErrorBodyReturnsNullForNonErrorJson() {
        ConnectErrorBody b = D.parseErrorBody(utf8("{\"foo\":\"bar\"}"));
        assertThat(b).isNull();
    }

    @Test
    void parseErrorBodySkipsMalformedBase64Detail() {
        String json = "{\"code\":\"internal\",\"details\":[{\"type\":\"t\",\"value\":\"!!!not-base64!!!\"}]}";

        ConnectErrorBody b = D.parseErrorBody(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.details()).isEmpty();
    }

    @Test
    void parseErrorBodyDecodesMultipleDetailsInOrder() {
        String v1 = Base64.getEncoder().encodeToString(new byte[]{1});
        String v2 = Base64.getEncoder().encodeToString(new byte[]{2});
        String json = "{\"code\":\"internal\",\"details\":["
            + "{\"type\":\"a\",\"value\":\"" + v1 + "\"},"
            + "{\"type\":\"b\",\"value\":\"" + v2 + "\"}"
            + "]}";

        ConnectErrorBody b = D.parseErrorBody(utf8(json));

        assertThat(b).isNotNull();
        assertThat(b.details()).hasSize(2);
        assertThat(b.details().get(0).value()).isEqualTo(new byte[]{1});
        assertThat(b.details().get(1).value()).isEqualTo(new byte[]{2});
    }

    // ---- parseEndStreamError ----

    @Test
    void parseEndStreamErrorReadsNestedError() {
        ConnectError e = D.parseEndStreamError(utf8("{\"error\":{\"code\":\"not_found\",\"message\":\"gone\"}}"));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(e.message()).isEqualTo("gone");
    }

    @Test
    void parseEndStreamErrorReturnsNullWhenNoError() {
        assertThat(D.parseEndStreamError(utf8("{}"))).isNull();
        assertThat(D.parseEndStreamError(utf8("{\"metadata\":{\"a\":[\"b\"]}}"))).isNull();
    }

    @Test
    void parseEndStreamErrorWithoutCodeIsUnknown() {
        ConnectError e = D.parseEndStreamError(utf8("{\"error\":{\"message\":\"oops\"}}"));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(e.message()).isEqualTo("oops");
    }

    @Test
    void parseEndStreamErrorHandlesNestedBracesInDetails() {
        String b64 = Base64.getEncoder().encodeToString(new byte[]{9});
        String json = "{\"error\":{\"code\":\"internal\",\"message\":\"m\",\"details\":[{\"type\":\"t\",\"value\":\"" + b64 + "\"}]}}";

        ConnectError e = D.parseEndStreamError(utf8(json));

        assertThat(e).isNotNull();
        assertThat(e.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(e.details()).hasSize(1);
        assertThat(e.details().get(0).value()).isEqualTo(new byte[]{9});
    }

    // ---- parseEndStreamMetadata ----

    @Test
    void parseEndStreamMetadataExtractsMultiValue() {
        String json = "{\"metadata\":{\"foo\":[\"a\",\"b\"],\"bar\":[\"c\"]}}";

        Map<String, List<String>> m = D.parseEndStreamMetadata(utf8(json));

        assertThat(m.get("foo")).containsExactly("a", "b");
        assertThat(m.get("bar")).containsExactly("c");
    }

    @Test
    void parseEndStreamMetadataAbsentReturnsEmpty() {
        assertThat(D.parseEndStreamMetadata(utf8("{}"))).isEmpty();
        assertThat(D.parseEndStreamMetadata(utf8("{\"error\":{\"code\":\"internal\"}}"))).isEmpty();
    }

    @Test
    void parseEndStreamMetadataUnescapesValues() {
        String json = "{\"metadata\":{\"k\":[\"line1\\nline2\"]}}";

        Map<String, List<String>> m = D.parseEndStreamMetadata(utf8(json));

        assertThat(m.get("k").get(0)).isEqualTo("line1\nline2");
    }
}
