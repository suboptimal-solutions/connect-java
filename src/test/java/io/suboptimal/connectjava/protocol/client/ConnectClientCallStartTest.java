package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectClientCallStartTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "svc.Service", List.of(METHOD), null);

    @Test
    void normalizesHeaderNamesToLowerCase() {
        ConnectClientCallStart callStart = new ConnectClientCallStart(
            SERVICE, METHOD, Map.of("Content-Type", List.of("application/proto")), false, "proto");

        assertThat(callStart.requestHeaders()).containsKey("content-type");
        assertThat(callStart.requestHeaders()).doesNotContainKey("Content-Type");
    }

    @Test
    void copiesHeadersDefensively() {
        Map<String, List<String>> source = new java.util.HashMap<>();
        source.put("x-test", new ArrayList<>(List.of("a")));

        ConnectClientCallStart callStart = new ConnectClientCallStart(
            SERVICE, METHOD, source, false, null);

        source.put("x-added", List.of("b"));
        assertThat(callStart.requestHeaders()).doesNotContainKey("x-added");
        assertThatThrownBy(() -> callStart.requestHeaders().put("x-mutate", List.of("c")))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void allowsNullCodecName() {
        ConnectClientCallStart callStart = new ConnectClientCallStart(
            SERVICE, METHOD, Map.of(), true, null);
        assertThat(callStart.codecName()).isNull();
    }

    @Test
    void withHeaderAppendsValueAndLowercasesName() {
        ConnectClientCallStart base = new ConnectClientCallStart(
            SERVICE, METHOD, Map.of("x-existing", List.of("a")), false, "proto");

        ConnectClientCallStart out = base.withHeader("X-Existing", "b").withHeader("X-New", "c");

        assertThat(out.requestHeaders().get("x-existing")).containsExactly("a", "b");
        assertThat(out.requestHeaders().get("x-new")).containsExactly("c");
        // original is unchanged
        assertThat(base.requestHeaders().get("x-existing")).containsExactly("a");
        assertThat(base.requestHeaders()).doesNotContainKey("x-new");
    }

    @Test
    void withTimeoutMsAndCodecReplaceSingleField() {
        ConnectClientCallStart base = new ConnectClientCallStart(
            SERVICE, METHOD, Map.of(), false, "proto", 1000L);

        assertThat(base.withTimeoutMs(5000L).timeoutMs()).isEqualTo(5000L);
        assertThat(base.withTimeoutMs(5000L).codecName()).isEqualTo("proto");
        assertThat(base.withCodecName("json").codecName()).isEqualTo("json");
        assertThat(base.withCodecName("json").timeoutMs()).isEqualTo(1000L);
        // original unchanged
        assertThat(base.timeoutMs()).isEqualTo(1000L);
        assertThat(base.codecName()).isEqualTo("proto");
    }

    @Test
    void rejectsNullRequiredFields() {
        assertThatThrownBy(() -> new ConnectClientCallStart(null, METHOD, Map.of(), false, "proto"))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConnectClientCallStart(SERVICE, null, Map.of(), false, "proto"))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConnectClientCallStart(SERVICE, METHOD, null, false, "proto"))
            .isInstanceOf(NullPointerException.class);
    }
}
