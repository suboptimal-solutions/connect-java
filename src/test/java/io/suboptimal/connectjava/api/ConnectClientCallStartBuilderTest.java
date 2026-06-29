package io.suboptimal.connectjava.api;

import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientCallStartBuilderTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "svc.Service", List.of(METHOD), null);

    private ConnectClientCallStartBuilder newBuilder() {
        return new ConnectClientCallStartBuilder(new ConnectClientCallStart(
            SERVICE, METHOD, Map.of("x-start", List.of("v0")), false, "proto", 1000L));
    }

    @Test
    void copiesInitialState() {
        ConnectClientCallStartBuilder builder = newBuilder();

        assertThat(builder.serviceDefinition()).isSameAs(SERVICE);
        assertThat(builder.methodDefinition()).isSameAs(METHOD);
        assertThat(builder.headerValues("x-start")).containsExactly("v0");
        assertThat(builder.preferGet()).isFalse();
        assertThat(builder.codecName()).isEqualTo("proto");
        assertThat(builder.timeoutMs()).isEqualTo(1000L);
    }

    @Test
    void addHeaderAppendsAndLowercases() {
        ConnectClientCallStartBuilder builder = newBuilder();
        builder.addHeader("X-Start", "v1");

        assertThat(builder.headerValues("x-start")).containsExactly("v0", "v1");
    }

    @Test
    void setHeaderReplaces() {
        ConnectClientCallStartBuilder builder = newBuilder();
        builder.setHeader("X-Start", "replaced");

        assertThat(builder.headerValues("x-start")).containsExactly("replaced");
    }

    @Test
    void removeHeaderDeletesAllValues() {
        ConnectClientCallStartBuilder builder = newBuilder();
        builder.removeHeader("X-Start");

        assertThat(builder.headerValues("x-start")).isEmpty();
    }

    @Test
    void missingHeaderReturnsEmptyList() {
        assertThat(newBuilder().headerValues("no-such-header")).isEmpty();
    }

    @Test
    void codecNameAndTimeoutMutators() {
        ConnectClientCallStartBuilder builder = newBuilder();
        builder.codecName("json");
        builder.timeoutMs(5000L);

        assertThat(builder.codecName()).isEqualTo("json");
        assertThat(builder.timeoutMs()).isEqualTo(5000L);
    }

    @Test
    void preferGetMutator() {
        ConnectClientCallStartBuilder builder = newBuilder();
        assertThat(builder.preferGet()).isFalse();

        builder.preferGet(true);
        assertThat(builder.preferGet()).isTrue();
    }

    @Test
    void buildFreezesState() {
        ConnectClientCallStartBuilder builder = newBuilder();
        builder.addHeader("x-frozen", "yes");
        builder.codecName("json");
        builder.timeoutMs(999L);

        ConnectClientCallStart frozen = builder.build();

        assertThat(frozen.requestHeaders()).containsKey("x-frozen");
        assertThat(frozen.codecName()).isEqualTo("json");
        assertThat(frozen.timeoutMs()).isEqualTo(999L);
        assertThat(frozen.serviceDefinition()).isSameAs(SERVICE);
        assertThat(frozen.methodDefinition()).isSameAs(METHOD);
    }

    @Test
    void subsequentBuildCallsAreIndependent() {
        ConnectClientCallStartBuilder builder = newBuilder();

        ConnectClientCallStart first = builder.build();
        builder.addHeader("x-after", "1");
        ConnectClientCallStart second = builder.build();

        assertThat(first.requestHeaders()).doesNotContainKey("x-after");
        assertThat(second.requestHeaders()).containsKey("x-after");
    }

    @Test
    void mutatingBuilderDoesNotAffectBuiltInstance() {
        ConnectClientCallStartBuilder builder = newBuilder();
        ConnectClientCallStart frozen = builder.build();

        builder.setHeader("x-start", "mutated");
        assertThat(frozen.requestHeaders().get("x-start")).containsExactly("v0");
    }

    @Test
    void nullNullableFieldsAllowed() {
        ConnectClientCallStartBuilder builder = new ConnectClientCallStartBuilder(
            new ConnectClientCallStart(SERVICE, METHOD, Map.of(), false, null));
        assertThat(builder.codecName()).isNull();
        assertThat(builder.timeoutMs()).isNull();

        builder.codecName(null);
        builder.timeoutMs(null);
        ConnectClientCallStart frozen = builder.build();
        assertThat(frozen.codecName()).isNull();
        assertThat(frozen.timeoutMs()).isNull();
    }
}
