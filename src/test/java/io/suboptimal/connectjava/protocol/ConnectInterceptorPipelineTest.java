package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectInterceptorPipelineTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Method", ConnectMethodType.UNARY, String.class, String.class, false);

    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "test.Service", List.of(METHOD), null);
    
    private static final ConnectCallExchange EXCHANGE =
        new ConnectCallExchange(SERVICE, METHOD, new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());

    @Test
    void emptyPipelineContinues() {
        ConnectInterceptor.Decision decision = ConnectInterceptorPipeline.EMPTY.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectInterceptor.Decision.Continue.class);
        assertThat(decision.observer()).isSameAs(ConnectCallObserver.NOOP);
    }

    @Test
    void allNoOpObserversProduceNoOp() {
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueCall(),
            exchange -> ConnectInterceptor.continueCall()
        ));

        ConnectInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectInterceptor.Decision.Continue.class);
        assertThat(decision.observer()).isSameAs(ConnectCallObserver.NOOP);
    }

    @Test
    void singleNonNoOpObserverIsReturnedDirectly() {
        ConnectCallObserver observer = new ConnectCallObserver() {};
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueWith(observer)
        ));

        ConnectInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision.observer()).isSameAs(observer);
    }

    @Test
    void noOpObserversAreFilteredFromComposite() {
        ConnectCallObserver real = new ConnectCallObserver() {};
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueCall(),
            exchange -> ConnectInterceptor.continueWith(real),
            exchange -> ConnectInterceptor.continueCall()
        ));

        ConnectInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision.observer()).isSameAs(real);
    }

    @Test
    void responsePayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("first"); }
            }),
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("second"); }
            })
        ));

        ConnectCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponsePayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responseHeaderCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseHeadersBuilder h) { log.add("first"); }
            }),
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseHeadersBuilder h) { log.add("second"); }
            })
        ));

        ConnectCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponseHeaders(new ResponseHeadersBuilder());

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void responseTrailerCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponseTrailers(ConnectResponseTrailersBuilder t, @Nullable ConnectError e) { log.add("first"); }
            }),
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onResponseTrailers(ConnectResponseTrailersBuilder t, @Nullable ConnectError e) { log.add("second"); }
            })
        ));

        ConnectCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponseTrailers(new ResponseTrailersBuilder(), null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void callCompleteCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { log.add("first"); }
            }),
            exchange -> ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { log.add("second"); }
            })
        ));

        ConnectCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onCallComplete(null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void rejectionStopsIterationAndReturnsCompositeOfPriorObservers() {
        List<String> callOrder = new ArrayList<>();
        List<String> completedLog = new ArrayList<>();

        ConnectInterceptor first = exchange -> {
            callOrder.add("first");
            return ConnectInterceptor.continueWith(new ConnectCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { completedLog.add("first"); }
            });
        };
        ConnectError rejectError = ConnectError.permissionDenied("no");
        ConnectInterceptor rejecting = exchange -> {
            callOrder.add("rejecting");
            return ConnectInterceptor.reject(rejectError);
        };
        ConnectInterceptor notReached = exchange -> {
            callOrder.add("notReached");
            return ConnectInterceptor.continueCall();
        };

        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(first, rejecting, notReached));

        ConnectInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectInterceptor.Decision.Reject.class);
        assertThat(((ConnectInterceptor.Decision.Reject) decision).error()).isSameAs(rejectError);
        assertThat(callOrder).containsExactly("first", "rejecting");

        // The composite observer from prior Continue decisions can still be invoked for terminal callbacks
        decision.observer().onCallComplete(rejectError);
        assertThat(completedLog).containsExactly("first");
    }

    @Test
    void rejectionWithNoPriorContinueObserversReturnsNoOp() {
        ConnectInterceptorPipeline pipeline = new ConnectInterceptorPipeline(List.of(
            exchange -> ConnectInterceptor.reject(ConnectError.unauthenticated("go away"))
        ));

        ConnectInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectInterceptor.Decision.Reject.class);
        assertThat(decision.observer()).isSameAs(ConnectCallObserver.NOOP);
    }
}
