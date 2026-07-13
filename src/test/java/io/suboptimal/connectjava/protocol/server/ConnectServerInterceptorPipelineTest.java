package io.suboptimal.connectjava.protocol.server;

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

class ConnectServerInterceptorPipelineTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Method", ConnectMethodType.UNARY, String.class, String.class, false);

    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "test.Service", List.of(METHOD), null);
    
    private static final ConnectCallExchange EXCHANGE =
        new ConnectCallExchange(SERVICE, METHOD, new ConnectRequestMeta(Map.of()),
            new ResponseHeadersBuilder(), new ResponseTrailersBuilder());

    @Test
    void emptyPipelineContinues() {
        ConnectServerInterceptor.Decision decision = ConnectServerInterceptorPipeline.EMPTY.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectServerInterceptor.Decision.Continue.class);
        assertThat(decision.observer()).isSameAs(ConnectServerCallObserver.NOOP);
    }

    @Test
    void allNoOpObserversProduceNoOp() {
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueCall(),
            exchange -> ConnectServerInterceptor.continueCall()
        ));

        ConnectServerInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectServerInterceptor.Decision.Continue.class);
        assertThat(decision.observer()).isSameAs(ConnectServerCallObserver.NOOP);
    }

    @Test
    void singleNonNoOpObserverIsReturnedDirectly() {
        ConnectServerCallObserver observer = new ConnectServerCallObserver() {};
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueWith(observer)
        ));

        ConnectServerInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision.observer()).isSameAs(observer);
    }

    @Test
    void noOpObserversAreFilteredFromComposite() {
        ConnectServerCallObserver real = new ConnectServerCallObserver() {};
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueCall(),
            exchange -> ConnectServerInterceptor.continueWith(real),
            exchange -> ConnectServerInterceptor.continueCall()
        ));

        ConnectServerInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision.observer()).isSameAs(real);
    }

    @Test
    void responsePayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("first"); }
            }),
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("second"); }
            })
        ));

        ConnectServerCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponsePayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responseHeaderCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseHeadersBuilder h) { log.add("first"); }
            }),
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseHeadersBuilder h) { log.add("second"); }
            })
        ));

        ConnectServerCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponseHeaders(new ResponseHeadersBuilder());

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void responseTrailerCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponseTrailers(ConnectResponseTrailersBuilder t, @Nullable ConnectError e) { log.add("first"); }
            }),
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onResponseTrailers(ConnectResponseTrailersBuilder t, @Nullable ConnectError e) { log.add("second"); }
            })
        ));

        ConnectServerCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onResponseTrailers(new ResponseTrailersBuilder(), null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void callCompleteCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { log.add("first"); }
            }),
            exchange -> ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { log.add("second"); }
            })
        ));

        ConnectServerCallObserver composite = pipeline.interceptCall(EXCHANGE).observer();
        composite.onCallComplete(null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void rejectionStopsIterationAndReturnsCompositeOfPriorObservers() {
        List<String> callOrder = new ArrayList<>();
        List<String> completedLog = new ArrayList<>();

        ConnectServerInterceptor first = exchange -> {
            callOrder.add("first");
            return ConnectServerInterceptor.continueWith(new ConnectServerCallObserver() {
                @Override public void onCallComplete(@Nullable ConnectError e) { completedLog.add("first"); }
            });
        };
        ConnectError rejectError = ConnectError.permissionDenied("no");
        ConnectServerInterceptor rejecting = exchange -> {
            callOrder.add("rejecting");
            return ConnectServerInterceptor.reject(rejectError);
        };
        ConnectServerInterceptor notReached = exchange -> {
            callOrder.add("notReached");
            return ConnectServerInterceptor.continueCall();
        };

        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(first, rejecting, notReached));

        ConnectServerInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectServerInterceptor.Decision.Reject.class);
        assertThat(((ConnectServerInterceptor.Decision.Reject) decision).error()).isSameAs(rejectError);
        assertThat(callOrder).containsExactly("first", "rejecting");

        // The composite observer from prior Continue decisions can still be invoked for terminal callbacks
        decision.observer().onCallComplete(rejectError);
        assertThat(completedLog).containsExactly("first");
    }

    @Test
    void rejectionWithNoPriorContinueObserversReturnsNoOp() {
        ConnectServerInterceptorPipeline pipeline = new ConnectServerInterceptorPipeline(List.of(
            exchange -> ConnectServerInterceptor.reject(ConnectError.unauthenticated("go away"))
        ));

        ConnectServerInterceptor.Decision decision = pipeline.interceptCall(EXCHANGE);

        assertThat(decision).isInstanceOf(ConnectServerInterceptor.Decision.Reject.class);
        assertThat(decision.observer()).isSameAs(ConnectServerCallObserver.NOOP);
    }
}
