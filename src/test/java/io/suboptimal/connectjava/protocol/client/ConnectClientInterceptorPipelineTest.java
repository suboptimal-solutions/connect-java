package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import io.suboptimal.connectjava.protocol.ClientTestSupport;
import io.suboptimal.connectjava.testfixtures.UnaryPostRequest;
import io.suboptimal.connectjava.testfixtures.UnaryPostResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientInterceptorPipelineTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "svc.Service", List.of(METHOD), null);
    private static final ConnectClientCallStart CALL_START = new ConnectClientCallStart(
        SERVICE, METHOD, Map.of(), false, "proto");
    private static final ConnectResponseMeta META =
        new ConnectResponseMeta(200, Map.of(), Map.of());

    @Test
    void emptyPipelineContinues() {
        ConnectClientInterceptor.Decision d = ConnectClientInterceptorPipeline.EMPTY.interceptCall(CALL_START);

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Continue.class);
        assertThat(d.observer()).isSameAs(ConnectClientCallObserver.NOOP);
        assertThat(((ConnectClientInterceptor.Decision.Continue) d).callStart()).isSameAs(CALL_START);
    }

    @Test
    void allNoOpObserversProduceNoOp() {
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueCall(),
            cs -> ConnectClientInterceptor.continueCall()
        ));

        assertThat(pipeline.interceptCall(CALL_START).observer()).isSameAs(ConnectClientCallObserver.NOOP);
    }

    @Test
    void singleNonNoOpObserverIsReturnedDirectly() {
        ClientTestSupport.RecordingObserver observer = new ClientTestSupport.RecordingObserver();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(observer)
        ));

        assertThat(pipeline.interceptCall(CALL_START).observer()).isSameAs(observer);
    }

    @Test
    void noOpObserversAreFilteredFromComposite() {
        ClientTestSupport.RecordingObserver real = new ClientTestSupport.RecordingObserver();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueCall(),
            cs -> ConnectClientInterceptor.continueWith(real),
            cs -> ConnectClientInterceptor.continueCall()
        ));

        assertThat(pipeline.interceptCall(CALL_START).observer()).isSameAs(real);
    }

    @Test
    void requestPayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestPayload(Object p) { log.add("first"); }
            }),
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestPayload(Object p) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(CALL_START).observer();
        composite.onRequestPayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void requestFinishedCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestFinished() { log.add("first"); }
            }),
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestFinished() { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(CALL_START).observer();
        composite.onRequestFinished();

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responsePayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("first"); }
            }),
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(CALL_START).observer();
        composite.onResponsePayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responseHeaderCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseMeta m) { log.add("first"); }
            }),
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseMeta m) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(CALL_START).observer();
        composite.onResponseHeaders(META);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void callCompleteCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { log.add("first"); }
            }),
            cs -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(CALL_START).observer();
        composite.onCallComplete(null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void rejectionStopsIterationAndReturnsCompositeOfPriorObservers() {
        List<String> callOrder = new ArrayList<>();
        List<String> completedLog = new ArrayList<>();

        ConnectClientInterceptor first = cs -> {
            callOrder.add("first");
            return ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { completedLog.add("first"); }
            });
        };
        ConnectError rejectError = ConnectError.permissionDenied("no");
        ConnectClientInterceptor rejecting = cs -> {
            callOrder.add("rejecting");
            return ConnectClientInterceptor.reject(rejectError);
        };
        ConnectClientInterceptor notReached = cs -> {
            callOrder.add("notReached");
            return ConnectClientInterceptor.continueCall();
        };

        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(
            List.of(first, rejecting, notReached));

        ConnectClientInterceptor.Decision d = pipeline.interceptCall(CALL_START);

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Reject.class);
        assertThat(((ConnectClientInterceptor.Decision.Reject) d).error()).isSameAs(rejectError);
        assertThat(callOrder).containsExactly("first", "rejecting");

        d.observer().onCallComplete(rejectError);
        assertThat(completedLog).containsExactly("first");
    }

    @Test
    void rejectionWithNoPriorContinueObserversReturnsNoOp() {
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            cs -> ConnectClientInterceptor.reject(ConnectError.unauthenticated("go away"))
        ));

        ConnectClientInterceptor.Decision d = pipeline.interceptCall(CALL_START);

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Reject.class);
        assertThat(d.observer()).isSameAs(ConnectClientCallObserver.NOOP);
    }

    @Test
    void rewriteIsThreadedToNextInterceptor() {
        AtomicReference<ConnectClientCallStart> seenBySecond = new AtomicReference<>();

        ConnectClientInterceptor first = cs -> ConnectClientInterceptor.continueWith(cs.withHeader("x-a", "1"));
        ConnectClientInterceptor second = cs -> {
            seenBySecond.set(cs);
            return ConnectClientInterceptor.continueWith(cs.withHeader("x-b", "2"));
        };

        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(first, second));
        ConnectClientInterceptor.Decision d = pipeline.interceptCall(CALL_START);

        assertThat(seenBySecond.get().requestHeaders()).containsKey("x-a");

        ConnectClientCallStart effective = ((ConnectClientInterceptor.Decision.Continue) d).callStart();
        assertThat(effective.requestHeaders()).containsKey("x-a");
        assertThat(effective.requestHeaders()).containsKey("x-b");
    }
}
