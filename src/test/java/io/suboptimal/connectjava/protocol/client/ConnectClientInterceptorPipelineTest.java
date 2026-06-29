package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectClientCallStart;
import io.suboptimal.connectjava.api.ConnectClientCallStartBuilder;
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

import static org.assertj.core.api.Assertions.assertThat;

class ConnectClientInterceptorPipelineTest {
    private static final ConnectMethodDefinition METHOD = new ConnectMethodDefinition(
        "Unary", ConnectMethodType.UNARY, UnaryPostRequest.class, UnaryPostResponse.class, false);
    private static final ConnectServiceDefinition SERVICE = new ConnectServiceDefinition(
        "svc.Service", List.of(METHOD), null);
    private static final ConnectClientCallStart CALL_START = new ConnectClientCallStart(
        SERVICE, METHOD, Map.of(), false, "proto");
    private static final ConnectResponseMeta META =
        new ConnectResponseMeta(200, Map.of());

    private ConnectClientCallStartBuilder newBuilder() {
        return new ConnectClientCallStartBuilder(CALL_START);
    }

    @Test
    void emptyPipelineContinues() {
        ConnectClientCallStartBuilder builder = newBuilder();
        ConnectClientInterceptor.Decision d = ConnectClientInterceptorPipeline.EMPTY.interceptCall(builder);

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Continue.class);
        assertThat(d.observer()).isSameAs(ConnectClientCallObserver.NOOP);
    }

    @Test
    void allNoOpObserversProduceNoOp() {
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueCall(),
            b -> ConnectClientInterceptor.continueCall()
        ));

        assertThat(pipeline.interceptCall(newBuilder()).observer()).isSameAs(ConnectClientCallObserver.NOOP);
    }

    @Test
    void singleNonNoOpObserverIsReturnedDirectly() {
        ClientTestSupport.RecordingObserver observer = new ClientTestSupport.RecordingObserver();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(observer)
        ));

        assertThat(pipeline.interceptCall(newBuilder()).observer()).isSameAs(observer);
    }

    @Test
    void noOpObserversAreFilteredFromComposite() {
        ClientTestSupport.RecordingObserver real = new ClientTestSupport.RecordingObserver();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueCall(),
            b -> ConnectClientInterceptor.continueWith(real),
            b -> ConnectClientInterceptor.continueCall()
        ));

        assertThat(pipeline.interceptCall(newBuilder()).observer()).isSameAs(real);
    }

    @Test
    void requestPayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestPayload(Object p) { log.add("first"); }
            }),
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestPayload(Object p) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(newBuilder()).observer();
        composite.onRequestPayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void requestFinishedCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestFinished() { log.add("first"); }
            }),
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onRequestFinished() { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(newBuilder()).observer();
        composite.onRequestFinished();

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responsePayloadCallbacksAreFIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("first"); }
            }),
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponsePayload(Object p) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(newBuilder()).observer();
        composite.onResponsePayload("x");

        assertThat(log).containsExactly("first", "second");
    }

    @Test
    void responseHeaderCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseMeta m) { log.add("first"); }
            }),
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onResponseHeaders(ConnectResponseMeta m) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(newBuilder()).observer();
        composite.onResponseHeaders(META);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void callCompleteCallbacksAreLIFO() {
        List<String> log = new ArrayList<>();
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { log.add("first"); }
            }),
            b -> ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { log.add("second"); }
            })
        ));

        ConnectClientCallObserver composite = pipeline.interceptCall(newBuilder()).observer();
        composite.onCallComplete(null);

        assertThat(log).containsExactly("second", "first");
    }

    @Test
    void rejectionStopsIterationAndReturnsCompositeOfPriorObservers() {
        List<String> callOrder = new ArrayList<>();
        List<String> completedLog = new ArrayList<>();

        ConnectClientInterceptor first = b -> {
            callOrder.add("first");
            return ConnectClientInterceptor.continueWith(new ConnectClientCallObserver() {
                @Override public void onCallComplete(ConnectError e) { completedLog.add("first"); }
            });
        };
        ConnectError rejectError = ConnectError.permissionDenied("no");
        ConnectClientInterceptor rejecting = b -> {
            callOrder.add("rejecting");
            return ConnectClientInterceptor.reject(rejectError);
        };
        ConnectClientInterceptor notReached = b -> {
            callOrder.add("notReached");
            return ConnectClientInterceptor.continueCall();
        };

        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(
            List.of(first, rejecting, notReached));

        ConnectClientInterceptor.Decision d = pipeline.interceptCall(newBuilder());

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Reject.class);
        assertThat(((ConnectClientInterceptor.Decision.Reject) d).error()).isSameAs(rejectError);
        assertThat(callOrder).containsExactly("first", "rejecting");

        d.observer().onCallComplete(rejectError);
        assertThat(completedLog).containsExactly("first");
    }

    @Test
    void rejectionWithNoPriorContinueObserversReturnsNoOp() {
        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(
            b -> ConnectClientInterceptor.reject(ConnectError.unauthenticated("go away"))
        ));

        ConnectClientInterceptor.Decision d = pipeline.interceptCall(newBuilder());

        assertThat(d).isInstanceOf(ConnectClientInterceptor.Decision.Reject.class);
        assertThat(d.observer()).isSameAs(ConnectClientCallObserver.NOOP);
    }

    @Test
    void mutationsAreSharedAcrossInterceptors() {
        ConnectClientCallStartBuilder builder = newBuilder();

        ConnectClientInterceptor first = b -> {
            b.addHeader("x-a", "1");
            return ConnectClientInterceptor.continueCall();
        };
        ConnectClientInterceptor second = b -> {
            // second interceptor sees mutation from first since they share the builder
            assertThat(b.headerValues("x-a")).containsExactly("1");
            b.addHeader("x-b", "2");
            return ConnectClientInterceptor.continueCall();
        };

        ConnectClientInterceptorPipeline pipeline = new ConnectClientInterceptorPipeline(List.of(first, second));
        pipeline.interceptCall(builder);

        ConnectClientCallStart effective = builder.build();
        assertThat(effective.requestHeaders()).containsKey("x-a");
        assertThat(effective.requestHeaders()).containsKey("x-b");
    }
}
