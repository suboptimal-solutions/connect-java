package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.BindableService;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.channel.embedded.EmbeddedChannel;
import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import io.suboptimal.connectjava.api.ConnectRequestMeta;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The call itself: unary and streaming exchanges, and every way one can fail without the failure
 * escaping into the Netty pipeline.
 *
 * <p>One test class per behaviour, and a behaviour earns its own class once it needs a fixture of
 * its own. What lives elsewhere: {@link GrpcBridgeCancellationTest} for how a call ends,
 * {@link GrpcBridgeMetadataTest} for headers and trailers, {@link GrpcBridgeErrorDetailsTest} for
 * {@code grpc-status-details-bin}, {@link GrpcBridgeFlowControlTest} for inbound demand and the
 * auto-read watermark, {@link GrpcBridgeWritabilityTest} for {@code isReady}/{@code onReady}.
 *
 * <p>Nearly every test here is parameterized over {@link ExecutorMode}, because that is where the
 * value is: the direct mode and the executor mode reach the same result along visibly different
 * paths - inline versus queued call tasks, inline versus posted writes - and only running both
 * catches a change that holds for one of them.
 *
 * <p>What is deliberately <em>not</em> asserted is the relative order of a listener callback and
 * the terminal message reaching the pipeline: in executor mode the terminal write is posted, so
 * {@code onComplete} is delivered before it lands, while in direct mode it lands first. Nothing
 * observable to a service depends on that, and pinning it would freeze an implementation detail.
 */
class GrpcBridgeHandlerTest {
    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void unaryCallReturnsResponse(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.newBuilder()
                    .setText("echo: " + request.getText()).build());
                observer.onCompleted();
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(
            TestRequest.newBuilder().setText("hello").build()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectPayload outPayload = call.readOutbound();
        TestResponse response = (TestResponse) outPayload.data();
        assertThat(response.getText()).isEqualTo("echo: hello");

        Object terminal = call.readOutbound();
        assertThat(terminal).isInstanceOf(ConnectEndOfStream.class);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void unaryCallPropagatesError(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("bad request")
                    .asRuntimeException());
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INVALID_ARGUMENT);
        assertThat(error.message()).isEqualTo("bad request");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void closeFromInsideACallbackWinsOverAThrowThatFollows(ExecutorMode mode) {
        // The one thing GrpcCallExecutor requires rather than merely permits: a task submitted from
        // inside a running task runs inline. close() hops onto the call executor, so were it
        // deferred, the throw below would reach failInternal while the call is still active and the
        // client would be told INTERNAL instead of the status the service picked.
        ServerInterceptor closeThenThrow = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onHalfClose() {
                        // Never delegates, so the service method itself never runs.
                        call.close(Status.PERMISSION_DENIED.withDescription("denied"),
                            new Metadata());
                        throw new RuntimeException("boom");
                    }
                };
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {};

        BindableService intercepted = () -> ServerInterceptors.intercept(service, closeThenThrow);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.PERMISSION_DENIED);
        assertThat(error.message()).isEqualTo("denied");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void unknownMethodIsReportedAsConnectErrorNotThrown(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {};
        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        ConnectServiceDefinition sd = TestExchanges.serviceOf(call.bridge);
        ConnectMethodDefinition unknown = new ConnectMethodDefinition(
            "NoSuchMethod", ConnectMethodType.UNARY, TestRequest.class, TestResponse.class, false);

        call.writeInbound(new ConnectCallExchange(sd, unknown,
            new ConnectRequestMeta(Map.of()),
            new TestResponseHeadersBuilder(), new TestResponseTrailersBuilder()));

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.UNIMPLEMENTED);
        assertThat(error.message()).contains("NoSuchMethod");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void exceptionFromStartCallIsReportedAsConnectError(ExecutorMode mode) {
        var listenerNotified = new boolean[1];
        ServerInterceptor throwingInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                throw Status.RESOURCE_EXHAUSTED.withDescription("quota").asRuntimeException();
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                listenerNotified[0] = true;
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        BindableService intercepted =
            () -> ServerInterceptors.intercept(service, throwingInterceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);
        call.close();

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.RESOURCE_EXHAUSTED);
        assertThat(error.message()).isEqualTo("quota");
        assertThat(listenerNotified[0]).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void exceptionFromServiceMethodIsReportedAsInternalConnectError(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                throw new RuntimeException("boom");
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        // The exception text stays server-side - only a fixed description reaches the client.
        assertThat(error.message())
            .isEqualTo("Application error processing RPC")
            .doesNotContain("boom");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void illegalStateEscapingAListenerCallbackIsReportedAsInternalConnectError(ExecutorMode mode) {
        ServerInterceptor eagerHeadersInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                // Sends headers itself, then lets the stub send them again on first onNext.
                call.sendHeaders(new Metadata());
                return next.startCall(call, headers);
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        BindableService intercepted =
            () -> ServerInterceptors.intercept(service, eagerHeadersInterceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        // The stub's observer keeps its own "headers sent" flag, which the interceptor's direct
        // call did not set, so it sends them again on first onNext. BridgeServerCall throws
        // IllegalStateException, which escapes onHalfClose and terminates the call.
        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.message())
            .isEqualTo("Application error processing RPC")
            .doesNotContain("sendHeaders");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void sendMessageBeforeSendHeadersThrowsAndLeavesTheCallUsable(ExecutorMode mode) {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        ServerInterceptor eagerMessageInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                // Skips sendHeaders, which gRPC requires before the first message.
                @SuppressWarnings("unchecked")
                RespT message = (RespT) TestResponse.getDefaultInstance();
                thrown.set(catchThrowable(() -> call.sendMessage(message)));
                return next.startCall(call, headers);
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        BindableService intercepted =
            () -> ServerInterceptors.intercept(service, eagerMessageInterceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        // Reported to the caller rather than to the client, exactly as gRPC's ServerCall does.
        assertThat(thrown.get())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("sendHeaders has not been called");

        // A ServerCall method that throws leaves the call untouched, so the RPC still succeeds.
        Object payload = call.readOutbound();
        Object terminal = call.readOutbound();
        assertThat(payload).isInstanceOf(ConnectPayload.class);
        assertThat(terminal).isSameAs(ConnectEndOfStream.INSTANCE);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void illegalStateThrownInsideStartCallTerminatesTheCall(ExecutorMode mode) {
        ServerInterceptor eagerMessageInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                @SuppressWarnings("unchecked")
                RespT message = (RespT) TestResponse.getDefaultInstance();
                call.sendMessage(message); // throws out of startCall
                return next.startCall(call, headers);
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {};

        BindableService intercepted =
            () -> ServerInterceptors.intercept(service, eagerMessageInterceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));

        // Status.fromThrowable maps a plain exception to UNKNOWN with no description, which is
        // what gRPC's ServerImpl writes when startCall throws.
        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.UNKNOWN);
        assertThat(error.message()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void payloadBeforeExchangeIsReportedAsInternalConnectError(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {};
        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        // Deliberately says nothing about which message arrived: the description reaches the
        // client, and the bridge's own type names are not the client's business.
        assertThat(error.message()).isEqualTo("RPC request hasn't been started");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void endOfStreamBeforeExchangeIsReportedAsInternalConnectError(ExecutorMode mode) {
        // Shares the enqueue path with the payload case above, and therefore its description.
        // Pinning both is what would catch the two being split apart again.
        var service = new TestServiceGrpc.TestServiceImplBase() {};
        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.message()).isEqualTo("RPC request hasn't been started");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void serverStreamingReturnsMultipleResponses(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request,
                                     StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.newBuilder().setText("one").build());
                observer.onNext(TestResponse.newBuilder().setText("two").build());
                observer.onCompleted();
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(call.exchange("ServerStream"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        TestResponse first = (TestResponse) ((ConnectPayload) call.readOutbound()).data();
        assertThat(first.getText()).isEqualTo("one");

        TestResponse second = (TestResponse) ((ConnectPayload) call.readOutbound()).data();
        assertThat(second.getText()).isEqualTo("two");

        Object terminal = call.readOutbound();
        assertThat(terminal).isInstanceOf(ConnectEndOfStream.class);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void clientStreamingAccumulatesRequests(ExecutorMode mode) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public StreamObserver<TestRequest> clientStream(
                StreamObserver<TestResponse> responseObserver)
            {
                var received = new ArrayList<String>();
                return new StreamObserver<>() {
                    @Override
                    public void onNext(TestRequest request) {
                        received.add(request.getText());
                    }

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onCompleted() {
                        responseObserver.onNext(TestResponse.newBuilder()
                            .setText(String.join(",", received)).build());
                        responseObserver.onCompleted();
                    }
                };
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);

        call.writeInbound(call.exchange("ClientStream"));
        call.writeInbound(new ConnectPayload(
            TestRequest.newBuilder().setText("a").build()));
        call.writeInbound(new ConnectPayload(
            TestRequest.newBuilder().setText("b").build()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        TestResponse response =
            (TestResponse) ((ConnectPayload) call.readOutbound()).data();
        assertThat(response.getText()).isEqualTo("a,b");

        Object terminal = call.readOutbound();
        assertThat(terminal).isInstanceOf(ConnectEndOfStream.class);
    }

    @Test
    void responsesFromAServiceThreadKeepTheirOrder() {
        // The case SerializingCallExecutor exists for: the service answers from a thread that is
        // neither the event loop nor the one draining the call executor, so every submission loses
        // the drain latch and goes on the queue. The drain in progress - parked in join() below -
        // picks them up when the service method returns.
        //
        // Executor mode only. Under an EmbeddedChannel the direct mode cannot express this at all:
        // EmbeddedEventLoop.inEventLoop() answers true for every thread, so the service thread
        // would be taken for the loop and the test would prove nothing.
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request, StreamObserver<TestResponse> observer) {
                Thread worker = new Thread(() -> {
                    for (int i = 0; i < 8; i++) {
                        observer.onNext(TestResponse.newBuilder().setText("msg-" + i).build());
                    }
                    observer.onCompleted();
                }, "service-thread");
                worker.start();
                try {
                    worker.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        };

        ManualExecutor executor = new ManualExecutor();
        ConnectGrpcBridge bridge =
            ConnectGrpcBridge.builder().withServiceExecutor(executor).addService(service).build();
        EmbeddedChannel channel = new EmbeddedChannel(bridge.create());

        channel.writeInbound(TestExchanges.create(bridge, "ServerStream", Map.of()));
        channel.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        channel.writeInbound(ConnectEndOfStream.INSTANCE);
        ExecutorUtil.settleAllTasks(channel, executor);

        var texts = new ArrayList<String>();
        Object out;
        while ((out = channel.readOutbound()) instanceof ConnectPayload payload) {
            texts.add(((TestResponse) payload.data()).getText());
        }

        assertThat(texts).containsExactly(
            "msg-0", "msg-1", "msg-2", "msg-3", "msg-4", "msg-5", "msg-6", "msg-7");
        assertThat(out).isSameAs(ConnectEndOfStream.INSTANCE);
    }

    @Test
    void inExecutorModeNothingRunsUntilBothQueuesAreDrained() {
        var serviceRan = new boolean[1];
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                serviceRan[0] = true;
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        ManualExecutor executor = new ManualExecutor();
        ConnectGrpcBridge bridge =
            ConnectGrpcBridge.builder().withServiceExecutor(executor).addService(service).build();
        EmbeddedChannel channel = new EmbeddedChannel(bridge.create());

        channel.writeInbound(TestExchanges.create(bridge, "Unary", Map.of()));
        channel.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        channel.writeInbound(ConnectEndOfStream.INSTANCE);

        // writeInbound only hands the events to the call executor, which is still holding them.
        assertThat(serviceRan[0]).isFalse();
        Object beforeDrain = channel.readOutbound();
        assertThat(beforeDrain).isNull();

        // Draining runs the call to completion, but runsOnEventLoop() is false in this mode, so
        // every write it made was posted to the loop instead of performed inline - and nothing has
        // run the loop yet.
        assertThat(executor.runAllTasks()).isPositive();
        assertThat(serviceRan[0]).isTrue();
        Object beforeLoop = channel.readOutbound();
        assertThat(beforeLoop).isNull();

        channel.runPendingTasks();
        Object payload = channel.readOutbound();
        Object terminal = channel.readOutbound();
        assertThat(payload).isInstanceOf(ConnectPayload.class);
        assertThat(terminal).isSameAs(ConnectEndOfStream.INSTANCE);
    }
}
