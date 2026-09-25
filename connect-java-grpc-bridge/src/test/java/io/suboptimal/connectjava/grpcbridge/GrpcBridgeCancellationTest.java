package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How a call ends: {@code onComplete} versus {@code onCancel}, and what {@code isCancelled()}
 * reports on either side of it.
 *
 * <p>The {@code Listener} contract allows exactly one of the two, whatever the channel does
 * afterwards, and the state machine is what enforces that. The pair is also split across two
 * executors - the context is cancelled on the raw one so that it cannot queue behind blocked
 * service code, while the decision of whether {@code onCancel} is owed stays on the call executor -
 * so these run in both modes, where the two submissions are ordered differently.
 */
class GrpcBridgeCancellationTest {
    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void normalCompletionInvokesOnComplete(ExecutorMode mode) {
        var completed = new boolean[1];
        ServerInterceptor interceptor = onCompleteInterceptor(() -> completed[0] = true);
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(completed[0]).isTrue();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorClosingCallDuringStartCallInvokesOnCompleteNotOnCancel(ExecutorMode mode) {
        var events = new ArrayList<String>();
        // Canonical gRPC auth-rejection pattern: close the call inside interceptCall and
        // return a listener without ever delegating to next.startCall().
        ServerInterceptor rejectingInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                call.close(Status.UNAUTHENTICATED.withDescription("no token"), new Metadata());
                return new ServerCall.Listener<ReqT>() {
                    @Override
                    public void onComplete() {
                        events.add("onComplete");
                    }

                    @Override
                    public void onCancel() {
                        events.add("onCancel");
                    }
                };
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
            () -> ServerInterceptors.intercept(service, rejectingInterceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.close();

        ConnectError error = call.readOutbound();
        assertThat(error.code()).isEqualTo(ConnectErrorCode.UNAUTHENTICATED);
        assertThat(events).containsExactly("onComplete");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void isCancelledReflectsChannelCloseButNotNormalCompletion(ExecutorMode mode) {
        var callRef = new ServerCall<?, ?>[1];
        ServerInterceptor capturingInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                callRef[0] = call;
                return next.startCall(call, headers);
            }
        };

        // Never completes, so closing the channel cancels a call that is still in flight.
        var hangingService = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request, StreamObserver<TestResponse> observer) {
            }
        };
        GrpcCallHarness hanging = new GrpcCallHarness(
            (BindableService) () -> ServerInterceptors.intercept(hangingService, capturingInterceptor),
            mode);
        hanging.writeInbound(hanging.exchange("ServerStream"));
        hanging.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        hanging.writeInbound(ConnectEndOfStream.INSTANCE);
        assertThat(callRef[0].isCancelled()).isFalse();
        hanging.close();
        assertThat(callRef[0].isCancelled()).isTrue();

        // A call that completed normally must not report itself as cancelled, even once the
        // channel goes away afterwards.
        var completingService = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };
        GrpcCallHarness done = new GrpcCallHarness(
            (BindableService) () -> ServerInterceptors.intercept(completingService, capturingInterceptor),
            mode);
        done.writeInbound(done.exchange("Unary"));
        done.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        done.writeInbound(ConnectEndOfStream.INSTANCE);
        done.close();
        assertThat(callRef[0].isCancelled()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void channelCloseBeforeCompletionCancelsListener(ExecutorMode mode) {
        var cancelled = new boolean[1];
        ServerInterceptor interceptor = onCancelInterceptor(() -> cancelled[0] = true);
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request, StreamObserver<TestResponse> observer) {
                // Never completes: simulates a call still in flight when the client disconnects.
            }
        };

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("ServerStream"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);
        call.close();

        assertThat(cancelled[0]).isTrue();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void onCancelSeesTheCallAndItsContextAlreadyCancelled(ExecutorMode mode) {
        // Cancellation is delivered by a different executor from the one that raises the flag and
        // cancels the context. A callback that branches on isCancelled() before starting cleanup
        // would take the wrong branch if that callback could run first.
        var observations = new boolean[2];
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onCancel() {
                        observations[0] = call.isCancelled();
                        observations[1] = Context.current().isCancelled();
                        super.onCancel();
                    }
                };
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request, StreamObserver<TestResponse> observer) {
            }
        };

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("ServerStream"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);
        call.close();

        assertThat(observations[0]).as("ServerCall.isCancelled() inside onCancel").isTrue();
        assertThat(observations[1]).as("Context.current().isCancelled() inside onCancel").isTrue();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void channelCloseAfterNormalCompletionDoesNotCancelListener(ExecutorMode mode) {
        var events = new ArrayList<String>();
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onComplete() {
                        events.add("onComplete");
                        super.onComplete();
                    }

                    @Override
                    public void onCancel() {
                        events.add("onCancel");
                        super.onCancel();
                    }
                };
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                observer.onNext(TestResponse.getDefaultInstance());
                observer.onCompleted();
            }
        };

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);
        call.close();

        // The Listener contract allows exactly one of the two, whatever the channel does after.
        assertThat(events).containsExactly("onComplete");
    }

    static ServerInterceptor onCompleteInterceptor(Runnable onComplete) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onComplete() {
                        onComplete.run();
                        super.onComplete();
                    }
                };
            }
        };
    }

    static ServerInterceptor onCancelInterceptor(Runnable onCancel) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
                return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onCancel() {
                        onCancel.run();
                        super.onCancel();
                    }
                };
            }
        };
    }
}
