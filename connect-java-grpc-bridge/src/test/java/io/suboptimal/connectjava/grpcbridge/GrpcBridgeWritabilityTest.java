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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.embedded.EmbeddedChannel;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Outbound flow control: {@code ServerCall.isReady()} and {@code Listener.onReady()}.
 *
 * <p>Writability is driven through {@code ChannelOutboundBuffer.setUserDefinedWritability}, the
 * hook Netty gives handlers that need to hold a channel back for reasons of their own. It flips the
 * same flag the write-buffer watermarks do and fires the same {@code channelWritabilityChanged},
 * without the test having to guess how many bytes an unflushed {@code ConnectPayload} is worth.
 */
class GrpcBridgeWritabilityTest {

    /** Index reserved for a user of the channel; 0 is Netty's own watermark bit. */
    private static final int TEST_WRITABILITY_INDEX = 1;

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void isReadyFollowsTheChannel(ExecutorMode mode) {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.startCall(mode);

        assertThat(fixture.call().isReady())
            .as("a fresh channel takes writes")
            .isTrue();

        setWritable(call, false);
        assertThat(fixture.call().isReady()).isFalse();

        setWritable(call, true);
        assertThat(fixture.call().isReady()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void onReadyIsDeliveredWhenTheChannelBecomesWritableAgain(ExecutorMode mode) {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.startCall(mode);

        // One from starting the call, which gRPC also delivers unprompted on stream allocation.
        assertThat(fixture.readyNotifications).isEqualTo(1);

        setWritable(call, false);
        assertThat(fixture.readyNotifications)
            .as("becoming unwritable is what isReady() is for, not a callback")
            .isEqualTo(1);

        setWritable(call, true);
        assertThat(fixture.readyNotifications).isEqualTo(2);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void onReadyStopsOnceTheCallIsOver(ExecutorMode mode) {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.startCall(mode);

        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);
        int afterCompletion = fixture.readyNotifications;

        setWritable(call, false);
        setWritable(call, true);

        assertThat(fixture.readyNotifications)
            .as("a finished call has nothing left to write")
            .isEqualTo(afterCompletion);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void isReadyIsFalseOnceTheCallIsClosedEvenOnAWritableChannel(ExecutorMode mode) {
        // gRPC reports a closed call as not ready regardless of the transport
        // (ServerCallImpl.isReady:202-207) - there is nothing left that may be sent.
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.startCall(mode);

        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(call.channel.isWritable()).isTrue();
        assertThat(fixture.call().isReady()).isFalse();
    }

    /** Flips the channel's writability and lets the resulting event reach the listener. */
    static void setWritable(GrpcCallHarness call, boolean writable) {
        EmbeddedChannel channel = call.channel;
        //noinspection resource
        channel.unsafe().outboundBuffer().setUserDefinedWritability(TEST_WRITABILITY_INDEX, writable);
        // Netty fires channelWritabilityChanged from a task rather than inline.
        channel.runPendingTasks();
        call.settle();
    }

    /** A unary call whose {@code ServerCall} and {@code onReady} count the test can inspect. */
    static final class Fixture {
        int readyNotifications;

        private @Nullable ServerCall<?, ?> call;

        ServerCall<?, ?> call() {
            assertThat(call).as("the call was captured by the interceptor").isNotNull();
            return call;
        }

        GrpcCallHarness startCall(ExecutorMode mode) {
            GrpcCallHarness harness = new GrpcCallHarness(service(), mode);
            harness.writeInbound(harness.exchange("Unary"));
            return harness;
        }

        private BindableService service() {
            var stub = new TestServiceGrpc.TestServiceImplBase() {
                @Override
                public void unary(TestRequest request, StreamObserver<TestResponse> observer) {
                    observer.onNext(TestResponse.getDefaultInstance());
                    observer.onCompleted();
                }
            };
            ServerInterceptor interceptor = new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> serverCall, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next)
                {
                    call = serverCall;
                    ServerCall.Listener<ReqT> delegate = next.startCall(serverCall, headers);
                    return new ForwardingServerCallListener
                        .SimpleForwardingServerCallListener<>(delegate) {
                        @Override
                        public void onReady() {
                            readyNotifications++;
                            super.onReady();
                        }
                    };
                }
            };
            return () -> ServerInterceptors.intercept(stub, interceptor);
        }
    }

    /**
     * A service that keeps writing while the channel is unwritable is not stopped by the bridge -
     * {@code isReady()} is advice, and gRPC says so. What must hold is that ignoring it costs
     * buffering and nothing else: every message still arrives, in order.
     */
    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void ignoringIsReadyStillDeliversEveryMessageInOrder(ExecutorMode mode) {
        var observed = new ArrayList<Boolean>();
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void serverStream(TestRequest request, StreamObserver<TestResponse> observer) {
                var serverObserver = (ServerCallStreamObserver<TestResponse>) observer;
                for (int i = 0; i < 4; i++) {
                    observed.add(serverObserver.isReady());
                    observer.onNext(TestResponse.newBuilder().setText("r" + i).build());
                }
                observer.onCompleted();
            }
        };
        GrpcCallHarness call = new GrpcCallHarness(service, mode);
        call.writeInbound(call.exchange("ServerStream"));

        //noinspection resource
        call.channel.unsafe().outboundBuffer()
            .setUserDefinedWritability(TEST_WRITABILITY_INDEX, false);
        call.channel.runPendingTasks();

        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(observed).containsExactly(false, false, false, false);

        List<String> texts = new ArrayList<>();
        Object out;
        while ((out = call.readOutbound()) instanceof ConnectPayload payload) {
            texts.add(((TestResponse) payload.data()).getText());
        }
        assertThat(texts).containsExactly("r0", "r1", "r2", "r3");
        assertThat(out).isSameAs(ConnectEndOfStream.INSTANCE);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void anExceptionFromOnReadyTerminatesTheCall(ExecutorMode mode) {
        ServerInterceptor throwingOnReady = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> serverCall, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                ServerCall.Listener<ReqT> delegate = next.startCall(serverCall, headers);
                return new ForwardingServerCallListener
                    .SimpleForwardingServerCallListener<>(delegate) {
                    @Override
                    public void onReady() {
                        throw Status.RESOURCE_EXHAUSTED.withDescription("busy").asRuntimeException();
                    }
                };
            }
        };
        var service = new TestServiceGrpc.TestServiceImplBase() {};
        BindableService intercepted = () -> ServerInterceptors.intercept(service, throwingOnReady);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary"));

        // Reported like any other escaped callback: a fixed description, never the exception text.
        ConnectError error = call.readOutbound();
        assertThat(error.code())
            .isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.message())
            .isEqualTo("Application error processing RPC")
            .doesNotContain("busy");
    }
}
