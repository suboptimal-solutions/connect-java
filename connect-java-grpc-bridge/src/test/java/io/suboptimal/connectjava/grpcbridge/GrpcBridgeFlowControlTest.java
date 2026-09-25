package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.BindableService;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.stub.StreamObserver;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Inbound flow control: the queue, {@code request(n)} demand, and the auto-read watermark.
 *
 * <p>Every test here drives demand by hand, because the generated stubs do not leave a queue to
 * observe: {@code ServerCalls} tops demand up from inside {@code onMessage}, so a message is
 * consumed as fast as it arrives. {@link Fixture} therefore swallows the stub's automatic
 * {@code request(n)} until a test opens the gate, which is the same position a service using
 * {@code disableAutoRequest()} puts the bridge in.
 */
class GrpcBridgeFlowControlTest {

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void messagesWaitForDemandAndThenArriveInOrder(ExecutorMode mode) {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.start(mode);

        writePayloads(call, 5);

        assertThat(fixture.received)
            .as("nothing may reach the service before it asks")
            .isEmpty();

        // Partial demand, so that the queue is drained by the counter rather than emptied: asking
        // for everything at once would pass even if the delivery loop ignored demand entirely.
        fixture.request(2);
        call.settle();

        assertThat(fixture.received).containsExactly("m0", "m1");

        fixture.request(3);
        call.settle();

        assertThat(fixture.received).containsExactly("m0", "m1", "m2", "m3", "m4");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void halfCloseArrivesAfterEveryQueuedMessageAndOnlyOnce(ExecutorMode mode) {
        // The service deliberately does not answer on completion, so the call stays active
        // afterwards and a second delivery would be observable. A service that answers inline
        // closes the call and hides the question.
        Fixture fixture = new Fixture();
        fixture.respondOnCompleted = false;
        GrpcCallHarness call = fixture.start(mode);

        writePayloads(call, 3);
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(fixture.events)
            .as("half close may not overtake messages the service has not seen")
            .isEmpty();

        // Exactly enough demand for the messages and none left over, which is the whole question:
        // a service that asks for the N it expects still has to be told the client is done. The
        // stub's automatic top-up stays suppressed so that nothing funds the marker by accident.
        fixture.request(3);
        call.settle();

        assertThat(fixture.received).containsExactly("m0", "m1", "m2");
        assertThat(fixture.events).containsExactly("onHalfClose", "onCompleted");

        // Half close is not a message and is not charged against demand, so leftover demand must
        // not produce it a second time.
        fixture.request(1);
        call.settle();

        assertThat(fixture.events).containsExactly("onHalfClose", "onCompleted");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void halfCloseIsNotRedeliveredByARequestMadeFromInsideIt(ExecutorMode mode) {
        // Legal via ServerCallStreamObserver.request(int), and the queue is empty at that point -
        // which used to re-enter the delivery loop and hand the callback to itself.
        Fixture fixture = new Fixture();
        fixture.respondOnCompleted = false;
        fixture.requestFromInsideHalfClose = true;
        GrpcCallHarness call = fixture.start(mode);

        call.writeInbound(ConnectEndOfStream.INSTANCE);
        call.settle();

        assertThat(fixture.events).containsExactly("onHalfClose", "onCompleted");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void autoReadStopsAtTheHighWatermarkAndResumesOnceDrained(ExecutorMode mode) {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.start(mode);

        assertThat(call.channel.config().isAutoRead())
            .as("Netty's own default, which the handler starts from")
            .isTrue();

        writePayloads(call, HIGH_WATERMARK);

        assertThat(call.channel.config().isAutoRead())
            .as("a service that stopped asking must stop the socket")
            .isFalse();

        fixture.request(HIGH_WATERMARK);
        call.settle();

        assertThat(fixture.received).hasSize(HIGH_WATERMARK);
        // Draining is the only event left that can lift the watermark: with the socket muted, no
        // further message arrives to trigger the check.
        assertThat(call.channel.config().isAutoRead())
            .as("reads have to resume once the service caught up")
            .isTrue();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void aBacklogDrainedByOneRequestDoesNotGrowTheStack(ExecutorMode mode) {
        // A stub auto-requests from inside onMessage, and a submission from the call executor's own
        // thread runs inline - so without a reentrancy guard the delivery loop recurses once per
        // queued message rather than unwinding, at roughly five frames each.
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.start(mode);

        int backlog = 200;
        writePayloads(call, backlog);

        // The gate is open here on purpose: the stub's request(1) from inside onMessage is
        // exactly the reentrant submission the guard exists for.
        fixture.forwardRequests = true;
        fixture.request(backlog);
        call.settle();

        assertThat(fixture.received)
            .containsExactlyElementsOf(IntStream.range(0, backlog).mapToObj(i -> "m" + i).toList());
        assertThat(fixture.lastStackDepth - fixture.firstStackDepth)
            .as("stack frames added between the first delivery and the last of %d", backlog)
            .isLessThan(20);
    }

    @Test
    void requestRejectsNonPositiveDemand() {
        Fixture fixture = new Fixture();
        fixture.start(ExecutorMode.DIRECT);

        // Reported to the caller rather than to the client, on the caller's own thread, like the
        // IllegalStateException checks on the other ServerCall methods.
        assertThatThrownBy(() -> fixture.request(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fixture.request(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void demandSaturatesInsteadOfWrappingToNegative() {
        Fixture fixture = new Fixture();
        GrpcCallHarness call = fixture.start(ExecutorMode.DIRECT);

        // gRPC lets a service ask for everything at once, so two such calls are legal. Adding them
        // up wraps an int to negative, which reads as "no demand" and stops the call for good.
        fixture.request(Integer.MAX_VALUE);
        fixture.request(Integer.MAX_VALUE);
        call.settle();

        writePayloads(call, 1);

        assertThat(fixture.received).containsExactly("m0");
    }

    /** Mirrors {@code GrpcBridgeHandler.MESSAGE_Q_HIGH_WATERMARK}, which is private. */
    private static final int HIGH_WATERMARK = 128;

    static void writePayloads(GrpcCallHarness call, int count) {
        for (int i = 0; i < count; i++) {
            call.writeInbound(new ConnectPayload(TestRequest.newBuilder().setText("m" + i).build()));
        }
    }

    /**
     * A client-streaming call whose demand the test owns.
     *
     * <p>Flags are read while the call runs, so set them before {@link #start} unless a test is
     * deliberately changing course mid-call.
     */
    static final class Fixture {
        final List<String> received = new ArrayList<>();
        /** Listener callbacks in the order the bridge delivered them. */
        final List<String> events = new ArrayList<>();

        /**
          * While false, the stub's automatic {@code request(n)} is swallowed. It does not affect
          * {@link #request}, which drives the underlying call directly - a test needs to grant an
          * exact amount of demand without the stub topping it back up.
          */
        boolean forwardRequests;
        /** When false the service never answers, so the call stays active after half close. */
        boolean respondOnCompleted = true;
        /** Asks for one more message from inside {@code onHalfClose}. */
        boolean requestFromInsideHalfClose;

        int firstStackDepth;
        int lastStackDepth;

        private @Nullable ServerCall<?, ?> call;

        GrpcCallHarness start(ExecutorMode mode) {
            GrpcCallHarness harness = new GrpcCallHarness(service(), mode);
            harness.writeInbound(harness.exchange("ClientStream"));
            return harness;
        }

        /** Demand from the service's side, on the {@code ServerCall} the bridge itself created. */
        void request(int numMessages) {
            assertThat(call).as("the call was captured by the interceptor").isNotNull();
            call.request(numMessages);
        }

        private BindableService service() {
            var stub = new TestServiceGrpc.TestServiceImplBase() {
                @Override
                public StreamObserver<TestRequest> clientStream(StreamObserver<TestResponse> out) {
                    return new StreamObserver<>() {
                        @Override
                        public void onNext(TestRequest request) {
                            int depth = Thread.currentThread().getStackTrace().length;
                            if (received.isEmpty()) {
                                firstStackDepth = depth;
                            }
                            lastStackDepth = depth;
                            received.add(request.getText());
                        }

                        @Override
                        public void onError(Throwable t) {}

                        @Override
                        public void onCompleted() {
                            events.add("onCompleted");
                            if (respondOnCompleted) {
                                out.onNext(TestResponse.newBuilder()
                                    .setText(String.join(",", received)).build());
                                out.onCompleted();
                            }
                        }
                    };
                }
            };
            return () -> ServerInterceptors.intercept(stub, interceptor());
        }

        private ServerInterceptor interceptor() {
            return new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> serverCall, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next)
                {
                    call = serverCall;
                    var gated =
                        new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(serverCall) {
                            @Override
                            public void request(int numMessages) {
                                if (forwardRequests) {
                                    super.request(numMessages);
                                }
                            }
                        };

                    ServerCall.Listener<ReqT> delegate = next.startCall(gated, headers);
                    return new ForwardingServerCallListener
                        .SimpleForwardingServerCallListener<>(delegate) {
                        @Override
                        public void onHalfClose() {
                            events.add("onHalfClose");
                            if (requestFromInsideHalfClose) {
                                serverCall.request(1);
                            }
                            super.onHalfClose();
                        }
                    };
                }
            };
        }
    }
}
