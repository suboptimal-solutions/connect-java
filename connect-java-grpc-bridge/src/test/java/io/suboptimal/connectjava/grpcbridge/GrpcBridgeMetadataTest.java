package io.suboptimal.connectjava.grpcbridge;

import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.BindableService;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * gRPC {@link Metadata} crossing the bridge in both directions, as a service sees it.
 *
 * <p>{@link MetadataMappingTest} covers the conversion itself; these tests cover it in place -
 * an interceptor reading request headers, and one setting response headers and trailers that have
 * to reach the {@code ConnectCallExchange} builders. That last part is why they run in both modes:
 * the builders are mutated inside the write hop, which is inline in one mode and posted in the
 * other.
 */
class GrpcBridgeMetadataTest {
    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorCanSetBinaryResponseTrailers(ExecutorMode mode) {
        byte[] raw = {7, 8, (byte) 0xFF};
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                var wrapped = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        trailers.put(
                            Metadata.Key.of("x-trailer-bin", Metadata.BINARY_BYTE_MARSHALLER),
                            raw);
                        super.close(status, trailers);
                    }
                };
                return next.startCall(wrapped, headers);
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
        var trailersBuilder = new TestResponseTrailersBuilder();

        call.writeInbound(call.exchange("Unary", new TestResponseHeadersBuilder(), trailersBuilder));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(trailersBuilder.allValues("x-trailer-bin"))
            .containsExactly(Base64.getEncoder().withoutPadding().encodeToString(raw));
    }

    static final Metadata.Key<String> CUSTOM_HEADER_KEY =
        Metadata.Key.of("x-custom", Metadata.ASCII_STRING_MARSHALLER);

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorCanReadIncomingRequestHeaders(ExecutorMode mode) {
        var capturedValue = new String[1];
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                capturedValue[0] = headers.get(CUSTOM_HEADER_KEY);
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

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        call.writeInbound(call.exchange("Unary", Map.of("x-custom", List.of("test-value"))));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(capturedValue[0]).isEqualTo("test-value");
    }

    static final Metadata.Key<byte[]> CUSTOM_BINARY_HEADER_KEY =
        Metadata.Key.of("x-custom-bin", Metadata.BINARY_BYTE_MARSHALLER);

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorCanReadIncomingBinaryRequestHeaders(ExecutorMode mode) {
        var capturedValue = new byte[1][];
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                capturedValue[0] = headers.get(CUSTOM_BINARY_HEADER_KEY);
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

        BindableService intercepted = () -> ServerInterceptors.intercept(service, interceptor);
        GrpcCallHarness call = new GrpcCallHarness(intercepted, mode);

        byte[] rawValue = {1, 2, 3, (byte) 0xFF};
        String encoded = Base64.getEncoder().withoutPadding().encodeToString(rawValue);
        call.writeInbound(call.exchange("Unary", Map.of("x-custom-bin", List.of(encoded))));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(capturedValue[0]).isEqualTo(rawValue);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorCanSetResponseHeaders(ExecutorMode mode) {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                var wrapped = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void sendHeaders(Metadata responseHeaders) {
                        responseHeaders.put(
                            Metadata.Key.of("x-response-header", Metadata.ASCII_STRING_MARSHALLER),
                            "header-value");
                        super.sendHeaders(responseHeaders);
                    }
                };
                return next.startCall(wrapped, headers);
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
        var headersBuilder = new TestResponseHeadersBuilder();

        call.writeInbound(call.exchange("Unary", headersBuilder, new TestResponseTrailersBuilder()));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(headersBuilder.allValues("x-response-header"))
            .containsExactly("header-value");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void interceptorCanSetResponseTrailers(ExecutorMode mode) {
        ServerInterceptor interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next)
            {
                var wrapped = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void close(Status status, Metadata trailers) {
                        trailers.put(
                            Metadata.Key.of("x-trailer", Metadata.ASCII_STRING_MARSHALLER),
                            "trailer-value");
                        super.close(status, trailers);
                    }
                };
                return next.startCall(wrapped, headers);
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
        var trailersBuilder = new TestResponseTrailersBuilder();

        call.writeInbound(call.exchange("Unary", new TestResponseHeadersBuilder(), trailersBuilder));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        assertThat(trailersBuilder.allValues("x-trailer"))
            .containsExactly("trailer-value");
    }
}
