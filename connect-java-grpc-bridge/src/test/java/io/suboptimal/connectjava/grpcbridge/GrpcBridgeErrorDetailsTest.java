package io.suboptimal.connectjava.grpcbridge;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import connectjava.grpcbridge.test.v1.TestRequest;
import connectjava.grpcbridge.test.v1.TestResponse;
import connectjava.grpcbridge.test.v1.TestServiceGrpc;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.suboptimal.connectjava.api.ConnectEndOfStream;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectErrorCode;
import io.suboptimal.connectjava.api.ConnectErrorDetail;
import io.suboptimal.connectjava.api.ConnectPayload;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapping of gRPC's {@code grpc-status-details-bin} trailer onto Connect error details.
 *
 * <p>Run in both {@link ExecutorMode}s because the mapping happens inside the terminal write, which
 * is made inline in one mode and posted to the event loop in the other.
 */
class GrpcBridgeErrorDetailsTest {

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void singleDetailIsMappedToConnectError(ExecutorMode mode) {
        byte[] retryInfoBytes = new byte[]{0x0a, 0x02, 0x08, 0x1e};
        Any detail = Any.newBuilder()
            .setTypeUrl("type.googleapis.com/google.rpc.RetryInfo")
            .setValue(ByteString.copyFrom(retryInfoBytes))
            .build();
        com.google.rpc.Status richStatus = com.google.rpc.Status.newBuilder()
            .setCode(com.google.rpc.Code.UNAVAILABLE_VALUE)
            .setMessage("try again")
            .addDetails(detail)
            .build();

        ConnectError error = runWithGrpcError(mode, StatusProto.toStatusRuntimeException(richStatus));

        assertThat(error.code()).isEqualTo(ConnectErrorCode.UNAVAILABLE);
        assertThat(error.message()).isEqualTo("try again");
        assertThat(error.details()).hasSize(1);
        ConnectErrorDetail d = error.details().get(0);
        assertThat(d.type()).isEqualTo("google.rpc.RetryInfo");
        assertThat(d.value()).isEqualTo(retryInfoBytes);
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void multipleDetailsPreserveOrder(ExecutorMode mode) {
        Any first = Any.newBuilder()
            .setTypeUrl("type.googleapis.com/foo.First")
            .setValue(ByteString.copyFrom(new byte[]{0x01}))
            .build();
        Any second = Any.newBuilder()
            .setTypeUrl("type.googleapis.com/foo.Second")
            .setValue(ByteString.copyFrom(new byte[]{0x02}))
            .build();
        com.google.rpc.Status richStatus = com.google.rpc.Status.newBuilder()
            .setCode(com.google.rpc.Code.INTERNAL_VALUE)
            .setMessage("multi")
            .addDetails(first)
            .addDetails(second)
            .build();

        ConnectError error = runWithGrpcError(mode, StatusProto.toStatusRuntimeException(richStatus));

        assertThat(error.details()).hasSize(2);
        assertThat(error.details().get(0).type()).isEqualTo("foo.First");
        assertThat(error.details().get(1).type()).isEqualTo("foo.Second");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void asciiTrailersPreservedAlongsideDetails(ExecutorMode mode) {
        // The bridge consumes grpc-status-details-bin to build the details; every other trailer the
        // service set has to survive that and still reach the response.
        Any detail = Any.newBuilder()
            .setTypeUrl("type.googleapis.com/foo.Bar")
            .setValue(ByteString.copyFrom(new byte[]{0x01}))
            .build();
        com.google.rpc.Status richStatus = com.google.rpc.Status.newBuilder()
            .setCode(com.google.rpc.Code.INTERNAL_VALUE)
            .setMessage("boom")
            .addDetails(detail)
            .build();

        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest req, StreamObserver<TestResponse> obs) {
                Metadata trailers = new Metadata();
                trailers.put(
                    Metadata.Key.of("x-detail-trailer", Metadata.ASCII_STRING_MARSHALLER),
                    "kept");
                obs.onError(StatusProto.toStatusRuntimeException(richStatus, trailers));
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);
        var trailersBuilder = new TestResponseTrailersBuilder();
        call.writeInbound(
            call.exchange("Unary", new TestResponseHeadersBuilder(), trailersBuilder));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        ConnectError error = call.readOutbound();
        assertThat(error.details()).hasSize(1);
        assertThat(error.details().get(0).type()).isEqualTo("foo.Bar");
        assertThat(trailersBuilder.allValues("x-detail-trailer")).containsExactly("kept");
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void malformedStatusDetailsBinYieldsErrorWithoutDetails(ExecutorMode mode) {
        Metadata trailers = new Metadata();
        trailers.put(
            Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER),
            new byte[]{(byte) 0xff, (byte) 0xfe}); // invalid protobuf

        ConnectError error = runWithGrpcError(mode,
            Status.INTERNAL.withDescription("bad").asRuntimeException(trailers));

        assertThat(error.code()).isEqualTo(ConnectErrorCode.INTERNAL);
        assertThat(error.details()).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(ExecutorMode.class)
    void noStatusDetailsBinYieldsEmptyDetails(ExecutorMode mode) {
        ConnectError error = runWithGrpcError(mode,
            Status.NOT_FOUND.withDescription("gone").asRuntimeException());

        assertThat(error.code()).isEqualTo(ConnectErrorCode.NOT_FOUND);
        assertThat(error.details()).isEmpty();
    }

    /** Runs a unary call whose service fails with {@code error}, and returns what the client sees. */
    static ConnectError runWithGrpcError(ExecutorMode mode, Throwable error) {
        var service = new TestServiceGrpc.TestServiceImplBase() {
            @Override
            public void unary(TestRequest req, StreamObserver<TestResponse> obs) {
                obs.onError(error);
            }
        };

        GrpcCallHarness call = new GrpcCallHarness(service, mode);
        call.writeInbound(call.exchange("Unary"));
        call.writeInbound(new ConnectPayload(TestRequest.getDefaultInstance()));
        call.writeInbound(ConnectEndOfStream.INSTANCE);

        return call.readOutbound();
    }
}
