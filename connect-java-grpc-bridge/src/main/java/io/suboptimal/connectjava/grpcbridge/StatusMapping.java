package io.suboptimal.connectjava.grpcbridge;

import io.grpc.Status;
import io.suboptimal.connectjava.api.ConnectErrorCode;

import java.util.EnumMap;
import java.util.Map;

/**
 * Bidirectional mapping between gRPC {@link Status.Code} and {@link ConnectErrorCode}.
 *
 * <p>Both enums cover the same 16 error codes. {@link Status.Code#OK} has no
 * {@code ConnectErrorCode} counterpart and maps to {@link ConnectErrorCode#UNKNOWN}.
 */
final class StatusMapping {
    private static final Map<Status.Code, ConnectErrorCode> TO_CONNECT;
    private static final Map<ConnectErrorCode, Status.Code> TO_GRPC;

    static {
        TO_CONNECT = new EnumMap<>(Status.Code.class);
        TO_CONNECT.put(Status.Code.CANCELLED, ConnectErrorCode.CANCELED);
        TO_CONNECT.put(Status.Code.UNKNOWN, ConnectErrorCode.UNKNOWN);
        TO_CONNECT.put(Status.Code.INVALID_ARGUMENT, ConnectErrorCode.INVALID_ARGUMENT);
        TO_CONNECT.put(Status.Code.DEADLINE_EXCEEDED, ConnectErrorCode.DEADLINE_EXCEEDED);
        TO_CONNECT.put(Status.Code.NOT_FOUND, ConnectErrorCode.NOT_FOUND);
        TO_CONNECT.put(Status.Code.ALREADY_EXISTS, ConnectErrorCode.ALREADY_EXISTS);
        TO_CONNECT.put(Status.Code.PERMISSION_DENIED, ConnectErrorCode.PERMISSION_DENIED);
        TO_CONNECT.put(Status.Code.RESOURCE_EXHAUSTED, ConnectErrorCode.RESOURCE_EXHAUSTED);
        TO_CONNECT.put(Status.Code.FAILED_PRECONDITION, ConnectErrorCode.FAILED_PRECONDITION);
        TO_CONNECT.put(Status.Code.ABORTED, ConnectErrorCode.ABORTED);
        TO_CONNECT.put(Status.Code.OUT_OF_RANGE, ConnectErrorCode.OUT_OF_RANGE);
        TO_CONNECT.put(Status.Code.UNIMPLEMENTED, ConnectErrorCode.UNIMPLEMENTED);
        TO_CONNECT.put(Status.Code.INTERNAL, ConnectErrorCode.INTERNAL);
        TO_CONNECT.put(Status.Code.UNAVAILABLE, ConnectErrorCode.UNAVAILABLE);
        TO_CONNECT.put(Status.Code.DATA_LOSS, ConnectErrorCode.DATA_LOSS);
        TO_CONNECT.put(Status.Code.UNAUTHENTICATED, ConnectErrorCode.UNAUTHENTICATED);

        TO_GRPC = new EnumMap<>(ConnectErrorCode.class);
        TO_CONNECT.forEach((grpc, connect) -> TO_GRPC.put(connect, grpc));
    }

    static ConnectErrorCode toConnect(Status.Code code) {
        ConnectErrorCode result = TO_CONNECT.get(code);
        return result != null ? result : ConnectErrorCode.UNKNOWN;
    }

    static Status.Code toGrpc(ConnectErrorCode code) {
        Status.Code result = TO_GRPC.get(code);
        return result != null ? result : Status.Code.UNKNOWN;
    }

    private StatusMapping() {}
}
