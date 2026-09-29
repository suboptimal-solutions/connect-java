package io.suboptimal.connectjava.conformance;

import connectrpc.conformance.v1.ConfigOuterClass.Code;
import io.grpc.Status;

final class ConformanceCodeMapping {

    static Status.Code toGrpcStatusCode(Code code) {
        return switch (code) {
            case CODE_CANCELED -> Status.Code.CANCELLED;
            case CODE_UNKNOWN -> Status.Code.UNKNOWN;
            case CODE_INVALID_ARGUMENT -> Status.Code.INVALID_ARGUMENT;
            case CODE_DEADLINE_EXCEEDED -> Status.Code.DEADLINE_EXCEEDED;
            case CODE_NOT_FOUND -> Status.Code.NOT_FOUND;
            case CODE_ALREADY_EXISTS -> Status.Code.ALREADY_EXISTS;
            case CODE_PERMISSION_DENIED -> Status.Code.PERMISSION_DENIED;
            case CODE_RESOURCE_EXHAUSTED -> Status.Code.RESOURCE_EXHAUSTED;
            case CODE_FAILED_PRECONDITION -> Status.Code.FAILED_PRECONDITION;
            case CODE_ABORTED -> Status.Code.ABORTED;
            case CODE_OUT_OF_RANGE -> Status.Code.OUT_OF_RANGE;
            case CODE_UNIMPLEMENTED -> Status.Code.UNIMPLEMENTED;
            case CODE_INTERNAL -> Status.Code.INTERNAL;
            case CODE_UNAVAILABLE -> Status.Code.UNAVAILABLE;
            case CODE_DATA_LOSS -> Status.Code.DATA_LOSS;
            case CODE_UNAUTHENTICATED -> Status.Code.UNAUTHENTICATED;
            case CODE_UNSPECIFIED, UNRECOGNIZED -> Status.Code.UNKNOWN;
        };
    }

    private ConformanceCodeMapping() {}
}
