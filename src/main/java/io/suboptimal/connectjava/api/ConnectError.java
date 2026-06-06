package io.suboptimal.connectjava.api;

import java.util.List;

/**
 * Connect-native error.
 */
public record ConnectError(ConnectErrorCode code, String message, List<ConnectErrorDetail> details)
    implements ConnectMessage
{
    public ConnectError(ConnectErrorCode code, String message) {
        this(code, message, List.of());
    }

    public static ConnectError canceled(String message) {
        return new ConnectError(ConnectErrorCode.CANCELED, message);
    }

    public static ConnectError unknown(String message) {
        return new ConnectError(ConnectErrorCode.UNKNOWN, message);
    }

    public static ConnectError invalidArgument(String message) {
        return new ConnectError(ConnectErrorCode.INVALID_ARGUMENT, message);
    }

    public static ConnectError deadlineExceeded(String message) {
        return new ConnectError(ConnectErrorCode.DEADLINE_EXCEEDED, message);
    }

    public static ConnectError notFound(String message) {
        return new ConnectError(ConnectErrorCode.NOT_FOUND, message);
    }

    public static ConnectError alreadyExists(String message) {
        return new ConnectError(ConnectErrorCode.ALREADY_EXISTS, message);
    }

    public static ConnectError permissionDenied(String message) {
        return new ConnectError(ConnectErrorCode.PERMISSION_DENIED, message);
    }

    public static ConnectError resourceExhausted(String message) {
        return new ConnectError(ConnectErrorCode.RESOURCE_EXHAUSTED, message);
    }

    public static ConnectError failedPrecondition(String message) {
        return new ConnectError(ConnectErrorCode.FAILED_PRECONDITION, message);
    }

    public static ConnectError aborted(String message) {
        return new ConnectError(ConnectErrorCode.ABORTED, message);
    }

    public static ConnectError outOfRange(String message) {
        return new ConnectError(ConnectErrorCode.OUT_OF_RANGE, message);
    }

    public static ConnectError unimplemented(String message) {
        return new ConnectError(ConnectErrorCode.UNIMPLEMENTED, message);
    }

    public static ConnectError internal(String message) {
        return new ConnectError(ConnectErrorCode.INTERNAL, message);
    }

    public static ConnectError unavailable(String message) {
        return new ConnectError(ConnectErrorCode.UNAVAILABLE, message);
    }

    public static ConnectError dataLoss(String message) {
        return new ConnectError(ConnectErrorCode.DATA_LOSS, message);
    }

    public static ConnectError unauthenticated(String message) {
        return new ConnectError(ConnectErrorCode.UNAUTHENTICATED, message);
    }
}
