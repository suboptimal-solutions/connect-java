package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectErrorCode;
import org.jetbrains.annotations.ApiStatus;

/**
 * HTTP status mappings used by Connect transports.
 *
 * <p>The server and client mappings are intentionally separate: multiple Connect error codes can
 * produce the same HTTP status, while a client has to choose one fallback code for an HTTP response
 * that does not contain a valid Connect error body.
 *
 * <p>This type is public only so transport modules can share it. It is not part of the stable API.
 */
@ApiStatus.Internal
public final class ConnectHttpErrorMapping {
    private ConnectHttpErrorMapping() {}

    /** Returns the HTTP status used by the server for a unary Connect error. */
    public static int statusFor(ConnectErrorCode code) {
        return switch (code) {
            case CANCELED -> 499;
            case UNKNOWN, INTERNAL, DATA_LOSS -> 500;
            case INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE -> 400;
            case DEADLINE_EXCEEDED -> 504;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS, ABORTED -> 409;
            case PERMISSION_DENIED -> 403;
            case RESOURCE_EXHAUSTED -> 429;
            case UNIMPLEMENTED -> 501;
            case UNAVAILABLE -> 503;
            case UNAUTHENTICATED -> 401;
        };
    }

    /** Returns the fallback Connect code for an HTTP response without a valid Connect error body. */
    public static ConnectErrorCode codeForStatus(int status) {
        return switch (status) {
            case 400 -> ConnectErrorCode.INVALID_ARGUMENT;
            case 401 -> ConnectErrorCode.UNAUTHENTICATED;
            case 403 -> ConnectErrorCode.PERMISSION_DENIED;
            case 404 -> ConnectErrorCode.UNIMPLEMENTED;
            case 408 -> ConnectErrorCode.DEADLINE_EXCEEDED;
            case 412 -> ConnectErrorCode.FAILED_PRECONDITION;
            case 413, 431 -> ConnectErrorCode.RESOURCE_EXHAUSTED;
            case 429, 502, 503, 504 -> ConnectErrorCode.UNAVAILABLE;
            default -> ConnectErrorCode.UNKNOWN;
        };
    }
}
