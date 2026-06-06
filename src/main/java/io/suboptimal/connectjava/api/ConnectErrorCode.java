package io.suboptimal.connectjava.api;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Connect protocol error code.
 *
 * <p>The enum covers the full code list defined by the Connect protocol. The wire name is
 * available via {@link #wireName()}, and {@link #httpStatus()} is the HTTP status used for
 * unary Connect error responses produced by this protocol implementation.
 */
public enum ConnectErrorCode {
    CANCELED("canceled", new HttpResponseStatus(499, "Client Closed Request")),
    UNKNOWN("unknown", HttpResponseStatus.INTERNAL_SERVER_ERROR),
    INVALID_ARGUMENT("invalid_argument", HttpResponseStatus.BAD_REQUEST),
    DEADLINE_EXCEEDED("deadline_exceeded", HttpResponseStatus.GATEWAY_TIMEOUT),
    NOT_FOUND("not_found", HttpResponseStatus.NOT_FOUND),
    ALREADY_EXISTS("already_exists", HttpResponseStatus.CONFLICT),
    PERMISSION_DENIED("permission_denied", HttpResponseStatus.FORBIDDEN),
    RESOURCE_EXHAUSTED("resource_exhausted", HttpResponseStatus.TOO_MANY_REQUESTS),
    FAILED_PRECONDITION("failed_precondition", HttpResponseStatus.BAD_REQUEST),
    ABORTED("aborted", HttpResponseStatus.CONFLICT),
    OUT_OF_RANGE("out_of_range", HttpResponseStatus.BAD_REQUEST),
    UNIMPLEMENTED("unimplemented", HttpResponseStatus.NOT_IMPLEMENTED),
    INTERNAL("internal", HttpResponseStatus.INTERNAL_SERVER_ERROR),
    UNAVAILABLE("unavailable", HttpResponseStatus.SERVICE_UNAVAILABLE),
    DATA_LOSS("data_loss", HttpResponseStatus.INTERNAL_SERVER_ERROR),
    UNAUTHENTICATED("unauthenticated", HttpResponseStatus.UNAUTHORIZED);

    private final String wireName;
    private final HttpResponseStatus httpStatus;

    ConnectErrorCode(String wireName, HttpResponseStatus httpStatus) {
        this.wireName = wireName;
        this.httpStatus = httpStatus;
    }

    public String wireName() {
        return wireName;
    }

    public HttpResponseStatus httpStatus() {
        return httpStatus;
    }
}

