package io.suboptimal.connectjava.api;

/**
 * Connect protocol error code.
 *
 * <p>The enum covers the full code list defined by the Connect protocol. The wire name is
 * available via {@link #wireName()}. Transport-specific status mappings live outside the API
 * module.
 */
public enum ConnectErrorCode {
    CANCELED("canceled"),
    UNKNOWN("unknown"),
    INVALID_ARGUMENT("invalid_argument"),
    DEADLINE_EXCEEDED("deadline_exceeded"),
    NOT_FOUND("not_found"),
    ALREADY_EXISTS("already_exists"),
    PERMISSION_DENIED("permission_denied"),
    RESOURCE_EXHAUSTED("resource_exhausted"),
    FAILED_PRECONDITION("failed_precondition"),
    ABORTED("aborted"),
    OUT_OF_RANGE("out_of_range"),
    UNIMPLEMENTED("unimplemented"),
    INTERNAL("internal"),
    UNAVAILABLE("unavailable"),
    DATA_LOSS("data_loss"),
    UNAUTHENTICATED("unauthenticated");

    private final String wireName;

    ConnectErrorCode(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }
}
