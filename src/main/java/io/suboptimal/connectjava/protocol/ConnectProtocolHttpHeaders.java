package io.suboptimal.connectjava.protocol;

import org.jetbrains.annotations.ApiStatus;

@ApiStatus.Internal
public final class ConnectProtocolHttpHeaders {
    public static final CharSequence CONNECT_PROTOCOL_VERSION = "connect-protocol-version";
    public static final CharSequence CONNECT_CONTENT_ENCODING = "connect-content-encoding";
    public static final CharSequence CONNECT_ACCEPT_ENCODING = "connect-accept-encoding";
    public static final CharSequence CONNECT_TIMEOUT_MS = "connect-timeout-ms";

    private ConnectProtocolHttpHeaders() {}
}
