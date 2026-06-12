package io.suboptimal.connectjava.api;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;

/**
 * Terminal signal indicating the end of one side of an RPC payload stream.
 *
 * <p>For streaming calls the {@code trailers} field carries trailing metadata from the
 * end-stream envelope, and {@code error} is non-null when the end-stream envelope carried
 * an error. A consumer must treat this message as terminal in both cases: a non-null
 * {@code error} means the call failed, and the trailers are still available.
 * For unary calls use {@link #INSTANCE} (no trailers, no error).
 */
public record ConnectEndOfStream(Map<String, List<String>> trailers, @Nullable ConnectError error)
        implements ConnectMessage {
    public static final ConnectEndOfStream INSTANCE = new ConnectEndOfStream(Map.of(), null);

    public ConnectEndOfStream(Map<String, List<String>> trailers) {
        this(trailers, null);
    }
}
