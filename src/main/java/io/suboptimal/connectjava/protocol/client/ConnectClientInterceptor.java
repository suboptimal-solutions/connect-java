package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectClientCallStartBuilder;
import io.suboptimal.connectjava.api.ConnectError;

/**
 * Factory for per-call client-side Connect interceptors.
 *
 * <p>Registered interceptors are invoked in registration order before the request is sent. Each
 * interceptor mutates the outgoing request in place via the supplied
 * {@link ConnectClientCallStartBuilder} (headers, timeout, codec, GET preference) and then either
 * continues the call ({@link #continueCall()} / {@link #continueWith(ConnectClientCallObserver)})
 * or rejects it with a Connect-native error ({@link #reject(ConnectError)}). Later interceptors
 * observe earlier mutations, since they share the same builder. The call target
 * (service/method) is read-only.
 */
public interface ConnectClientInterceptor {
    /**
     * Called once per call, before the request is sent.
     *
     * @param callStart mutable view of the outgoing call; mutate in place to shape the request
     * @return decision to continue or reject the call
     */
    Decision interceptCall(ConnectClientCallStartBuilder callStart);

    /** Continues the call without attaching an observer. */
    static Decision continueCall() {
        return new Decision.Continue(ConnectClientCallObserver.NOOP);
    }

    /** Continues the call and attaches {@code observer}. */
    static Decision continueWith(ConnectClientCallObserver observer) {
        return new Decision.Continue(observer);
    }

    /** Rejects the call with the given Connect error; the request is never sent. */
    static Decision reject(ConnectError error) {
        return new Decision.Reject(ConnectClientCallObserver.NOOP, error);
    }

    sealed interface Decision permits Decision.Continue, Decision.Reject {
        ConnectClientCallObserver observer();

        /** Continue the call. The request was shaped in place via the builder. */
        record Continue(ConnectClientCallObserver observer) implements Decision {}

        record Reject(ConnectClientCallObserver observer, ConnectError error) implements Decision {}
    }
}
