package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectError;
import org.jspecify.annotations.Nullable;

/**
 * Factory for per-call client-side Connect interceptors.
 *
 * <p>Registered interceptors are invoked in registration order when a {@link ConnectClientCallStart}
 * is written, before the request is sent. Each interceptor either continues the call by returning
 * {@link #continueCall()} / {@link #continueWith(ConnectClientCallObserver)}, optionally rewriting
 * the outgoing request via {@link #continueWith(ConnectClientCallStart)} /
 * {@link #continueWith(ConnectClientCallStart, ConnectClientCallObserver)}, or rejects it with a
 * Connect-native error by returning {@link #reject(ConnectError)}.
 *
 * <p>An interceptor that rewrites the {@link ConnectClientCallStart} shapes the request the client
 * is about to emit (headers, timeout, codec, GET preference). The rewritten value is threaded to the
 * next interceptor in the chain, so later interceptors observe earlier rewrites. The intent is to
 * adjust request metadata; rebuilding the record with a different service or method is possible but
 * outside the intended contract.
 */
public interface ConnectClientInterceptor {
    /**
     * Called once per call, before the request is sent.
     *
     * @param callStart the (possibly already rewritten) outgoing call description
     * @return decision to continue (optionally rewriting the request) or reject the call
     */
    Decision interceptCall(ConnectClientCallStart callStart);

    /** Continues the call without attaching an observer or rewriting the request. */
    static Decision continueCall() {
        return new Decision.Continue(ConnectClientCallObserver.NOOP, null);
    }

    /** Continues the call and attaches {@code observer}, without rewriting the request. */
    static Decision continueWith(ConnectClientCallObserver observer) {
        return new Decision.Continue(observer, null);
    }

    /** Continues the call with the rewritten {@code callStart}, without attaching an observer. */
    static Decision continueWith(ConnectClientCallStart callStart) {
        return new Decision.Continue(ConnectClientCallObserver.NOOP, callStart);
    }

    /** Continues the call with the rewritten {@code callStart} and attaches {@code observer}. */
    static Decision continueWith(ConnectClientCallStart callStart, ConnectClientCallObserver observer) {
        return new Decision.Continue(observer, callStart);
    }

    /** Rejects the call with the given Connect error; the request is never sent. */
    static Decision reject(ConnectError error) {
        return new Decision.Reject(ConnectClientCallObserver.NOOP, error);
    }

    sealed interface Decision permits Decision.Continue, Decision.Reject {
        ConnectClientCallObserver observer();

        /**
         * Continue the call, optionally rewriting the outgoing request.
         *
         * @param observer  observer that receives client call lifecycle callbacks
         * @param callStart rewritten request, or {@code null} to leave it unchanged
         */
        record Continue(ConnectClientCallObserver observer,
                        @Nullable ConnectClientCallStart callStart) implements Decision {}

        record Reject(ConnectClientCallObserver observer, ConnectError error) implements Decision {}
    }
}
