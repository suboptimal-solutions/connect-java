package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectError;

/**
 * Factory for per-call Connect protocol observers.
 *
 * <p>Registered interceptors are invoked in registration order after Connect request metadata is
 * decoded and after basic Connect protocol validation (version header, compression negotiation).
 * Each interceptor either continues the call by returning {@link #continueCall()} /
 * {@link #continueWith(ConnectCallObserver)}, or rejects it with a Connect-native error by
 * returning {@link #reject(ConnectError)}.
 *
 * <p>If the interceptor returns {@link Decision.Continue}, the pipeline guarantees that the
 * supplied {@link ConnectCallObserver} receives exactly one {@link ConnectCallObserver#onCallComplete}
 * callback, regardless of whether the call succeeds, fails, or is cancelled by the client.
 *
 * <p>If the interceptor returns {@link Decision.Reject}, the call is stopped at the Connect layer;
 * the service is never invoked and no observer callbacks are delivered to that interceptor.
 */
public interface ConnectInterceptor {
    /**
     * Called once per Connect RPC call after request metadata has been decoded and basic protocol
     * validation (version header, compression negotiation) has passed.
     *
     * <p>Return {@link #continueCall()} or {@link #continueWith(ConnectCallObserver)} to let the
     * call proceed, or {@link #reject(ConnectError)} to abort it with a Connect-native error.
     *
     * @param exchange call metadata and mutable response builders for this RPC
     * @return decision to continue or reject the call
     */
    Decision interceptCall(ConnectCallExchange exchange);

    /**
     * Returns a {@link Decision} that continues the call without attaching an observer.
     */
    static Decision continueCall() {
        return new Decision.Continue(ConnectCallObserver.NOOP);
    }

    /**
     * Returns a {@link Decision} that continues the call and attaches {@code observer} to receive
     * lifecycle callbacks. The observer is guaranteed to receive exactly one
     * {@link ConnectCallObserver#onCallComplete} for the lifetime of the call.
     */
    static Decision continueWith(ConnectCallObserver observer) {
        return new Decision.Continue(observer);
    }

    /**
     * Returns a {@link Decision} that rejects the call with the given Connect error.
     *
     * <p>The service is not invoked. No observer callbacks are delivered for the rejecting
     * interceptor; however, observers from earlier interceptors that returned
     * {@link Decision.Continue} still receive their terminal callbacks.
     */
    static Decision reject(ConnectError error) {
        return new Decision.Reject(ConnectCallObserver.NOOP, error);
    }

    /**
     * Result of {@link ConnectInterceptor#interceptCall(ConnectCallExchange)}.
     *
     * <p>Use the static factory methods on {@link ConnectInterceptor} to create instances.
     * The {@link #observer()} accessor returns the effective observer associated with this decision;
     * for {@link Reject} it is the composite of all observers from prior {@link Continue} decisions.
     */
    sealed interface Decision {
        /** Returns the observer associated with this decision. */
        ConnectCallObserver observer();

        /**
         * Continue the call and attach a per-call observer.
         *
         * @param observer observer that receives Connect call lifecycle callbacks
         */
        record Continue(ConnectCallObserver observer) implements Decision {}

        /**
         * Reject the call with a Connect-native error.
         *
         * <p>The {@link #observer()} field holds the composite of all observers from prior
         * {@link Continue} decisions in the pipeline so that terminal callbacks can still be
         * delivered to already-started interceptors.
         *
         * @param observer composite observer of all prior {@link Continue} decisions
         * @param error error to serialize to the Connect response
         */
        record Reject(ConnectCallObserver observer, ConnectError error) implements Decision {}
    }
}

