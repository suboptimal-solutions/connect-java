package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectResponseMeta;
import io.suboptimal.connectjava.api.ConnectError;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

final class ConnectClientInterceptorPipeline {
    static final ConnectClientInterceptorPipeline EMPTY = new ConnectClientInterceptorPipeline(List.of());

    private final List<ConnectClientInterceptor> interceptors;

    ConnectClientInterceptorPipeline(List<ConnectClientInterceptor> interceptors) {
        this.interceptors = List.copyOf(interceptors);
    }

    /**
     * Runs the interceptor chain. Returns a {@link ConnectClientInterceptor.Decision.Continue} whose
     * {@code callStart()} is the effective (possibly rewritten) request, or a
     * {@link ConnectClientInterceptor.Decision.Reject} if any interceptor rejected the call.
     */
    ConnectClientInterceptor.Decision interceptCall(ConnectClientCallStart callStart) {
        if (interceptors.isEmpty()) {
            return ConnectClientInterceptor.continueWith(callStart);
        }

        ConnectClientCallStart current = callStart;
        List<ConnectClientCallObserver> observers = new ArrayList<>(interceptors.size());
        for (ConnectClientInterceptor interceptor : interceptors) {
            switch (interceptor.interceptCall(current)) {
                case ConnectClientInterceptor.Decision.Continue(var observer, var modified) -> {
                    if (modified != null) {
                        current = modified;
                    }
                    observers.add(observer);
                }
                case ConnectClientInterceptor.Decision.Reject(ConnectClientCallObserver ignore, ConnectError error) -> {
                    return new ConnectClientInterceptor.Decision.Reject(composite(observers), error);
                }
            }
        }
        return ConnectClientInterceptor.continueWith(current, composite(observers));
    }

    private static ConnectClientCallObserver composite(List<ConnectClientCallObserver> observers) {
        List<ConnectClientCallObserver> filtered = observers.stream()
            .filter(o -> o != ConnectClientCallObserver.NOOP)
            .toList();

        if (filtered.isEmpty()) {
            return ConnectClientCallObserver.NOOP;
        }

        if (filtered.size() == 1) {
            return filtered.getFirst();
        }

        return new CompositeConnectClientCallObserver(filtered);
    }

    private record CompositeConnectClientCallObserver(List<ConnectClientCallObserver> observers)
        implements ConnectClientCallObserver
    {
        @Override
        public void onRequestPayload(Object payload) {
            observers.forEach(o -> o.onRequestPayload(payload));
        }

        @Override
        public void onRequestFinished() {
            observers.forEach(ConnectClientCallObserver::onRequestFinished);
        }

        @Override
        public void onResponseHeaders(ConnectResponseMeta meta) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onResponseHeaders(meta);
            }
        }

        @Override
        public void onResponsePayload(Object payload) {
            observers.forEach(o -> o.onResponsePayload(payload));
        }

        @Override
        public void onCallComplete(@Nullable ConnectError error) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onCallComplete(error);
            }
        }
    }
}
