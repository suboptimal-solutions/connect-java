package io.suboptimal.connectjava.protocol.server;

import io.suboptimal.connectjava.api.ConnectCallExchange;
import io.suboptimal.connectjava.api.ConnectError;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the per-call observer chain for registered Connect interceptors.
 */
final class ConnectServerInterceptorPipeline {
    static final ConnectServerInterceptorPipeline EMPTY = new ConnectServerInterceptorPipeline(List.of());

    private final List<ConnectServerInterceptor> interceptors;

    ConnectServerInterceptorPipeline(List<ConnectServerInterceptor> interceptors) {
        this.interceptors = List.copyOf(interceptors);
    }

    /**
     * Runs all registered interceptors in order, building a composite observer from the
     * {@link ConnectServerInterceptor.Decision.Continue} results.
     *
     * <p>If any interceptor returns {@link ConnectServerInterceptor.Decision.Reject}, iteration stops
     * immediately and a {@link ConnectServerInterceptor.Decision.Reject} carrying the composite of all
     * prior {@link ConnectServerInterceptor.Decision.Continue} observers (and the rejection error) is
     * returned. {@link ConnectServerCallObserver#NOOP} observers are filtered out of the composite.
     */
    ConnectServerInterceptor.Decision interceptCall(ConnectCallExchange exchange) {
        if (interceptors.isEmpty()) {
            return ConnectServerInterceptor.continueCall();
        }

        List<ConnectServerCallObserver> observers = new ArrayList<>(interceptors.size());
        for (ConnectServerInterceptor interceptor : interceptors) {
            switch (interceptor.interceptCall(exchange)) {
                case ConnectServerInterceptor.Decision.Continue(ConnectServerCallObserver observer) -> observers.add(observer);
                case ConnectServerInterceptor.Decision.Reject(ConnectServerCallObserver ignore, ConnectError error) -> {
                    return new ConnectServerInterceptor.Decision.Reject(composite(observers), error);
                }
            }
        }
        return ConnectServerInterceptor.continueWith(composite(observers));
    }

    private static ConnectServerCallObserver composite(List<ConnectServerCallObserver> observers) {
        List<ConnectServerCallObserver> filtered = observers.stream()
                .filter(o -> o != ConnectServerCallObserver.NOOP)
                .toList();

        if (filtered.isEmpty()) {
            return ConnectServerCallObserver.NOOP;
        }
        if (filtered.size() == 1) {
            return filtered.getFirst();
        }
        return new CompositeConnectCallObserver(filtered);
    }

    private record CompositeConnectCallObserver(List<ConnectServerCallObserver> observers)
        implements ConnectServerCallObserver
    {
        @Override
        public void onResponseHeaders(ConnectResponseHeadersBuilder headers) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onResponseHeaders(headers);
            }
        }

        @Override
        public void onResponsePayload(Object payload) {
            observers.forEach(observer -> observer.onResponsePayload(payload));
        }

        @Override
        public void onResponseTrailers(ConnectResponseTrailersBuilder trailers, @Nullable ConnectError error) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onResponseTrailers(trailers, error);
            }
        }

        @Override
        public void onCallComplete(@Nullable ConnectError error) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onCallComplete(error);
            }
        }
    }
}
