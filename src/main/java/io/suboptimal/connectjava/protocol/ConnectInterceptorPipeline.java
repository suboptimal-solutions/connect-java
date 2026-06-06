package io.suboptimal.connectjava.protocol;

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
final class ConnectInterceptorPipeline {
    static final ConnectInterceptorPipeline EMPTY = new ConnectInterceptorPipeline(List.of());

    private final List<ConnectInterceptor> interceptors;

    ConnectInterceptorPipeline(List<ConnectInterceptor> interceptors) {
        this.interceptors = List.copyOf(interceptors);
    }

    /**
     * Runs all registered interceptors in order, building a composite observer from the
     * {@link ConnectInterceptor.Decision.Continue} results.
     *
     * <p>If any interceptor returns {@link ConnectInterceptor.Decision.Reject}, iteration stops
     * immediately and a {@link ConnectInterceptor.Decision.Reject} carrying the composite of all
     * prior {@link ConnectInterceptor.Decision.Continue} observers (and the rejection error) is
     * returned. {@link ConnectCallObserver#NOOP} observers are filtered out of the composite.
     */
    ConnectInterceptor.Decision interceptCall(ConnectCallExchange exchange) {
        if (interceptors.isEmpty()) {
            return ConnectInterceptor.continueCall();
        }

        List<ConnectCallObserver> observers = new ArrayList<>(interceptors.size());
        for (ConnectInterceptor interceptor : interceptors) {
            switch (interceptor.interceptCall(exchange)) {
                case ConnectInterceptor.Decision.Continue(ConnectCallObserver observer) -> observers.add(observer);
                case ConnectInterceptor.Decision.Reject(ConnectCallObserver ignore, ConnectError error) -> {
                    return new ConnectInterceptor.Decision.Reject(composite(observers), error);
                }
            }
        }
        return ConnectInterceptor.continueWith(composite(observers));
    }

    private static ConnectCallObserver composite(List<ConnectCallObserver> observers) {
        List<ConnectCallObserver> filtered = observers.stream()
                .filter(o -> o != ConnectCallObserver.NOOP)
                .toList();

        if (filtered.isEmpty()) {
            return ConnectCallObserver.NOOP;
        }
        if (filtered.size() == 1) {
            return filtered.getFirst();
        }
        return new CompositeConnectCallObserver(filtered);
    }

    private record CompositeConnectCallObserver(List<ConnectCallObserver> observers)
        implements ConnectCallObserver
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
