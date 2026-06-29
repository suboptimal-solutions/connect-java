package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectClientCallStartBuilder;
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

    ConnectClientInterceptor.Decision interceptCall(ConnectClientCallStartBuilder builder) {
        if (interceptors.isEmpty()) {
            return ConnectClientInterceptor.continueCall();
        }

        List<ConnectClientCallObserver> observers = new ArrayList<>(interceptors.size());
        for (ConnectClientInterceptor interceptor : interceptors) {
            switch (interceptor.interceptCall(builder)) {
                case ConnectClientInterceptor.Decision.Continue(var observer) -> observers.add(observer);
                case ConnectClientInterceptor.Decision.Reject(var ignore, var error) -> {
                    return new ConnectClientInterceptor.Decision.Reject(composite(observers), error);
                }
            }
        }
        return ConnectClientInterceptor.continueWith(composite(observers));
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
            for (int i = 0; i < observers.size(); i++) {
                observers.get(i).onRequestPayload(payload);
            }
        }

        @Override
        public void onRequestFinished() {
            for (int i = 0; i < observers.size(); i++) {
                observers.get(i).onRequestFinished();
            }
        }

        @Override
        public void onResponseHeaders(ConnectResponseMeta meta) {
            for (int i = observers.size() - 1; i >= 0; i--) {
                observers.get(i).onResponseHeaders(meta);
            }
        }

        @Override
        public void onResponsePayload(Object payload) {
            for (int i = 0; i < observers.size(); i++) {
                observers.get(i).onResponsePayload(payload);
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
