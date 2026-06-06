package io.suboptimal.connectjava.protocol;

import io.netty.handler.codec.http.HttpHeaders;
import io.suboptimal.connectjava.api.ConnectResponseHeadersBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects response header mutations and applies them to an {@link HttpHeaders} instance once
 * {@link #applyTo(HttpHeaders)} is called. Each instance may be applied at most once;
 * subsequent mutations or applications throw {@link IllegalStateException}.
 */
class ResponseHeadersBuilder implements ConnectResponseHeadersBuilder {
    private final List<HeaderOperation> operations = new ArrayList<>();
    private boolean applied;

    @Override
    public ResponseHeadersBuilder add(CharSequence name, CharSequence value) {
        verifyState();
        operations.add(new HeaderOperation(false, name, value));
        return this;
    }

    @Override
    public ResponseHeadersBuilder set(CharSequence name, CharSequence value) {
        verifyState();
        operations.add(new HeaderOperation(true, name, value));
        return this;
    }

    void applyTo(HttpHeaders target) {
        verifyState();
        for (HeaderOperation operation : operations) {
            if (operation.set()) {
                target.set(operation.name(), operation.value());
            } else {
                target.add(operation.name(), operation.value());
            }
        }
        applied = true;
    }

    private void verifyState() {
        if (applied) {
            throw new IllegalStateException("Already applied.");
        }
    }

    record HeaderOperation(boolean set, CharSequence name, CharSequence value) {
    }
}
