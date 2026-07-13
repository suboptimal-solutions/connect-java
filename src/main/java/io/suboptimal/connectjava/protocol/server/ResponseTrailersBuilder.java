package io.suboptimal.connectjava.protocol.server;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.AsciiString;
import io.suboptimal.connectjava.api.ConnectResponseTrailersBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects response trailer mutations and applies them either to HTTP trailing headers
 * (unary) or to a {@link ConnectEndStreamMeta.Builder} (streaming). Each instance may be
 * applied at most once; subsequent mutations or applications throw {@link IllegalStateException}.
 */
final class ResponseTrailersBuilder implements ConnectResponseTrailersBuilder {
    private static final AsciiString TRAILER_PREFIX = AsciiString.cached("Trailer-");

    private final List<HeaderOperation> operations = new ArrayList<>();
    private boolean applied;

    @Override
    public ResponseTrailersBuilder add(CharSequence name, CharSequence value) {
        verifyState();
        operations.add(new HeaderOperation(false, name, value));
        return this;
    }

    @Override
    public ResponseTrailersBuilder set(CharSequence name, CharSequence value) {
        verifyState();
        operations.add(new HeaderOperation(true, name, value));
        return this;
    }

    void applyTo(HttpHeaders target) {
        verifyState();
        for (HeaderOperation operation : operations) {
            CharSequence trailerName = TRAILER_PREFIX.concat(operation.name());
            if (operation.set()) {
                target.set(trailerName, operation.value());
            } else {
                target.add(trailerName, operation.value());
            }
        }
        applied = true;
    }

    void applyTo(ConnectEndStreamMeta.Builder meta) {
        verifyState();
        for (HeaderOperation operation : operations) {
            if (operation.set()) {
                meta.set(operation.name(), operation.value());
            } else {
                meta.add(operation.name(), operation.value());
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
