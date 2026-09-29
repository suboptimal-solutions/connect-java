package io.suboptimal.connectjava.conformance;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Exposes request headers to service methods and lets them contribute response headers/trailers,
 * using nothing but standard gRPC: a {@link Context}-attached {@link Metadata} per direction, and a
 * {@link ForwardingServerCall} that merges the response ones in when the call actually sends them.
 *
 * <p>The wrapper sends the headers itself when a call is closed without having sent any, because
 * {@link io.grpc.stub.ServerCalls} only sends them lazily, from {@code onNext}
 * (ServerCalls.java:376-378). An RPC that produces no messages - an immediate {@code onError}, or an
 * {@code onCompleted} over an empty response stream - goes straight to {@code call.close} (:390,
 * :396), so a wrapper contributing only through {@code sendHeaders} never runs at all and its
 * headers are lost.
 *
 * <p>That is a property of grpc-stub rather than of the transport under it. On a real gRPC server it
 * stays invisible: HTTP/2 lets such a response degenerate into a Trailers-Only frame that has no
 * separate headers section to miss. Connect has no Trailers-Only form - every response carries
 * headers - so they have to be sent explicitly. The bridge cannot do this on our behalf: it hands
 * its own {@code ServerCall} to {@code startCall} and never sees the wrapper an interceptor puts
 * around it, so nothing it calls on itself can reach this override.
 */
final class ConformanceMetadataInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next)
    {
        Metadata responseHeaders = new Metadata();
        Metadata responseTrailers = new Metadata();

        ServerCall<ReqT, RespT> wrapped = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            /** Plain field, like ServerCalls' own {@code sentHeaders}: gRPC serializes these calls. */
            private boolean headersSent;

            @Override
            public void sendHeaders(Metadata toSend) {
                headersSent = true;
                toSend.merge(responseHeaders);
                super.sendHeaders(toSend);
            }

            @Override
            public void close(Status status, Metadata trailers) {
                if (!headersSent) {
                    // this.sendHeaders, not super's: the override above is what merges the headers,
                    // and only a call from inside the wrapper reaches it.
                    sendHeaders(new Metadata());
                }
                trailers.merge(responseTrailers);
                super.close(status, trailers);
            }
        };

        Context context = Context.current()
            .withValue(ConformanceRpcContext.REQUEST_HEADERS, headers)
            .withValue(ConformanceRpcContext.RESPONSE_HEADERS, responseHeaders)
            .withValue(ConformanceRpcContext.RESPONSE_TRAILERS, responseTrailers);
        return Contexts.interceptCall(context, wrapped, headers, next);
    }
}
