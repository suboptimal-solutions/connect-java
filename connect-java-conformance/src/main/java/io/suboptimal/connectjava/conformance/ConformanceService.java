package io.suboptimal.connectjava.conformance;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import connectrpc.conformance.v1.ConformanceServiceGrpc;
import connectrpc.conformance.v1.Service.BidiStreamRequest;
import connectrpc.conformance.v1.Service.BidiStreamResponse;
import connectrpc.conformance.v1.Service.ClientStreamRequest;
import connectrpc.conformance.v1.Service.ClientStreamResponse;
import connectrpc.conformance.v1.Service.ConformancePayload;
import connectrpc.conformance.v1.Service.Error;
import connectrpc.conformance.v1.Service.Header;
import connectrpc.conformance.v1.Service.IdempotentUnaryRequest;
import connectrpc.conformance.v1.Service.IdempotentUnaryResponse;
import connectrpc.conformance.v1.Service.ServerStreamRequest;
import connectrpc.conformance.v1.Service.ServerStreamResponse;
import connectrpc.conformance.v1.Service.StreamResponseDefinition;
import connectrpc.conformance.v1.Service.UnaryRequest;
import connectrpc.conformance.v1.Service.UnaryResponse;
import connectrpc.conformance.v1.Service.UnaryResponseDefinition;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class ConformanceService extends ConformanceServiceGrpc.ConformanceServiceImplBase {
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "connect-conformance-delay");
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public void unary(UnaryRequest request, StreamObserver<UnaryResponse> observer) {
        ConformancePayload.RequestInfo requestInfo = buildRequestInfo(List.of(Any.pack(request)));

        if (!request.hasResponseDefinition()) {
            observer.onNext(UnaryResponse.newBuilder()
                .setPayload(ConformancePayload.newBuilder().setRequestInfo(requestInfo))
                .build());
            observer.onCompleted();
            return;
        }

        UnaryResponseDefinition definition = request.getResponseDefinition();
        applyUnaryMetadata(definition);
        scheduleOrRun(() -> {
            if (definition.hasError()) {
                observer.onError(toStatusException(definition.getError(), requestInfo));
                return;
            }
            ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                .setRequestInfo(requestInfo);
            if (definition.hasResponseData()) {
                payload.setData(definition.getResponseData());
            }
            observer.onNext(UnaryResponse.newBuilder().setPayload(payload).build());
            observer.onCompleted();
        }, definition.getResponseDelayMs());
    }

    @Override
    public void idempotentUnary(
        IdempotentUnaryRequest request, StreamObserver<IdempotentUnaryResponse> observer)
    {
        ConformancePayload.RequestInfo requestInfo = buildRequestInfo(List.of(Any.pack(request)));

        if (!request.hasResponseDefinition()) {
            observer.onNext(IdempotentUnaryResponse.newBuilder()
                .setPayload(ConformancePayload.newBuilder().setRequestInfo(requestInfo))
                .build());
            observer.onCompleted();
            return;
        }

        UnaryResponseDefinition definition = request.getResponseDefinition();
        applyUnaryMetadata(definition);
        scheduleOrRun(() -> {
            if (definition.hasError()) {
                observer.onError(toStatusException(definition.getError(), requestInfo));
                return;
            }
            ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                .setRequestInfo(requestInfo);
            if (definition.hasResponseData()) {
                payload.setData(definition.getResponseData());
            }
            observer.onNext(IdempotentUnaryResponse.newBuilder().setPayload(payload).build());
            observer.onCompleted();
        }, definition.getResponseDelayMs());
    }

    @Override
    public void serverStream(
        ServerStreamRequest request, StreamObserver<ServerStreamResponse> observer)
    {
        ConformancePayload.RequestInfo requestInfo = buildRequestInfo(List.of(Any.pack(request)));

        if (!request.hasResponseDefinition()) {
            observer.onCompleted();
            return;
        }

        StreamResponseDefinition definition = request.getResponseDefinition();
        applyStreamMetadata(definition);
        List<ByteString> data = definition.getResponseDataList();
        if (data.isEmpty()) {
            if (definition.hasError()) {
                observer.onError(toStatusException(definition.getError(), requestInfo));
            } else {
                observer.onCompleted();
            }
            return;
        }

        long delay = 0;
        for (int i = 0; i < data.size(); i++) {
            delay += definition.getResponseDelayMs();
            int index = i;
            scheduleOrRun(() -> {
                ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                    .setData(data.get(index));
                if (index == 0) {
                    payload.setRequestInfo(requestInfo);
                }
                observer.onNext(ServerStreamResponse.newBuilder().setPayload(payload).build());
            }, delay);
        }

        scheduleOrRun(() -> {
            if (definition.hasError()) {
                observer.onError(toStatusException(definition.getError()));
            } else {
                observer.onCompleted();
            }
        }, delay);
    }

    @Override
    public StreamObserver<ClientStreamRequest> clientStream(
        StreamObserver<ClientStreamResponse> responseObserver)
    {
        return new StreamObserver<>() {
            private final List<ClientStreamRequest> requests = new ArrayList<>();
            private @Nullable UnaryResponseDefinition responseDefinition;

            @Override
            public void onNext(ClientStreamRequest request) {
                requests.add(request);
                if (requests.size() == 1 && request.hasResponseDefinition()) {
                    responseDefinition = request.getResponseDefinition();
                    applyUnaryMetadata(responseDefinition);
                }
            }

            @Override
            public void onError(Throwable t) {}

            @Override
            public void onCompleted() {
                List<Any> packed = packAll(requests);
                ConformancePayload.RequestInfo requestInfo = buildRequestInfo(packed);
                UnaryResponseDefinition definition = responseDefinition;
                if (definition == null) {
                    responseObserver.onNext(ClientStreamResponse.newBuilder()
                        .setPayload(ConformancePayload.newBuilder().setRequestInfo(requestInfo))
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                scheduleOrRun(() -> {
                    if (definition.hasError()) {
                        responseObserver.onError(toStatusException(definition.getError(), requestInfo));
                        return;
                    }
                    ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                        .setRequestInfo(requestInfo);
                    if (definition.hasResponseData()) {
                        payload.setData(definition.getResponseData());
                    }
                    responseObserver.onNext(ClientStreamResponse.newBuilder().setPayload(payload).build());
                    responseObserver.onCompleted();
                }, definition.getResponseDelayMs());
            }
        };
    }

    @Override
    public StreamObserver<BidiStreamRequest> bidiStream(
        StreamObserver<BidiStreamResponse> responseObserver)
    {
        return new StreamObserver<>() {
            private final List<BidiStreamRequest> allRequests = new ArrayList<>();
            private final List<BidiStreamRequest> requestWindow = new ArrayList<>();
            private @Nullable StreamResponseDefinition responseDefinition;
            private boolean fullDuplex;
            private boolean firstRequestSeen;
            private boolean firstResponseSent;
            private boolean terminated;
            private int responseIndex;

            @Override
            public void onNext(BidiStreamRequest request) {
                if (terminated) {
                    return;
                }
                allRequests.add(request);
                requestWindow.add(request);
                if (!firstRequestSeen) {
                    firstRequestSeen = true;
                    fullDuplex = request.getFullDuplex();
                    if (request.hasResponseDefinition()) {
                        responseDefinition = request.getResponseDefinition();
                        applyStreamMetadata(responseDefinition);
                    }
                }
                if (fullDuplex) {
                    sendFullDuplexResponseForRequest();
                }
            }

            @Override
            public void onError(Throwable t) {
                terminated = true;
            }

            @Override
            public void onCompleted() {
                if (terminated) {
                    return;
                }
                if (fullDuplex) {
                    sendRemainingFullDuplexResponses();
                } else {
                    sendHalfDuplexResponses();
                }
            }

            private void sendFullDuplexResponseForRequest() {
                StreamResponseDefinition definition = responseDefinition;
                if (definition == null) {
                    return;
                }
                if (responseIndex >= definition.getResponseDataCount()) {
                    if (definition.hasError()) {
                        List<Any> pendingRequests = packAndClearWindow();
                        ConformancePayload.@Nullable RequestInfo requestInfo = firstResponseSent
                            ? null
                            : buildRequestInfo(pendingRequests);
                        terminated = true;
                        scheduleOrRun(() ->
                            responseObserver.onError(toStatusException(definition.getError(), requestInfo)),
                            definition.getResponseDelayMs());
                    }
                    return;
                }

                ByteString responseData = definition.getResponseData(responseIndex);
                ConformancePayload.RequestInfo requestInfo = firstResponseSent
                    ? buildRequestsOnlyInfo(packAndClearWindow())
                    : buildRequestInfo(packAndClearWindow());
                firstResponseSent = true;
                responseIndex++;
                scheduleOrRun(() -> responseObserver.onNext(BidiStreamResponse.newBuilder()
                    .setPayload(ConformancePayload.newBuilder()
                        .setData(responseData)
                        .setRequestInfo(requestInfo))
                    .build()), definition.getResponseDelayMs());
            }

            private void sendRemainingFullDuplexResponses() {
                StreamResponseDefinition definition = responseDefinition;
                if (definition == null) {
                    responseObserver.onCompleted();
                    return;
                }
                List<Any> pendingRequests = packAndClearWindow();
                if (responseIndex >= definition.getResponseDataCount()) {
                    completeOrError(definition, pendingRequests);
                    return;
                }

                long delay = 0;
                boolean includeRequestInfo = !firstResponseSent;
                for (int i = responseIndex; i < definition.getResponseDataCount(); i++) {
                    delay += definition.getResponseDelayMs();
                    ByteString responseData = definition.getResponseData(i);
                    ConformancePayload.@Nullable RequestInfo requestInfo = includeRequestInfo
                        ? buildRequestInfo(pendingRequests)
                        : null;
                    includeRequestInfo = false;
                    firstResponseSent = true;
                    scheduleOrRun(() -> {
                        ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                            .setData(responseData);
                        if (requestInfo != null) {
                            payload.setRequestInfo(requestInfo);
                        }
                        responseObserver.onNext(BidiStreamResponse.newBuilder()
                            .setPayload(payload)
                            .build());
                    }, delay);
                }
                responseIndex = definition.getResponseDataCount();
                scheduleOrRun(() -> {
                    if (definition.hasError()) {
                        responseObserver.onError(toStatusException(definition.getError()));
                    } else {
                        responseObserver.onCompleted();
                    }
                }, delay);
            }

            private void sendHalfDuplexResponses() {
                StreamResponseDefinition definition = responseDefinition;
                if (definition == null) {
                    responseObserver.onCompleted();
                    return;
                }
                ConformancePayload.RequestInfo requestInfo =
                    buildRequestInfo(packAll(allRequests));
                if (definition.getResponseDataCount() == 0) {
                    if (definition.hasError()) {
                        responseObserver.onError(toStatusException(definition.getError(), requestInfo));
                    } else {
                        responseObserver.onCompleted();
                    }
                    return;
                }

                long delay = 0;
                for (int i = 0; i < definition.getResponseDataCount(); i++) {
                    delay += definition.getResponseDelayMs();
                    int index = i;
                    scheduleOrRun(() -> {
                        ConformancePayload.Builder payload = ConformancePayload.newBuilder()
                            .setData(definition.getResponseData(index));
                        if (index == 0) {
                            payload.setRequestInfo(requestInfo);
                        }
                        responseObserver.onNext(BidiStreamResponse.newBuilder()
                            .setPayload(payload)
                            .build());
                    }, delay);
                }
                scheduleOrRun(() -> {
                    if (definition.hasError()) {
                        responseObserver.onError(toStatusException(definition.getError()));
                    } else {
                        responseObserver.onCompleted();
                    }
                }, delay);
            }

            private void completeOrError(StreamResponseDefinition definition, List<Any> pendingRequests) {
                if (definition.hasError()) {
                    ConformancePayload.@Nullable RequestInfo requestInfo = firstResponseSent
                        ? null
                        : buildRequestInfo(pendingRequests);
                    responseObserver.onError(toStatusException(definition.getError(), requestInfo));
                } else {
                    responseObserver.onCompleted();
                }
            }

            private List<Any> packAndClearWindow() {
                List<Any> packed = packAll(requestWindow);
                requestWindow.clear();
                return packed;
            }
        };
    }

    void shutdown() {
        scheduler.shutdownNow();
    }

    private static void applyUnaryMetadata(UnaryResponseDefinition definition) {
        addHeaders(definition.getResponseHeadersList(), definition.getResponseTrailersList());
    }

    private static void applyStreamMetadata(StreamResponseDefinition definition) {
        addHeaders(definition.getResponseHeadersList(), definition.getResponseTrailersList());
    }

    private static void addHeaders(List<Header> headers, List<Header> trailers) {
        Metadata responseHeaders = ConformanceRpcContext.responseHeaders();
        for (Header header : headers) {
            for (String value : header.getValueList()) {
                ConformanceRpcContext.put(responseHeaders, header.getName(), value);
            }
        }
        Metadata responseTrailers = ConformanceRpcContext.responseTrailers();
        for (Header trailer : trailers) {
            for (String value : trailer.getValueList()) {
                ConformanceRpcContext.put(responseTrailers, trailer.getName(), value);
            }
        }
    }

    private static ConformancePayload.RequestInfo buildRequestInfo(List<Any> requests) {
        ConformancePayload.RequestInfo.Builder builder = buildRequestsOnlyInfo(requests).toBuilder();
        Metadata requestHeaders = ConformanceRpcContext.requestHeaders();
        for (String name : requestHeaders.keys()) {
            builder.addRequestHeaders(Header.newBuilder()
                .setName(name)
                .addAllValue(ConformanceRpcContext.getAll(requestHeaders, name)));
        }

        String timeout = requestHeaders.get(Metadata.Key.of("connect-timeout-ms", Metadata.ASCII_STRING_MARSHALLER));
        if (timeout != null) {
            try {
                builder.setTimeoutMs(Long.parseLong(timeout));
            } catch (NumberFormatException ignored) {
                // Conformance reports observed metadata; malformed timeout values are left unset.
            }
        }
        return builder.build();
    }

    private static ConformancePayload.RequestInfo buildRequestsOnlyInfo(List<Any> requests) {
        return ConformancePayload.RequestInfo.newBuilder()
            .addAllRequests(requests)
            .build();
    }

    private static List<Any> packAll(List<? extends Message> messages) {
        List<Any> packed = new ArrayList<>(messages.size());
        for (Message message : messages) {
            packed.add(Any.pack(message));
        }
        return packed;
    }

    private static StatusRuntimeException toStatusException(Error error) {
        return toStatusException(error, null);
    }

    private static StatusRuntimeException toStatusException(
        Error error, ConformancePayload.@Nullable RequestInfo requestInfo)
    {
        Status.Code grpcCode = ConformanceCodeMapping.toGrpcStatusCode(error.getCode());
        com.google.rpc.Status.Builder rpcStatus = com.google.rpc.Status.newBuilder()
            .setCode(grpcCode.value())
            .setMessage(error.hasMessage() ? error.getMessage() : "");
        for (Any detail : error.getDetailsList()) {
            rpcStatus.addDetails(detail);
        }
        if (requestInfo != null) {
            rpcStatus.addDetails(Any.pack(requestInfo));
        }
        return StatusProto.toStatusRuntimeException(rpcStatus.build());
    }

    private void scheduleOrRun(Runnable action, long delayMs) {
        if (delayMs <= 0) {
            action.run();
            return;
        }
        scheduler.schedule(action, delayMs, TimeUnit.MILLISECONDS);
    }
}
