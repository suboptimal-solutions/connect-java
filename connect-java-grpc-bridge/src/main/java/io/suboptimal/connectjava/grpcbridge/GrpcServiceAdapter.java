package io.suboptimal.connectjava.grpcbridge;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.suboptimal.connectjava.model.ConnectMethodDefinition;
import io.suboptimal.connectjava.model.ConnectMethodType;
import io.suboptimal.connectjava.model.ConnectServiceDefinition;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adapts a gRPC {@link BindableService} into connect-java's service model.
 */
final class GrpcServiceAdapter {

    record AdaptedService(
        ConnectServiceDefinition definition,
        Map<String, ServerMethodDefinition<?, ?>> methods) {}

    static AdaptedService adapt(BindableService service) {
        ServerServiceDefinition ssd = service.bindService();
        String serviceName = ssd.getServiceDescriptor().getName();

        Map<String, ConnectMethodDefinition> connectMethods = new LinkedHashMap<>();
        Map<String, ServerMethodDefinition<?, ?>> grpcMethods = new LinkedHashMap<>();

        for (ServerMethodDefinition<?, ?> smd : ssd.getMethods()) {
            MethodDescriptor<?, ?> md = smd.getMethodDescriptor();
            String methodName = extractMethodName(md.getFullMethodName());
            ConnectMethodType type = mapMethodType(md.getType());
            Class<?> requestType = extractMessageType(md.getRequestMarshaller());
            Class<?> responseType = extractMessageType(md.getResponseMarshaller());

            connectMethods.put(methodName, new ConnectMethodDefinition(
                methodName, type, requestType, responseType, md.isSafe()));
            grpcMethods.put(methodName, smd);
        }

        return new AdaptedService(
            new ConnectServiceDefinition(serviceName, connectMethods, null),
            grpcMethods);
    }

    private static String extractMethodName(String fullMethodName) {
        int slashIndex = fullMethodName.lastIndexOf('/');
        return slashIndex < 0 ? fullMethodName : fullMethodName.substring(slashIndex + 1);
    }

    private static ConnectMethodType mapMethodType(MethodDescriptor.MethodType grpcType) {
        return switch (grpcType) {
            case UNARY -> ConnectMethodType.UNARY;
            case SERVER_STREAMING -> ConnectMethodType.SERVER_STREAMING;
            case CLIENT_STREAMING -> ConnectMethodType.CLIENT_STREAMING;
            case BIDI_STREAMING -> ConnectMethodType.BIDI_STREAMING;
            case UNKNOWN -> throw new IllegalArgumentException("Unknown gRPC method type");
        };
    }

    private static Class<?> extractMessageType(MethodDescriptor.Marshaller<?> marshaller) {
        if (marshaller instanceof MethodDescriptor.ReflectableMarshaller<?> rm) {
            return rm.getMessageClass();
        }
        throw new IllegalArgumentException(
            "Cannot extract message type: marshaller is not a ReflectableMarshaller");
    }

    private GrpcServiceAdapter() {}
}
