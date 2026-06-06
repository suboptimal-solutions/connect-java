package io.suboptimal.connectjava.protocol;

import org.jspecify.annotations.Nullable;

record ConnectRoute(String service, String method) {
    static @Nullable ConnectRoute parse(String uri) {
        int queryStart = uri.indexOf('?');
        String path = queryStart >= 0 ? uri.substring(0, queryStart) : uri;
        if (!path.startsWith("/")) {
            return null;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return null;
        }
        String methodName = path.substring(lastSlash + 1);
        String beforeMethod = path.substring(0, lastSlash);
        int serviceSlash = beforeMethod.lastIndexOf('/');
        if (serviceSlash < 0) {
            return null;
        }
        String serviceName = beforeMethod.substring(serviceSlash + 1);
        if (serviceName.isEmpty() || methodName.isEmpty()) {
            return null;
        }
        return new ConnectRoute(serviceName, methodName);
    }
}
