# connect-java gRPC bridge

`connect-java-grpc-bridge` adapts existing gRPC-Java `BindableService`
implementations to the Connect server pipeline. A generated `*ImplBase` service
can serve Connect requests without Connect-specific code. The bridge translates
messages, metadata, status and error details, cancellation, and flow control to
gRPC-Java's `ServerCall` and `ServerCall.Listener` contracts.

The bridge is an adapter, not an HTTP server. Your application still installs
`ConnectProtocol` into its Netty HTTP/1.1 or HTTP/2 stream pipeline.

## Dependencies

Import the BOM and add the server, bridge, and Protobuf codecs:

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>io.github.suboptimal-solutions</groupId>
      <artifactId>connect-java-bom</artifactId>
      <version>${connect-java.version}</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>

<dependencies>
  <dependency>
    <groupId>io.github.suboptimal-solutions</groupId>
    <artifactId>connect-java-server</artifactId>
  </dependency>
  <dependency>
    <groupId>io.github.suboptimal-solutions</groupId>
    <artifactId>connect-java-codec-protobuf</artifactId>
  </dependency>
  <dependency>
    <groupId>io.github.suboptimal-solutions</groupId>
    <artifactId>connect-java-grpc-bridge</artifactId>
  </dependency>
</dependencies>
```

The bridge itself depends on `connect-java-api` and `connect-java-server-spi`,
not the server implementation. The application adds `connect-java-server`
because it creates and installs `ConnectProtocol`.

## Configure the server

Create one bridge for the services mounted on a `ConnectProtocol`. The bridge
derives Connect service and method definitions from their gRPC descriptors:

```java
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.grpcbridge.ConnectGrpcBridge;
import io.suboptimal.connectjava.protocol.server.ConnectCorsParameters;
import io.suboptimal.connectjava.protocol.server.ConnectProtocol;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolConfig;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolParameters;

ConnectGrpcBridge bridge = ConnectGrpcBridge.builder()
    .addService(new GreeterService())
    .addService(new HealthService())
    .build();

ConnectProtocolConfig config = ConnectProtocolConfig.builder(
    bridge.serviceDefinitions(),
    bridge,
    new ConnectProtocolParameters(
        4 * 1024 * 1024,  // maximum request size
        1 * 1024 * 1024,  // maximum frame size
        ConnectCorsParameters.disabled()),
    ConnectProtobufCodecs.defaults())
    .build();

ConnectProtocol protocol = new ConnectProtocol(config);
// After HttpServerCodec in an HTTP/1.1 channel initializer:
protocol.http1().configure(channel);
// Or on each HTTP/2 stream channel:
protocol.http2().configure(streamChannel);
```

Use `ConnectGrpcBridge.of(service)` for a single service. Services must expose
gRPC-Java method descriptors with reflectable request and response marshallers;
the bridge uses them to discover message classes. Unsupported custom
marshallers cause bridge construction to fail.

Standard gRPC `ServerInterceptor`s also work. Apply them to the service before
adding it to the bridge:

```java
import io.grpc.BindableService;
import io.grpc.ServerInterceptors;

BindableService intercepted = () ->
    ServerInterceptors.intercept(new GreeterService(), auditInterceptor);
ConnectGrpcBridge bridge = ConnectGrpcBridge.of(intercepted);
```

## Choose how service code runs

By default, service and interceptor callbacks run on virtual threads. Work for
each call is serialized, but different calls can run concurrently. Blocking
service code does not stall the Netty event loop. The default is asynchronous:
a test using `EmbeddedChannel` must wait for executor work before reading a
response.

Use a shared application executor when you need to control where callbacks
run. The application owns that executor and its shutdown:

```java
ConnectGrpcBridge bridge = ConnectGrpcBridge.builder()
    .addService(service)
    .withServiceExecutor(serviceExecutor)
    .build();
```

Keep capacity available while calls are in flight. Cancellation is dispatched
through this executor so it can bypass a call whose service callback is
blocked; a saturated bounded pool can delay that cancellation.

For strictly non-blocking services, `withDirectExecutor()` runs callbacks on
the Netty event loop. Blocking a callback in this mode stalls other calls on
that loop. Direct mode also makes `EmbeddedChannel` calls complete synchronously:

```java
ConnectGrpcBridge bridge = ConnectGrpcBridge.builder()
    .addService(service)
    .withDirectExecutor()
    .build();
```

## Behaviour and limitations

- Unary, client-streaming, server-streaming, and bidirectional methods are
  supported wherever the Connect server supports their HTTP transport.
- Request headers become gRPC `Metadata`; response headers and trailers are
  copied back to Connect. Binary `-bin` metadata is base64-encoded on the wire.
  Reserved protocol headers are not forwarded as application response metadata.
- gRPC status codes map to Connect error codes. `grpc-status-details-bin` is
  represented as Connect error details.
- `ServerCall.request(n)` controls inbound message delivery. The bridge changes
  the channel's `autoRead` setting for backpressure during a call, so other
  handlers on that channel should not independently manage `autoRead` at the
  same time. `isReady()` reflects Netty writability; `onReady()` is advisory,
  and a callback should check `isReady()` again.
- The gRPC call context starts from `Context.ROOT`; it does not inherit values
  attached to the thread that configured the pipeline. A Connect timeout is
  not exposed as a gRPC `Context` deadline. During cancellation, `onCancel`
  may run with an already-cancelled context, so cleanup work should not rely
  on that context remaining active. The bridge does not implement
  per-call `ServerCall.setCompression` or `setMessageCompression`; Connect's
  codec and compression registries control wire encoding. `getAuthority()`
  retains gRPC-Java's default `null`; the inbound `host` header is available
  through request metadata.

One interceptor caveat matters for calls with no response messages. Generated
gRPC stubs call `sendHeaders` when they send the first message, so an interceptor
that adds response headers only in its `sendHeaders` override will not add them
to an immediate error or an empty response stream. If those headers are needed,
the interceptor must send them from its `close` path when none have been sent.
Response trailers are still processed on every close path.

For an interceptor that adds headers in `sendHeaders`, the relevant part of its
`ServerCall` wrapper can flush those headers before closing:

```java
@Override
public void close(Status status, Metadata trailers) {
    if (!headersSent) {
        sendHeaders(new Metadata()); // invokes this wrapper's sendHeaders override
    }
    super.close(status, trailers);
}
```

Source-level research on the gRPC-Java contracts and the bridge's design is
kept in the companion `connect-java-specs` project under
`grpc-java/grpc-specifics.md`. Its source citations were established against
gRPC-Java 1.76.0 and should be rechecked for version-sensitive details.
