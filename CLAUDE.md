# connect-java

## Purpose

`connect-java` is a standalone Connect RPC protocol implementation for Netty 4.2.x. It is intended to provide Connect protocol handling without requiring a larger web framework.

## Build commands

- `mvn compile`
- `mvn test`
- `mvn test -Dtest=ClassName`
- `mvn package`

## Tech stack

- Java 21
- Netty 4.2.x
- JSpecify for nullness annotations
- JUnit 5 and AssertJ for tests

## Architecture overview

The library is organized around Connect protocol layers:

1. Routing identifies the RPC endpoint and selects the terminal service handler.
2. Codec handlers translate between wire payloads and application messages.
3. Compression handlers apply or remove negotiated compression.
4. Connect protocol handlers enforce request, response, metadata, trailer, timeout, and error semantics.
5. The terminal handler invokes user service logic and writes protocol-level responses.

Expected message flow:

```text
Netty HTTP request
  -> routing
  -> Connect protocol validation
  -> compression
  -> codec
  -> terminal handler
  -> codec
  -> compression
  -> Connect response framing
  -> Netty HTTP response
```

## Module and package layout

Dependency direction should flow from protocol implementation toward small SPIs, not the other way around.

- `connect-java-api` - Netty-free public API in `io.suboptimal.connectjava.api` and service descriptors in `io.suboptimal.connectjava.model`.
- `connect-java-codec` - Netty-buffer-based codec SPI and registry in `io.suboptimal.connectjava.codec`.
- `connect-java-compression` - Netty-buffer-based compression SPI, registry, identity, and gzip in `io.suboptimal.connectjava.compression`.
- `connect-java-core` - Shared protocol utilities in `io.suboptimal.connectjava.protocol`.
- `connect-java-server-spi` - Netty call-handler factory in `io.suboptimal.connectjava.protocol.server.spi` for server adapters.
- `connect-java-codec-protobuf` - Protobuf implementations in `io.suboptimal.connectjava.codec.protobuf`.
- `connect-java-server` - Netty server implementation in `io.suboptimal.connectjava.protocol.server`. Internal handlers here are package-private.
- `connect-java-grpc-bridge` - Adapter from gRPC-Java services to the Connect server handler SPI in `io.suboptimal.connectjava.grpcbridge`.
- `connect-java-bom` - Public dependency management for the module family and compatible runtime dependencies.

Module dependencies flow `grpc-bridge -> server-spi`, `codec-protobuf -> codec`, `core -> api`, and `server -> api + core + codec + compression + server-spi`. The API module must not depend on Netty.

## Key conventions

- Public classes, interfaces, enums, and records are prefixed with `Connect` (e.g. `ConnectCodec`, `ConnectCompressionRegistry`). Package-private internal types do not need the prefix.
- Types that must be public only to cross Maven module boundaries are annotated with `@ApiStatus.Internal` and are not stable API.
- Put `@NullMarked` on library packages via `package-info.java`.
- Mark nullable values explicitly with `@Nullable`.
- Tests use JUnit 5 and AssertJ.
- Do not use Mockito.
- Prefer sealed hierarchies where they make exhaustive pattern matching clear.

## Maven POM conventions

- List JSpecify first in each module's dependencies, followed by JetBrains annotations when used.
- List dependencies on other connect-java modules next, then other production dependencies.
- Keep all test-scoped dependencies at the end of each dependency list.
- Use the `junit-jupiter` aggregate artifact for tests. Manage JUnit Jupiter, AssertJ, and SLF4J versions in the parent POM; child modules omit those versions.
- Use `connect-java: Component` names and concise descriptions that state each module's role.
