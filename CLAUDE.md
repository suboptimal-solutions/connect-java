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

## Package layout

Dependency direction should flow from protocol implementation toward small SPIs, not the other way around.

- `io.suboptimal.connectjava.api` - Public surface visible to service authors: messages, errors, exchange, headers/trailers builders, attribute keys.
- `io.suboptimal.connectjava.model` - Service and method descriptors used to register endpoints.
- `io.suboptimal.connectjava.codec` - Codec SPI for encoding and decoding application messages, plus built-in protobuf codecs.
- `io.suboptimal.connectjava.compression` - Compression SPI and built-in identity/gzip implementations.
- `io.suboptimal.connectjava.protocol` - Connect protocol implementation, HTTP mapping, framing, metadata, and error handling. Internal handlers here are package-private.

## Key conventions

- Public classes, interfaces, enums, and records are prefixed with `Connect` (e.g. `ConnectCodec`, `ConnectCompressionRegistry`). Package-private internal types do not need the prefix.
- Put `@NullMarked` on library packages via `package-info.java`.
- Mark nullable values explicitly with `@Nullable`.
- Tests use JUnit 5 and AssertJ.
- Do not use Mockito.
- Prefer sealed hierarchies where they make exhaustive pattern matching clear.

