# syntax=docker/dockerfile:1

FROM alpine:3.22 AS runner
RUN apk add --no-cache ca-certificates curl
ARG TARGETARCH
ARG CONFORMANCE_VERSION=v1.0.5
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) archive_arch=x86_64; archive_sha256=7d4d1f36980debf900b12514c8d51107245e9195b3c581e61fa165fd95c94284 ;; \
      arm64) archive_arch=aarch64; archive_sha256=4d8025305834da1937d4adb7890465d8ae0e2d9450d2abc651212857e2aeb768 ;; \
      *) echo "Unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    archive=/tmp/connectconformance.tar.gz; \
    curl -fL --retry 3 \
      "https://github.com/connectrpc/conformance/releases/download/${CONFORMANCE_VERSION}/connectconformance-${CONFORMANCE_VERSION}-Linux-${archive_arch}.tar.gz" \
      -o "$archive"; \
    echo "${archive_sha256}  ${archive}" | sha256sum -c -; \
    tar -xzf "$archive" -C /tmp connectconformance LICENSE

FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY connect-java-bom/pom.xml connect-java-bom/pom.xml
COPY connect-java-api/pom.xml connect-java-api/pom.xml
COPY connect-java-codec/pom.xml connect-java-codec/pom.xml
COPY connect-java-compression/pom.xml connect-java-compression/pom.xml
COPY connect-java-core/pom.xml connect-java-core/pom.xml
COPY connect-java-server-spi/pom.xml connect-java-server-spi/pom.xml
COPY connect-java-codec-protobuf/pom.xml connect-java-codec-protobuf/pom.xml
COPY connect-java-server/pom.xml connect-java-server/pom.xml
COPY connect-java-grpc-bridge/pom.xml connect-java-grpc-bridge/pom.xml
COPY connect-java-conformance/pom.xml connect-java-conformance/pom.xml
COPY connect-java-api/src connect-java-api/src
COPY connect-java-codec/src connect-java-codec/src
COPY connect-java-compression/src connect-java-compression/src
COPY connect-java-core/src connect-java-core/src
COPY connect-java-server-spi/src connect-java-server-spi/src
COPY connect-java-codec-protobuf/src connect-java-codec-protobuf/src
COPY connect-java-server/src connect-java-server/src
COPY connect-java-grpc-bridge/src connect-java-grpc-bridge/src
COPY connect-java-conformance/src connect-java-conformance/src
RUN mvn -B -ntp -pl connect-java-conformance -am -DskipTests package

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
COPY --from=runner /tmp/connectconformance /usr/local/bin/connectconformance
COPY --from=runner /tmp/LICENSE /licenses/connectconformance-LICENSE
COPY --from=build /workspace/connect-java-conformance/target/connect-java-conformance-jar-with-dependencies.jar /app/connect-java-conformance.jar
COPY connect-java-conformance/src/main/conformance /app/conformance
COPY --chmod=755 connect-java-conformance/docker/run-server /app/run-server
COPY --chmod=755 connect-java-conformance/docker/run-conformance /app/run-conformance
USER 10001:10001
ENTRYPOINT ["/app/run-conformance"]
