# connect-java conformance

This is a development-only Maven module for running the official Connect server
conformance suite against `connect-java-server` and `connect-java-grpc-bridge`.
The normal Maven build and CI compile this module but do not run the conformance
suite. The Docker commands below run it only when invoked explicitly. The module
is excluded from the Maven Central release.

The image contains two executables: the Java server implemented in this module
and the official `connectconformance` runner. The runner starts the Java server
as a child process, sends its configuration over stdin, and uses its built-in Go
reference client to make RPCs over localhost. The image does not need a Go SDK
at runtime, and no upstream binary is committed to this repository.
The Java server is packaged as an executable JAR with Maven Assembly during
the Docker build.

## Run

From the root of this repository, with Docker running:

```sh
docker build -f connect-java-conformance/Dockerfile -t connect-java-conformance:local .
docker run --rm connect-java-conformance:local
```

These commands work in a POSIX shell and in PowerShell on Windows or macOS with
Docker Desktop. The build selects the Linux amd64 or arm64 runner archive to
match the image architecture. No host port or bind mount is needed: the runner
and server share one container and communicate over localhost.

Additional runner flags can be passed after the image name. For example, to
run sequentially or select a test case:

```sh
docker run --rm connect-java-conformance:local -p 1
docker run --rm connect-java-conformance:local --run 'Basic/**'
```

The container's exit code is the conformance result. Its feature matrix is
defined in `src/main/conformance/connect-java-conformance.yaml`. The empty
`known-failing.txt` is kept under version control so any future exception must
be explicit and reviewable.

## Reproducibility

`Dockerfile` downloads the official `connectconformance` release **v1.0.5**
for Linux amd64 or arm64 and verifies the archive's SHA-256 before unpacking.
The Protobuf definitions in `src/main/protobuf` are byte-for-byte copies from
the same tagged release. When upgrading the runner, update the version,
checksums, and Protobuf definitions together, then rerun the entire suite.
