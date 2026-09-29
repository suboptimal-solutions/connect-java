package io.suboptimal.connectjava.conformance;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.google.protobuf.TypeRegistry;
import connectrpc.conformance.v1.ServerCompat.ServerCompatRequest;
import connectrpc.conformance.v1.ServerCompat.ServerCompatResponse;
import connectrpc.conformance.v1.Service;
import io.grpc.BindableService;
import io.grpc.ServerInterceptors;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.suboptimal.connectjava.codec.ConnectCodecRegistry;
import io.suboptimal.connectjava.codec.protobuf.ConnectProtobufCodecs;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.grpcbridge.ConnectGrpcBridge;
import io.suboptimal.connectjava.protocol.server.ConnectProtocol;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolConfig;
import io.suboptimal.connectjava.protocol.server.ConnectProtocolParameters;
import io.suboptimal.nettymultiprotocol.AppChannelConfigurer;
import io.suboptimal.nettymultiprotocol.AppProtocol;
import io.suboptimal.nettymultiprotocol.AppProtocolRegistry;
import io.suboptimal.nettymultiprotocol.NettyMultiprotocol;
import org.jspecify.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

public final class ConnectConformanceServerMain {
    private static final String HOST = "127.0.0.1";
    private static final int DEFAULT_MAX_BYTES = 4 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        ServerCompatRequest compatRequest = readDelimited(System.in, ServerCompatRequest.parser());
        int maxBytes = compatRequest.getMessageReceiveLimit() > 0
            ? compatRequest.getMessageReceiveLimit()
            : DEFAULT_MAX_BYTES;

        ConformanceService service = new ConformanceService();
        BindableService interceptedService =
            () -> ServerInterceptors.intercept(service, new ConformanceMetadataInterceptor());
        ConnectGrpcBridge bridge = ConnectGrpcBridge.of(interceptedService);
        TypeRegistry typeRegistry = buildTypeRegistry(Service.getDescriptor());
        ConnectCodecRegistry codecRegistry = ConnectProtobufCodecs.defaults(typeRegistry);

        ConnectProtocolConfig connectConfig = ConnectProtocolConfig
            .builder(bridge.serviceDefinitions(), bridge,
                new ConnectProtocolParameters(maxBytes, maxBytes),
                codecRegistry)
            .compressionRegistry(ConnectCompressionRegistry.standard())
            .build();
        ConnectProtocol connectProtocol = new ConnectProtocol(connectConfig);
        AppProtocolRegistry registry = new AppProtocolRegistry();
        registry.register("/", new AppProtocol() {
            @Override
            public @Nullable AppChannelConfigurer http1() {
                return channel -> connectProtocol.http1().configure(channel);
            }

            @Override
            public @Nullable AppChannelConfigurer http2() {
                return channel -> connectProtocol.http2().configure(channel);
            }
        });

        byte[] pemCert = new byte[0];
        @Nullable SslContext sslContext = null;
        if (compatRequest.getUseTls()) {
            pemCert = compatRequest.getServerCreds().getCert().toByteArray();
            sslContext = buildSslContext(compatRequest);
        }

        ChannelInitializer<Channel> initializer = NettyMultiprotocol.builder()
            .sslContext(sslContext)
            .registry(registry)
            .build();
        var group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        Channel serverChannel = null;
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(initializer);

            serverChannel = bootstrap.bind(HOST, 0).sync().channel();
            Channel shutdownChannel = serverChannel;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                shutdownChannel.close().syncUninterruptibly();
                group.shutdownGracefully();
                service.shutdown();
            }, "connect-conformance-shutdown"));

            InetSocketAddress address = (InetSocketAddress) serverChannel.localAddress();
            writeDelimited(System.out, ServerCompatResponse.newBuilder()
                .setHost(HOST)
                .setPort(address.getPort())
                .setPemCert(ByteString.copyFrom(pemCert))
                .build());
            System.out.flush();

            serverChannel.closeFuture().sync();
        } finally {
            if (serverChannel != null) {
                serverChannel.close().syncUninterruptibly();
            }
            group.shutdownGracefully().syncUninterruptibly();
            service.shutdown();
        }
    }

    private static TypeRegistry buildTypeRegistry(Descriptors.FileDescriptor fileDescriptor) {
        TypeRegistry.Builder builder = TypeRegistry.newBuilder();
        addFileTypes(builder, fileDescriptor, new HashSet<>(), new HashSet<>());
        return builder.build();
    }

    private static void addFileTypes(
        TypeRegistry.Builder builder,
        Descriptors.FileDescriptor fileDescriptor,
        Set<String> seenFiles,
        Set<String> seenMessages)
    {
        if (!seenFiles.add(fileDescriptor.getFullName())) {
            return;
        }
        for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
            addFileTypes(builder, dependency, seenFiles, seenMessages);
        }
        for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
            addMessageType(builder, descriptor, seenMessages);
        }
    }

    private static void addMessageType(
        TypeRegistry.Builder builder,
        Descriptors.Descriptor descriptor,
        Set<String> seenMessages)
    {
        if (seenMessages.add(descriptor.getFullName())) {
            builder.add(descriptor);
        }
        for (Descriptors.Descriptor nestedType : descriptor.getNestedTypes()) {
            addMessageType(builder, nestedType, seenMessages);
        }
    }

    private static SslContext buildSslContext(ServerCompatRequest request) throws IOException {
        byte[] certBytes = request.getServerCreds().getCert().toByteArray();
        byte[] keyBytes = normalizePrivateKey(request.getServerCreds().getKey().toByteArray());

        SslContextBuilder builder = SslContextBuilder
            .forServer(new ByteArrayInputStream(certBytes), new ByteArrayInputStream(keyBytes))
            .applicationProtocolConfig(new ApplicationProtocolConfig(
                ApplicationProtocolConfig.Protocol.ALPN,
                ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                ApplicationProtocolNames.HTTP_2,
                ApplicationProtocolNames.HTTP_1_1));

        if (!request.getClientTlsCert().isEmpty()) {
            builder.trustManager(new ByteArrayInputStream(request.getClientTlsCert().toByteArray()));
            builder.clientAuth(ClientAuth.REQUIRE);
        }

        return builder.build();
    }

    private static byte[] normalizePrivateKey(byte[] pemBytes) throws IOException {
        String pem = new String(pemBytes, StandardCharsets.US_ASCII);
        if (!pem.contains("-----BEGIN RSA PRIVATE KEY-----")) {
            return pemBytes;
        }

        String base64 = pem
            .replace("-----BEGIN RSA PRIVATE KEY-----", "")
            .replace("-----END RSA PRIVATE KEY-----", "")
            .replaceAll("\\s", "");
        byte[] pkcs1 = Base64.getDecoder().decode(base64);
        byte[] pkcs8 = pkcs8RsaPrivateKey(pkcs1);
        String encoded = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
            .encodeToString(pkcs8);
        return ("-----BEGIN PRIVATE KEY-----\n" + encoded + "\n-----END PRIVATE KEY-----\n")
            .getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] pkcs8RsaPrivateKey(byte[] pkcs1) throws IOException {
        ByteArrayOutputStream algorithmIdentifier = new ByteArrayOutputStream();
        algorithmIdentifier.write(0x06);
        writeDerLength(algorithmIdentifier, 9);
        algorithmIdentifier.write(new byte[] {
            0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7,
            0x0D, 0x01, 0x01, 0x01});
        algorithmIdentifier.write(0x05);
        writeDerLength(algorithmIdentifier, 0);
        byte[] algorithm = derSequence(algorithmIdentifier.toByteArray());

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0x02);
        writeDerLength(body, 1);
        body.write(0);
        body.write(algorithm);
        body.write(0x04);
        writeDerLength(body, pkcs1.length);
        body.write(pkcs1);
        return derSequence(body.toByteArray());
    }

    private static byte[] derSequence(byte[] body) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0x30);
        writeDerLength(out, body.length);
        out.write(body);
        return out.toByteArray();
    }

    private static void writeDerLength(OutputStream out, int length) throws IOException {
        if (length < 0x80) {
            out.write(length);
            return;
        }
        int bytes = 0;
        int value = length;
        while (value > 0) {
            bytes++;
            value >>= 8;
        }
        out.write(0x80 | bytes);
        for (int shift = (bytes - 1) * 8; shift >= 0; shift -= 8) {
            out.write((length >> shift) & 0xFF);
        }
    }

    private static <T extends MessageLite> T readDelimited(
        InputStream in, Parser<T> parser) throws IOException
    {
        byte[] lengthBytes = in.readNBytes(4);
        if (lengthBytes.length != 4) {
            throw new EOFException("Expected 4-byte message length prefix");
        }
        int length = ByteBuffer.wrap(lengthBytes).getInt();
        if (length < 0) {
            throw new IOException("Negative message length: " + length);
        }
        byte[] messageBytes = in.readNBytes(length);
        if (messageBytes.length != length) {
            throw new EOFException(
                "Expected " + length + " message bytes, got " + messageBytes.length);
        }
        return parser.parseFrom(messageBytes);
    }

    private static void writeDelimited(OutputStream out, MessageLite message) throws IOException {
        byte[] messageBytes = message.toByteArray();
        out.write(ByteBuffer.allocate(4).putInt(messageBytes.length).array());
        out.write(messageBytes);
    }

    private ConnectConformanceServerMain() {}
}
