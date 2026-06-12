package io.suboptimal.connectjava.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.suboptimal.connectjava.compression.ConnectCompression;
import io.suboptimal.connectjava.compression.ConnectCompressionRegistry;
import io.suboptimal.connectjava.compression.ConnectIdentityCompression;
import org.jetbrains.annotations.ApiStatus;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

@ApiStatus.Internal
public final class ConnectCompressionNegotiation {

    private ConnectCompressionNegotiation() {
    }

    public static @Nullable String compressionNameFor(@Nullable CharSequence encoding) {
        if (encoding == null) {
            return null;
        }
        String name = encoding.toString().trim().toLowerCase(Locale.ROOT);
        return name.isEmpty() ? null : name;
    }

    public static String formatSupportedEncodings(ConnectCompressionRegistry registry) {
        return String.join(",", registry.supportedNames());
    }

    public static ConnectCompression selectResponseEncoding(
        ConnectCompression requestEncoding, @Nullable String responseEncoding, ConnectCompressionRegistry registry)
    {
        if (!requestEncoding.isIdentity()) {
            return requestEncoding;
        }

        if (responseEncoding == null) {
            return ConnectIdentityCompression.INSTANCE;
        }

        List<Coding> codings = parseContentCodingList(responseEncoding);
        if (codings.isEmpty()) {
            return ConnectIdentityCompression.INSTANCE;
        }

        Coding wildcard = find(codings, "*");
        for (ConnectCompression compression : registry.preferred()) {
            double q = qFor(codings, compression.name(), wildcard != null ? wildcard.q() : 0.0d);
            if (q > 0.0d) {
                return compression;
            }
        }

        return ConnectIdentityCompression.INSTANCE;
    }

    static List<Coding> parseContentCodingList(CharSequence header) {
        List<Coding> codings = new ArrayList<>();
        for (String raw : header.toString().split(",")) {
            String part = raw.trim();
            if (part.isEmpty()) {
                continue;
            }
            String[] parameters = part.split(";");
            String name = parameters[0].trim();
            if (name.isEmpty()) {
                continue;
            }
            name = name.toLowerCase(Locale.ROOT);
            double q = 1.0d;
            for (int i = 1; i < parameters.length; i++) {
                String parameter = parameters[i].trim();
                int eq = parameter.indexOf('=');
                if (eq >= 0 && "q".equalsIgnoreCase(parameter.substring(0, eq).trim())) {
                    try {
                        q = Double.parseDouble(parameter.substring(eq + 1).trim());
                    } catch (NumberFormatException e) {
                        q = 0.0d;
                    }
                }
            }
            codings.add(new Coding(name, Math.max(0.0d, Math.min(1.0d, q))));
        }
        return List.copyOf(codings);
    }

    private static double qFor(List<Coding> codings, String name, double fallback) {
        Coding coding = find(codings, name);
        return coding != null ? coding.q() : fallback;
    }

    private static @Nullable Coding find(List<Coding> codings, String name) {
        for (Coding coding : codings) {
            if (coding.name().equalsIgnoreCase(name)) {
                return coding;
            }
        }
        return null;
    }

    public static ByteBuf decompressMessage(ByteBufAllocator alloc, ByteBuf body, ConnectCompression compression)
        throws IOException
    {
        if (body.readableBytes() == 0) {
            return body.retainedDuplicate();
        }
        ByteBuf compressed = body.retainedDuplicate();
        try {
            return compression.decompress(compressed, alloc);
        } finally {
            compressed.release();
        }
    }

    record Coding(String name, double q) {
    }
}
