package io.suboptimal.connectjava.codec.protobuf;

import com.google.protobuf.TypeRegistry;
import io.suboptimal.connectjava.codec.ConnectCodecRegistry;

public final class ConnectProtobufCodecs {
    private static final ConnectCodecRegistry DEFAULTS =
        ConnectCodecRegistry.builder()
            .register(ConnectProtobufCodec.INSTANCE)
            .register(ConnectProtobufJsonCodec.INSTANCE)
            .build();

    private ConnectProtobufCodecs() {}

    public static ConnectCodecRegistry defaults() {
        return DEFAULTS;
    }

    public static ConnectCodecRegistry defaults(TypeRegistry typeRegistry) {
        return ConnectCodecRegistry.builder()
            .register(ConnectProtobufCodec.INSTANCE)
            .register(new ConnectProtobufJsonCodec(typeRegistry))
            .build();
    }
}
