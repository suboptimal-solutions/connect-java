package io.suboptimal.connectjava.protocol;

import io.suboptimal.connectjava.api.ConnectAttributeKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectAttributeKeyTest {
    @Test
    void valueOfCreatesKeyWhenAbsent() {
        String name = "test.key.valueOf-new";
        assertThat(ConnectAttributeKey.exists(name)).isFalse();
        ConnectAttributeKey<?> key = ConnectAttributeKey.valueOf(name);
        assertThat(key.name()).isEqualTo(name);
        assertThat(ConnectAttributeKey.exists(name)).isTrue();
    }

    @Test
    void valueOfReturnsSameInstanceForSameName() {
        ConnectAttributeKey<?> k1 = ConnectAttributeKey.valueOf("test.key.valueOf-singleton");
        ConnectAttributeKey<?> k2 = ConnectAttributeKey.valueOf("test.key.valueOf-singleton");
        assertThat(k1).isSameAs(k2);
    }

    @Test
    void existsReturnsFalseBeforeRegistration() {
        assertThat(ConnectAttributeKey.exists("test.key.exists-before")).isFalse();
    }

    @Test
    void existsReturnsTrueAfterValueOf() {
        ConnectAttributeKey.valueOf("test.key.exists-after-valueOf");
        assertThat(ConnectAttributeKey.exists("test.key.exists-after-valueOf")).isTrue();
    }

    @Test
    void existsReturnsTrueAfterNewInstance() {
        ConnectAttributeKey.newInstance("test.key.exists-after-newinstance");
        assertThat(ConnectAttributeKey.exists("test.key.exists-after-newinstance")).isTrue();
    }

    @Test
    void newInstanceCreatesKeyForNewName() {
        ConnectAttributeKey<?> key = ConnectAttributeKey.newInstance("test.key.new-instance-unique");
        assertThat(key.name()).isEqualTo("test.key.new-instance-unique");
    }

    @Test
    void newInstanceThrowsForAlreadyRegisteredName() {
        String name = "test.key.duplicate";
        ConnectAttributeKey.newInstance(name);
        assertThatThrownBy(() -> ConnectAttributeKey.newInstance(name))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(name);
    }

    @Test
    void newInstanceThrowsForNameAlreadyCreatedByValueOf() {
        String name = "test.key.valueOf-then-newinstance";
        ConnectAttributeKey.valueOf(name);
        assertThatThrownBy(() -> ConnectAttributeKey.newInstance(name))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void valueOfReturnsKeyRegisteredByNewInstance() {
        String name = "test.key.newinstance-then-valueOf";
        ConnectAttributeKey<?> created = ConnectAttributeKey.newInstance(name);
        ConnectAttributeKey<?> looked = ConnectAttributeKey.valueOf(name);
        assertThat(looked).isSameAs(created);
    }

    @Test
    void emptyNameRejectedByValueOf() {
        assertThatThrownBy(() -> ConnectAttributeKey.valueOf(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyNameRejectedByNewInstance() {
        assertThatThrownBy(() -> ConnectAttributeKey.newInstance(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyNameRejectedByExists() {
        assertThatThrownBy(() -> ConnectAttributeKey.exists(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringContainsName() {
        ConnectAttributeKey<?> key = ConnectAttributeKey.valueOf("test.key.tostring");
        assertThat(key.toString()).contains("test.key.tostring");
    }
}
