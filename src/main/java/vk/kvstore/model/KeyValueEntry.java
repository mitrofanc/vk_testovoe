package vk.kvstore.model;

import java.util.Arrays;
import java.util.Objects;

public final class KeyValueEntry {

  private final String key;
  private final byte[] value;

  public KeyValueEntry(String key, byte[] value) {
    this.key = Objects.requireNonNull(key, "key must not be null");
    this.value = value;
  }

  public String key() {
    return key;
  }

  public byte[] value() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof KeyValueEntry that)) {
      return false;
    }
    return key.equals(that.key) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return 31 * key.hashCode() + Arrays.hashCode(value);
  }

  @Override
  public String toString() {
    return "KeyValueEntry{"
        + "key='"
        + key
        + '\''
        + ", valueLength="
        + (value == null ? "null" : value.length)
        + '}';
  }
}
