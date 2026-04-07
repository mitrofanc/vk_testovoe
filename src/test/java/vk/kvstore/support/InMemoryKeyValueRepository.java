package vk.kvstore.support;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.storage.KeyValueRepository;

public final class InMemoryKeyValueRepository implements KeyValueRepository {

  private final NavigableMap<String, byte[]> entries = new TreeMap<>();

  @Override
  public synchronized void put(String key, byte[] value) {
    entries.put(key, copy(value));
  }

  @Override
  public synchronized Optional<KeyValueEntry> get(String key) {
    if (!entries.containsKey(key)) {
      return Optional.empty();
    }
    return Optional.of(new KeyValueEntry(key, copy(entries.get(key))));
  }

  @Override
  public synchronized boolean delete(String key) {
    boolean existed = entries.containsKey(key);
    if (existed) {
      entries.remove(key);
    }
    return existed;
  }

  @Override
  public synchronized long count() {
    return entries.size();
  }

  @Override
  public synchronized void range(String keySince, String keyTo, Consumer<KeyValueEntry> consumer) {
    if (keySince.compareTo(keyTo) >= 0) {
      return;
    }
    entries.subMap(keySince, true, keyTo, false)
        .forEach((key, value) -> consumer.accept(new KeyValueEntry(key, copy(value))));
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : value.clone();
  }
}
