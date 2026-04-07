package vk.kvstore.service;

import java.util.Optional;
import java.util.function.Consumer;
import vk.kvstore.model.KeyValueEntry;

public interface KeyValueService {

  void put(String key, byte[] value);

  Optional<KeyValueEntry> get(String key);

  boolean delete(String key);

  long count();

  void range(String keySince, String keyTo, Consumer<KeyValueEntry> consumer);
}
