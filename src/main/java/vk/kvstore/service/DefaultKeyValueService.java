package vk.kvstore.service;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.storage.KeyValueRepository;

public final class DefaultKeyValueService implements KeyValueService {

    private final KeyValueRepository repository;

    public DefaultKeyValueService(KeyValueRepository repository) {
      this.repository = Objects.requireNonNull(repository, "repository must not be null");
    }

    @Override
    public void put(String key, byte[] value) {
      repository.put(requireKey(key, "key"), value);
    }

    @Override
    public Optional<KeyValueEntry> get(String key) {
      return repository.get(requireKey(key, "key"));
    }

    @Override
    public boolean delete(String key) {
      return repository.delete(requireKey(key, "key"));
    }

    @Override
    public long count() {
      return repository.count();
    }

    @Override
    public void range(String keySince, String keyTo, Consumer<KeyValueEntry> consumer) {
      requireKey(keySince, "keySince");
      requireKey(keyTo, "keyTo");
      repository.range(keySince, keyTo, consumer);
    }

    private static String requireKey(String value, String fieldName) {
      if (value == null) {
        throw new InvalidRequestException(fieldName + " must not be null");
      }
      return value;
  }

}
