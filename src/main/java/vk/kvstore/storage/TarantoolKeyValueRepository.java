package vk.kvstore.storage;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.TarantoolResponse;
import io.tarantool.mapping.Tuple;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.util.FutureUtils;
import vk.kvstore.util.TupleMapper;

public final class TarantoolKeyValueRepository implements KeyValueRepository {

  private static final String PRIMARY_INDEX = "primary";
  private static final String COUNT_EXPRESSION =
      "local space_name = ...; local target_space = box.space[space_name]; return target_space:len()";

  private final TarantoolBoxClient client;
  private final TarantoolBoxSpace space;
  private final String spaceName;
  private final int rangeBatchSize;

  public TarantoolKeyValueRepository(
      TarantoolBoxClient client, String spaceName, int rangeBatchSize) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.spaceName = requireNonNull(spaceName, "spaceName");
    if (rangeBatchSize <= 0) {
      throw new IllegalArgumentException("rangeBatchSize must be greater than 0");
    }
    this.rangeBatchSize = rangeBatchSize;
    this.space = client.space(spaceName);
  }

  @Override
  public void put(String key, byte[] value) {
    FutureUtils.await(space.replace(Arrays.asList(requireNonNull(key, "key"), value)));
  }

  @Override
  public Optional<KeyValueEntry> get(String key) {
    SelectResponse<List<Tuple<List<?>>>> response =
        FutureUtils.await(space.select(Collections.singletonList(requireNonNull(key, "key"))));
    List<Tuple<List<?>>> tuples = response.get();
    if (tuples.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(TupleMapper.fromTuple(tuples.get(0)));
  }

  @Override
  public boolean delete(String key) {
    Tuple<List<?>> deleted =
        FutureUtils.await(space.delete(Collections.singletonList(requireNonNull(key, "key"))));
    return deleted != null;
  }

  @Override
  public long count() {
    TarantoolResponse<List<?>> response =
        FutureUtils.await(client.eval(COUNT_EXPRESSION, Collections.singletonList(spaceName)));
    List<?> values = response.get();
    if (values.size() != 1 || !(values.get(0) instanceof Number number)) {
      throw new StorageException("Unexpected count response: " + values);
    }
    return number.longValue();
  }

  @Override
  public void range(String keySince, String keyTo, Consumer<KeyValueEntry> consumer) {
    requireNonNull(keySince, "keySince");
    requireNonNull(keyTo, "keyTo");
    Objects.requireNonNull(consumer, "consumer must not be null");

    if (keySince.compareTo(keyTo) >= 0) {
      return;
    }

    Object after = null;
    while (true) {
      SelectOptions.Builder optionsBuilder =
          SelectOptions.builder()
              .withIndex(PRIMARY_INDEX)
              .withIterator(BoxIterator.GE)
              .withLimit(rangeBatchSize)
              .fetchPosition();

      if (after != null) {
        optionsBuilder.after(after);
      }

      SelectResponse<List<Tuple<List<?>>>> response =
          FutureUtils.await(
              space.select(Collections.singletonList(keySince), optionsBuilder.build()));

      List<Tuple<List<?>>> tuples = response.get();
      if (tuples.isEmpty()) {
        return;
      }

      for (Tuple<List<?>> tuple : tuples) {
        KeyValueEntry entry = TupleMapper.fromTuple(tuple);
        if (entry.key().compareTo(keyTo) >= 0) {
          return;
        }
        consumer.accept(entry);
      }

      if (tuples.size() < rangeBatchSize) {
        return;
      }

      after = response.getPosition();
      if (after == null) {
        after = tuples.get(tuples.size() - 1).get();
      }
    }
  }

  private static String requireNonNull(String value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
    return value;
  }
}
