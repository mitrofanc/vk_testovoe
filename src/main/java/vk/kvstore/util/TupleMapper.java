package vk.kvstore.util;

import io.tarantool.mapping.Tuple;
import java.util.List;
import lombok.experimental.UtilityClass;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.storage.StorageException;

@UtilityClass
public class TupleMapper {

  public KeyValueEntry fromTuple(Tuple<List<?>> tuple) {
    if (tuple == null) {
      return null;
    }
    return fromTuple(tuple.get());
  }

  public KeyValueEntry fromTuple(List<?> tuple) {
    if (tuple == null) {
      return null;
    }
    if (tuple.size() != 2) {
      throw new StorageException("Expected tuple with exactly 2 fields, got " + tuple.size());
    }
    Object key = tuple.get(0);
    if (!(key instanceof String stringKey)) {
      throw new StorageException("Expected key to be a string, got " + describe(key));
    }
    Object value = tuple.get(1);
    if (value != null && !(value instanceof byte[])) {
      throw new StorageException("Expected value to be byte[] or null, got " + describe(value));
    }
    return new KeyValueEntry(stringKey, (byte[]) value);
  }

  private String describe(Object value) {
    return value == null ? "null" : value.getClass().getName();
  }
}
