package vk.kvstore.util;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.experimental.UtilityClass;
import vk.kvstore.storage.StorageException;

@UtilityClass
public class FutureUtils {

  public <T> T await(CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (CompletionException exception) {
      Throwable cause = exception.getCause();
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new StorageException("Tarantool request failed", cause);
    }
  }
}
