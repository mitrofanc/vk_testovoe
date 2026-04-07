package vk.kvstore.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.tarantool.core.connection.exceptions.ConnectionException;
import io.tarantool.core.exceptions.ClientException;
import io.tarantool.core.exceptions.ServerException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import lombok.experimental.UtilityClass;
import vk.kvstore.service.InvalidRequestException;
import vk.kvstore.storage.StorageException;

@UtilityClass
public class GrpcExceptionMapper {

  public StatusRuntimeException toStatusRuntimeException(Throwable throwable) {
    Throwable cause = unwrap(throwable);
    if (cause instanceof StatusRuntimeException statusRuntimeException) {
      return statusRuntimeException;
    }
    if (cause instanceof InvalidRequestException || cause instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT
          .withDescription(cause.getMessage())
          .withCause(cause)
          .asRuntimeException();
    }

    Throwable storageCause =
        cause instanceof StorageException && cause.getCause() != null
            ? unwrap(cause.getCause())
            : cause;

    if (storageCause instanceof ConnectionException || storageCause instanceof ClientException) {
      return Status.UNAVAILABLE
          .withDescription(storageCause.getMessage())
          .withCause(storageCause)
          .asRuntimeException();
    }
    if (storageCause instanceof ServerException) {
      return Status.INTERNAL
          .withDescription(storageCause.getMessage())
          .withCause(storageCause)
          .asRuntimeException();
    }
    return Status.INTERNAL
        .withDescription(
            cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage())
        .withCause(cause)
        .asRuntimeException();
  }

  private Throwable unwrap(Throwable throwable) {
    Throwable current = throwable;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }
}
