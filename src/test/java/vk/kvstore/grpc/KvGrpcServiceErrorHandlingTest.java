package vk.kvstore.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.tarantool.core.connection.exceptions.ConnectionException;
import io.tarantool.core.exceptions.ClientException;
import io.tarantool.core.exceptions.ServerException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.proto.CountRequest;
import vk.kvstore.proto.GetRequest;
import vk.kvstore.proto.KvStoreGrpc;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.proto.PutRequest;
import vk.kvstore.service.InvalidRequestException;
import vk.kvstore.service.KeyValueService;
import vk.kvstore.storage.StorageException;

class KvGrpcServiceErrorHandlingTest {

  private Server server;
  private ManagedChannel channel;

  @AfterEach
  void tearDown() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void getMapsInvalidRequestToInvalidArgument() throws IOException {
    KvStoreGrpc.KvStoreBlockingStub stub =
        startServer(
            failingService(
                new InvalidRequestException("key must not be null"),
                Operation.GET));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.get(GetRequest.newBuilder().setKey("").build()));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    assertEquals("key must not be null", exception.getStatus().getDescription());
  }

  @Test
  void countMapsConnectionFailureToUnavailable() throws IOException {
    KvStoreGrpc.KvStoreBlockingStub stub =
        startServer(
            failingService(
                new StorageException("down", new ConnectionException("connection lost")),
                Operation.COUNT));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.count(CountRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAVAILABLE, exception.getStatus().getCode());
    assertEquals("connection lost", exception.getStatus().getDescription());
  }

  @Test
  void countMapsServerFailureToInternal() throws IOException {
    KvStoreGrpc.KvStoreBlockingStub stub =
        startServer(
            failingService(
                new StorageException("server failed", new ServerException("box error")),
                Operation.COUNT));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.count(CountRequest.getDefaultInstance()));

    assertEquals(Status.Code.INTERNAL, exception.getStatus().getCode());
    assertEquals("box error", exception.getStatus().getDescription());
  }

  @Test
  void putUnwrapsCompletionFailuresToUnavailable() throws IOException {
    KvStoreGrpc.KvStoreBlockingStub stub =
        startServer(
            failingService(
                new CompletionException(
                    new StorageException("client failed", new ClientException("pool exhausted"))),
                Operation.PUT));

    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                stub.put(
                    PutRequest.newBuilder()
                        .setKey("a")
                        .setValue(
                            NullableBinaryValue.newBuilder()
                                .setNullValue(Empty.getDefaultInstance())
                                .build())
                        .build()));

    assertEquals(Status.Code.UNAVAILABLE, exception.getStatus().getCode());
    assertEquals("pool exhausted", exception.getStatus().getDescription());
  }

  private KvStoreGrpc.KvStoreBlockingStub startServer(KeyValueService service) throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KvGrpcService(service))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    return KvStoreGrpc.newBlockingStub(channel);
  }

  private static KeyValueService failingService(Throwable throwable, Operation operation) {
    return new KeyValueService() {
      @Override
      public void put(String key, byte[] value) {
        if (operation == Operation.PUT) {
          rethrow(throwable);
        }
      }

      @Override
      public Optional<KeyValueEntry> get(String key) {
        if (operation == Operation.GET) {
          rethrow(throwable);
        }
        return Optional.empty();
      }

      @Override
      public boolean delete(String key) {
        if (operation == Operation.DELETE) {
          rethrow(throwable);
        }
        return false;
      }

      @Override
      public long count() {
        if (operation == Operation.COUNT) {
          rethrow(throwable);
        }
        return 0L;
      }

      @Override
      public void range(
          String keySince,
          String keyTo,
          java.util.function.Consumer<KeyValueEntry> consumer) {
        if (operation == Operation.RANGE) {
          rethrow(throwable);
        }
      }
    };
  }

  private static void rethrow(Throwable throwable) {
    if (throwable instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    if (throwable instanceof Error error) {
      throw error;
    }
    throw new RuntimeException(throwable);
  }

  private enum Operation {
    PUT,
    GET,
    DELETE,
    COUNT,
    RANGE
  }
}
