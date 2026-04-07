package vk.kvstore.app;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vk.kvstore.config.ApplicationConfig;
import vk.kvstore.grpc.KvGrpcService;
import vk.kvstore.service.DefaultKeyValueService;
import vk.kvstore.service.KeyValueService;
import vk.kvstore.storage.KeyValueRepository;
import vk.kvstore.storage.TarantoolClientProvider;
import vk.kvstore.storage.TarantoolKeyValueRepository;

/**
 * Owns the gRPC server and Tarantool client lifecycle.
 *
 * <p>The server is configured lazily in {@link #start()} so tests or callers can instantiate the
 * class without opening network resources immediately. Shutdown is idempotent and closes the gRPC
 * server before releasing the Tarantool client.
 */
public final class GrpcServer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5L;

  private final ApplicationConfig config;
  private final Thread shutdownHook;

  private TarantoolBoxClient tarantoolClient;
  private Server server;
  private boolean started;
  private boolean closed;
  private boolean shutdownHookRegistered;

  public GrpcServer(ApplicationConfig config) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.shutdownHook = new Thread(this::close, "grpc-shutdown");
  }

  public static GrpcServer createDefault() {
    return new GrpcServer(ApplicationConfig.load());
  }

  public synchronized void start() throws IOException {
    if (started) {
      throw new IllegalStateException("gRPC server is already started");
    }
    if (closed) {
      throw new IllegalStateException("gRPC server is already closed");
    }

    tarantoolClient = TarantoolClientProvider.create(config);
    try {
      KeyValueRepository repository =
          new TarantoolKeyValueRepository(
              tarantoolClient, config.tarantoolSpace(), config.rangeBatchSize());
      KeyValueService keyValueService = new DefaultKeyValueService(repository);

      server =
          NettyServerBuilder.forPort(config.grpcPort())
              .addService(new KvGrpcService(keyValueService))
              .build()
              .start();
      started = true;
      registerShutdownHook();

      LOGGER.info(
          "gRPC server started on port {} and connected to {}:{} space={}",
          server.getPort(),
          config.tarantoolHost(),
          config.tarantoolPort(),
          config.tarantoolSpace());
    } catch (Exception exception) {
      stopServerQuietly();
      closeTarantoolClientQuietly();
      if (exception instanceof IOException ioException) {
        throw ioException;
      }
      throw new IllegalStateException("Failed to start gRPC server", exception);
    }
  }

  public void awaitTermination() throws InterruptedException {
    Server currentServer = requireStartedServer();
    currentServer.awaitTermination();
  }

  public int port() {
    Server currentServer = requireStartedServer();
    return currentServer.getPort();
  }

  public void stop() {
    close();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    unregisterShutdownHook();
    stopServerQuietly();
    closeTarantoolClientQuietly();
    started = false;
  }

  private Server requireStartedServer() {
    Server currentServer = server;
    if (!started || currentServer == null) {
      throw new IllegalStateException("gRPC server has not been started");
    }
    return currentServer;
  }

  private void registerShutdownHook() {
    if (shutdownHookRegistered) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    shutdownHookRegistered = true;
  }

  private void unregisterShutdownHook() {
    if (!shutdownHookRegistered || Thread.currentThread() == shutdownHook) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException exception) {
      LOGGER.debug("JVM shutdown is already in progress, shutdown hook cannot be removed");
    } finally {
      shutdownHookRegistered = false;
    }
  }

  private void stopServerQuietly() {
    Server currentServer = server;
    server = null;
    if (currentServer == null) {
      return;
    }

    try {
      currentServer.shutdown();
      if (!currentServer.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        currentServer.shutdownNow();
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      currentServer.shutdownNow();
    }
  }

  private void closeTarantoolClientQuietly() {
    TarantoolBoxClient currentClient = tarantoolClient;
    tarantoolClient = null;
    if (currentClient == null) {
      return;
    }

    try {
      currentClient.close();
    } catch (Exception exception) {
      LOGGER.warn("Failed to close Tarantool client cleanly", exception);
    }
  }
}
