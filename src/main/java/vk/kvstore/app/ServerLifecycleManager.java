package vk.kvstore.app;

/**
 * Keeps the application entry point minimal while delegating resource management to {@link
 * GrpcServer}.
 */
public final class ServerLifecycleManager {

  public void start() throws Exception {
    try (GrpcServer server = GrpcServer.createDefault()) {
      server.start();
      server.awaitTermination();
    }
  }
}
