package vk.kvstore.app;

public final class ServerLifecycleManager {

  public void start() throws Exception {
    try (GrpcServer server = GrpcServer.createDefault()) {
      server.start();
      server.awaitTermination();
    }
  }
}
