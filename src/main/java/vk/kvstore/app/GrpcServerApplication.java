package vk.kvstore.app;

public final class GrpcServerApplication {

  private GrpcServerApplication() {}

  public static void main(String[] args) throws Exception {
    new ServerLifecycleManager().start();
  }
}
