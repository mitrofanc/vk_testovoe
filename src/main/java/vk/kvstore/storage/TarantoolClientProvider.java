package vk.kvstore.storage;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolBoxClientBuilder;
import io.tarantool.client.factory.TarantoolFactory;
import lombok.experimental.UtilityClass;
import vk.kvstore.config.ApplicationConfig;

@UtilityClass
public class TarantoolClientProvider {

  public TarantoolBoxClient create(ApplicationConfig config) {
    TarantoolBoxClientBuilder builder =
        TarantoolFactory.box()
            .withHost(config.tarantoolHost())
            .withPort(config.tarantoolPort())
            .withUser(config.tarantoolUser())
            .withFetchSchema(true);

    if (!config.tarantoolPassword().isEmpty()) {
      builder.withPassword(config.tarantoolPassword());
    }

    try {
      return builder.build();
    } catch (Exception exception) {
      throw new IllegalStateException("Failed to create Tarantool client", exception);
    }
  }
}
