package vk.kvstore.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

public record ApplicationConfig(
    int grpcPort,
    String tarantoolHost,
    int tarantoolPort,
    String tarantoolUser,
    String tarantoolPassword,
    String tarantoolSpace,
    int rangeBatchSize) {

  private static final String RESOURCE_NAME = "application.properties";

  public ApplicationConfig {
    requirePositive(grpcPort, "grpc.port");
    requireNonBlank(tarantoolHost, "tarantool.host");
    requirePositive(tarantoolPort, "tarantool.port");
    requireNonBlank(tarantoolUser, "tarantool.user");
    Objects.requireNonNull(tarantoolPassword, "tarantool.password must not be null");
    requireNonBlank(tarantoolSpace, "tarantool.space");
    requirePositive(rangeBatchSize, "tarantool.range-batch-size");
  }

  public static ApplicationConfig load() {
    Properties properties = new Properties();
    try (InputStream inputStream =
        ApplicationConfig.class.getClassLoader().getResourceAsStream(RESOURCE_NAME)) {
      if (inputStream == null) {
        throw new IllegalStateException("Missing classpath resource: " + RESOURCE_NAME);
      }
      properties.load(inputStream);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to load " + RESOURCE_NAME, exception);
    }

    return new ApplicationConfig(
        integerValue(properties, "grpc.port", "GRPC_PORT"),
        stringValue(properties, "tarantool.host", "TARANTOOL_HOST", false),
        integerValue(properties, "tarantool.port", "TARANTOOL_PORT"),
        stringValue(properties, "tarantool.user", "TARANTOOL_USER", false),
        stringValue(properties, "tarantool.password", "TARANTOOL_PASSWORD", true),
        stringValue(properties, "tarantool.space", "TARANTOOL_SPACE", false),
        integerValue(properties, "tarantool.range-batch-size", "TARANTOOL_RANGE_BATCH_SIZE"));
  }

  private static int integerValue(Properties properties, String propertyKey, String envKey) {
    String value = resolvedValue(properties, propertyKey, envKey, false);
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException exception) {
      throw new IllegalArgumentException("Invalid integer value for " + propertyKey + ": " + value,
          exception);
    }
  }

  private static String stringValue(
      Properties properties, String propertyKey, String envKey, boolean allowBlank) {
    return resolvedValue(properties, propertyKey, envKey, allowBlank);
  }

  private static String resolvedValue(
      Properties properties, String propertyKey, String envKey, boolean allowBlank) {
    String systemProperty = System.getProperty(propertyKey);
    if (systemProperty != null) {
      return normalize(systemProperty, propertyKey, allowBlank);
    }

    String envValue = System.getenv(envKey);
    if (envValue != null) {
      return normalize(envValue, propertyKey, allowBlank);
    }

    String fileValue = properties.getProperty(propertyKey);
    if (fileValue == null) {
      throw new IllegalStateException("Missing property: " + propertyKey);
    }
    return normalize(fileValue, propertyKey, allowBlank);
  }

  private static String normalize(String value, String propertyKey, boolean allowBlank) {
    String normalized = value.trim();
    if (!allowBlank && normalized.isEmpty()) {
      throw new IllegalArgumentException(propertyKey + " must not be blank");
    }
    return allowBlank ? value : normalized;
  }

  private static void requirePositive(int value, String propertyName) {
    if (value <= 0) {
      throw new IllegalArgumentException(propertyName + " must be greater than 0");
    }
  }

  private static void requireNonBlank(String value, String propertyName) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(propertyName + " must not be blank");
    }
  }
}
