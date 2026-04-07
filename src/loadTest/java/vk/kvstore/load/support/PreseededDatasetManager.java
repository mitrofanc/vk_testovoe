package vk.kvstore.load.support;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.MountableFile;

public final class PreseededDatasetManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PreseededDatasetManager.class);

  private static final String TARANTOOL_IMAGE = "tarantool/tarantool:3.2.1";
  private static final int TARANTOOL_PORT = 3301;
  private static final String TARANTOOL_PASSWORD = "load-secret";
  private static final String DATASET_PREFIX = "full:";
  private static final String DATASET_ROOT_PROPERTY = "load.full.dataset.root";
  private static final String PROGRESS_STEP_PROPERTY = "load.full.prepare.progress.step";
  private static final String PREPARE_TIMEOUT_PROPERTY = "load.full.prepare.timeout.minutes";
  private static final String RUNTIME_TIMEOUT_PROPERTY = "load.full.runtime.timeout.minutes";
  private static final String METADATA_FILE_NAME = ".preseeded-dataset.properties";
  private static final String DATASET_FORMAT_VERSION = "2";
  private static final String READY_COMMAND =
      "sh -c \"printf '\\\\quit\\n' | "
          + "TT_CLI_USERNAME=kv_app "
          + "TT_CLI_PASSWORD=\\\"$TARANTOOL_PASSWORD\\\" "
          + "tt connect 127.0.0.1:3301 --binary >/dev/null\"";

  private static final Path INIT_FILE = Path.of("infra", "tarantool", "init.lua").toAbsolutePath();
  private static final Path CONFIG_FILE =
      Path.of("infra", "tarantool", "kv_config.yml").toAbsolutePath();
  private static final Path PREPARE_SCRIPT =
      Path.of("infra", "tarantool", "prepare_full_dataset.lua").toAbsolutePath();

  private PreseededDatasetManager() {}

  public static PreparedRuntime startPreparedRuntime(int datasetSize) {
    Path baseDatasetDir = ensureBaseDataset(datasetSize);
    Path runtimeDatasetDir = copyBaseDataset(baseDatasetDir, datasetSize);
    GenericContainer<?> container = createRuntimeContainer(runtimeDatasetDir);
    try {
      container.start();
      return new PreparedRuntime(container, runtimeDatasetDir, datasetSize, DATASET_PREFIX);
    } catch (RuntimeException exception) {
      try {
        deleteRecursively(runtimeDatasetDir);
      } catch (IOException cleanupException) {
        exception.addSuppressed(cleanupException);
      }
      throw exception;
    }
  }

  public static String tarantoolPassword() {
    return TARANTOOL_PASSWORD;
  }

  private static Path ensureBaseDataset(int datasetSize) {
    Path baseDatasetDir = datasetRoot().resolve("full-" + datasetSize + "-base");
    Path metadataFile = baseDatasetDir.resolve(METADATA_FILE_NAME);

    if (isReusable(baseDatasetDir, metadataFile, datasetSize)) {
      LOGGER.info("Reusing preseeded dataset at {}", baseDatasetDir);
      return baseDatasetDir;
    }

    try {
      deleteRecursively(baseDatasetDir);
      Files.createDirectories(baseDatasetDir);
    } catch (IOException exception) {
      throw new UncheckedIOException("Failed to create dataset directory " + baseDatasetDir, exception);
    }

    LOGGER.info("Preparing preseeded dataset at {} with {} records", baseDatasetDir, datasetSize);
    prepareBaseDataset(baseDatasetDir, datasetSize);
    writeMetadata(metadataFile, datasetSize);
    return baseDatasetDir;
  }

  private static void prepareBaseDataset(Path datasetDir, int datasetSize) {
    GenericContainer<?> prepareContainer =
        new GenericContainer<>(TARANTOOL_IMAGE)
            .withFileSystemBind(datasetDir.toString(), "/var/lib/tarantool", BindMode.READ_WRITE)
            .withEnv("TT_CONFIG", "/opt/tarantool/kv_config.yml")
            .withEnv("TT_INSTANCE_NAME", "instance001")
            .withEnv("TARANTOOL_PASSWORD", TARANTOOL_PASSWORD)
            .withEnv("LOAD_DATASET_SIZE", Integer.toString(datasetSize))
            .withEnv("LOAD_DATASET_PREFIX", DATASET_PREFIX)
            .withEnv("LOAD_DATASET_PROGRESS_STEP", Integer.toString(progressStep()))
            .withCopyFileToContainer(
                MountableFile.forHostPath(INIT_FILE), "/opt/tarantool/init.lua")
            .withCopyFileToContainer(
                MountableFile.forHostPath(CONFIG_FILE), "/opt/tarantool/kv_config.yml")
            .withCopyFileToContainer(
                MountableFile.forHostPath(PREPARE_SCRIPT),
                "/opt/tarantool/prepare_full_dataset.lua")
            .withCommand("tarantool", "/opt/tarantool/prepare_full_dataset.lua")
            .withStartupCheckStrategy(
                new OneShotStartupCheckStrategy().withTimeout(prepareTimeout()))
            .waitingFor(Wait.forLogMessage(".*Dataset preparation completed:.*\\n", 1));

    try {
      prepareContainer.start();
    } finally {
      prepareContainer.close();
    }
  }

  private static GenericContainer<?> createRuntimeContainer(Path datasetDir) {
    return new GenericContainer<>(TARANTOOL_IMAGE)
        .withExposedPorts(TARANTOOL_PORT)
        .withFileSystemBind(datasetDir.toString(), "/var/lib/tarantool", BindMode.READ_WRITE)
        .withEnv("TT_CONFIG", "/opt/tarantool/kv_config.yml")
        .withEnv("TT_INSTANCE_NAME", "instance001")
        .withEnv("TARANTOOL_PASSWORD", TARANTOOL_PASSWORD)
        .withCopyFileToContainer(MountableFile.forHostPath(INIT_FILE), "/opt/tarantool/init.lua")
        .withCopyFileToContainer(
            MountableFile.forHostPath(CONFIG_FILE), "/opt/tarantool/kv_config.yml")
        .withCommand("tarantool", "/opt/tarantool/init.lua")
        .waitingFor(
            Wait.forSuccessfulCommand(READY_COMMAND).withStartupTimeout(runtimeTimeout()));
  }

  private static Path copyBaseDataset(Path baseDatasetDir, int datasetSize) {
    Path runtimeDatasetDir =
        datasetRoot().resolve("full-" + datasetSize + "-run-" + UUID.randomUUID());
    try {
      copyRecursively(baseDatasetDir, runtimeDatasetDir);
      return runtimeDatasetDir;
    } catch (IOException exception) {
      throw new UncheckedIOException(
          "Failed to copy preseeded dataset into runtime directory " + runtimeDatasetDir,
          exception);
    }
  }

  private static boolean isReusable(Path baseDatasetDir, Path metadataFile, int datasetSize) {
    if (!Files.isDirectory(baseDatasetDir) || !Files.isRegularFile(metadataFile)) {
      return false;
    }

    Properties properties = new Properties();
    try (var inputStream = Files.newInputStream(metadataFile)) {
      properties.load(inputStream);
    } catch (IOException exception) {
      LOGGER.warn("Failed to read dataset metadata from {}", metadataFile, exception);
      return false;
    }

    String size = properties.getProperty("dataset.size");
    String prefix = properties.getProperty("dataset.prefix");
    String formatVersion = properties.getProperty("dataset.format.version");
    if (!Integer.toString(datasetSize).equals(size)
        || !DATASET_PREFIX.equals(prefix)
        || !DATASET_FORMAT_VERSION.equals(formatVersion)) {
      return false;
    }

    try (var files = Files.list(baseDatasetDir)) {
      return files.anyMatch(path -> path.getFileName().toString().endsWith(".snap"));
    } catch (IOException exception) {
      LOGGER.warn("Failed to inspect dataset directory {}", baseDatasetDir, exception);
      return false;
    }
  }

  private static void writeMetadata(Path metadataFile, int datasetSize) {
    Properties properties = new Properties();
    properties.setProperty("dataset.size", Integer.toString(datasetSize));
    properties.setProperty("dataset.prefix", DATASET_PREFIX);
    properties.setProperty("dataset.format.version", DATASET_FORMAT_VERSION);
    properties.setProperty("tarantool.image", TARANTOOL_IMAGE);
    try (OutputStream outputStream = Files.newOutputStream(metadataFile)) {
      properties.store(outputStream, "Preseeded load-test dataset");
    } catch (IOException exception) {
      throw new UncheckedIOException("Failed to write dataset metadata " + metadataFile, exception);
    }
  }

  private static Path datasetRoot() {
    return Path.of(System.getProperty(DATASET_ROOT_PROPERTY, "target/preseeded-datasets"))
        .toAbsolutePath();
  }

  private static int progressStep() {
    return Integer.getInteger(PROGRESS_STEP_PROPERTY, 250_000);
  }

  private static Duration prepareTimeout() {
    return Duration.ofMinutes(Long.getLong(PREPARE_TIMEOUT_PROPERTY, 30L));
  }

  private static Duration runtimeTimeout() {
    return Duration.ofMinutes(Long.getLong(RUNTIME_TIMEOUT_PROPERTY, 20L));
  }

  private static void copyRecursively(Path source, Path target) throws IOException {
    Files.walkFileTree(
        source,
        new SimpleFileVisitor<>() {
          @Override
          public java.nio.file.FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
              throws IOException {
            Files.createDirectories(target.resolve(source.relativize(dir)));
            return java.nio.file.FileVisitResult.CONTINUE;
          }

          @Override
          public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.copy(
                file,
                target.resolve(source.relativize(file)),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES);
            return java.nio.file.FileVisitResult.CONTINUE;
          }
        });
  }

  private static void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }

    Files.walkFileTree(
        path,
        new SimpleFileVisitor<>() {
          @Override
          public java.nio.file.FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return java.nio.file.FileVisitResult.CONTINUE;
          }

          @Override
          public java.nio.file.FileVisitResult postVisitDirectory(Path dir, IOException exc)
              throws IOException {
            Files.delete(dir);
            return java.nio.file.FileVisitResult.CONTINUE;
          }
        });
  }

  public static final class PreparedRuntime implements AutoCloseable {

    private final GenericContainer<?> container;
    private final Path runtimeDatasetDir;
    private final int datasetSize;
    private final String datasetPrefix;

    private PreparedRuntime(
        GenericContainer<?> container, Path runtimeDatasetDir, int datasetSize, String datasetPrefix) {
      this.container = container;
      this.runtimeDatasetDir = runtimeDatasetDir;
      this.datasetSize = datasetSize;
      this.datasetPrefix = datasetPrefix;
    }

    public String host() {
      return container.getHost();
    }

    public int port() {
      return container.getMappedPort(TARANTOOL_PORT);
    }

    public int datasetSize() {
      return datasetSize;
    }

    public String datasetPrefix() {
      return datasetPrefix;
    }

    @Override
    public void close() throws Exception {
      try {
        container.close();
      } finally {
        deleteRecursively(runtimeDatasetDir);
      }
    }
  }
}
