package vk.kvstore.load;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;
import vk.kvstore.app.GrpcServer;
import vk.kvstore.config.ApplicationConfig;
import vk.kvstore.proto.CountRequest;
import vk.kvstore.proto.DeleteRequest;
import vk.kvstore.proto.GetRequest;
import vk.kvstore.proto.GetResponse;
import vk.kvstore.proto.KvStoreGrpc;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.proto.PutRequest;
import vk.kvstore.proto.RangeEntry;
import vk.kvstore.proto.RangeRequest;
import vk.kvstore.storage.TarantoolClientProvider;
import vk.kvstore.storage.TarantoolKeyValueRepository;

@Testcontainers
class KvStoreLoadIT {

  private static final int TARANTOOL_PORT = 3301;
  private static final String TARANTOOL_PASSWORD = "load-secret";
  private static final int SERVER_RANGE_BATCH_SIZE = 128;
  private static final Path INIT_FILE = Path.of("infra", "tarantool", "init.lua").toAbsolutePath();
  private static final Path CONFIG_FILE =
      Path.of("infra", "tarantool", "kv_config.yml").toAbsolutePath();

  @Container
  private static final GenericContainer<?> TARANTOOL =
      new GenericContainer<>("tarantool/tarantool:3.2.1")
          .withExposedPorts(TARANTOOL_PORT)
          .withEnv("TT_CONFIG", "/opt/tarantool/kv_config.yml")
          .withEnv("TT_INSTANCE_NAME", "instance001")
          .withEnv("TARANTOOL_PASSWORD", TARANTOOL_PASSWORD)
          .withCopyFileToContainer(
              MountableFile.forHostPath(INIT_FILE), "/opt/tarantool/init.lua")
          .withCopyFileToContainer(
              MountableFile.forHostPath(CONFIG_FILE), "/opt/tarantool/kv_config.yml")
          .withCommand("tarantool", "/opt/tarantool/init.lua")
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

  private static GrpcServer grpcServer;
  private static ManagedChannel channel;
  private static KvStoreGrpc.KvStoreBlockingStub stub;
  private static TarantoolBoxClient seedingClient;
  private static TarantoolKeyValueRepository seedingRepository;

  @BeforeAll
  static void setUp() throws Exception {
    TARANTOOL.start();

    int grpcPort = findFreePort();
    ApplicationConfig config =
        new ApplicationConfig(
            grpcPort,
            TARANTOOL.getHost(),
            TARANTOOL.getMappedPort(TARANTOOL_PORT),
            "kv_app",
            TARANTOOL_PASSWORD,
            "kv",
            SERVER_RANGE_BATCH_SIZE);

    grpcServer = new GrpcServer(config);
    grpcServer.start();
    channel =
        NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.port()).usePlaintext().build();
    stub = KvStoreGrpc.newBlockingStub(channel);

    seedingClient = TarantoolClientProvider.create(config);
    seedingRepository = new TarantoolKeyValueRepository(seedingClient, "kv", SERVER_RANGE_BATCH_SIZE);
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }
    if (grpcServer != null) {
      grpcServer.close();
    }
    if (seedingClient != null) {
      seedingClient.close();
    }
  }

  @Test
  void representativeLargeDatasetCorrectness() {
    int datasetSize = Integer.getInteger("load.dataset.size", 20_000);
    String prefix = uniquePrefix("representative");
    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    seedPrefix(prefix, datasetSize);

    long countAfterSeed = stub.count(CountRequest.getDefaultInstance()).getCount();
    GetResponse nullResponse = stub.get(GetRequest.newBuilder().setKey(indexedKey(prefix, 0)).build());
    GetResponse emptyResponse = stub.get(GetRequest.newBuilder().setKey(indexedKey(prefix, 1)).build());
    GetResponse tailResponse =
        stub.get(GetRequest.newBuilder().setKey(indexedKey(prefix, datasetSize - 1)).build());
    List<RangeEntry> sampleEntries =
        collectRange(indexedKey(prefix, 0), indexedKey(prefix, 10));

    stub.put(
        PutRequest.newBuilder()
            .setKey(indexedKey(prefix, 2))
            .setValue(bytes("updated-value"))
            .build());
    long countAfterOverwrite = stub.count(CountRequest.getDefaultInstance()).getCount();
    boolean deleted =
        stub.delete(DeleteRequest.newBuilder().setKey(indexedKey(prefix, datasetSize - 1)).build())
            .getDeleted();
    long countAfterDelete = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertEquals(initialCount + datasetSize, countAfterSeed);
    assertTrue(nullResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, nullResponse.getValue().getKindCase());
    assertTrue(emptyResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.DATA, emptyResponse.getValue().getKindCase());
    assertEquals(0, emptyResponse.getValue().getData().size());
    assertTrue(tailResponse.getFound());
    assertArrayEquals(seedValue(datasetSize - 1), tailResponse.getValue().getData().toByteArray());
    assertEquals(10, sampleEntries.size());
    assertEquals(indexedKey(prefix, 0), sampleEntries.get(0).getKey());
    assertEquals(indexedKey(prefix, 9), sampleEntries.get(9).getKey());
    assertEquals(initialCount + datasetSize, countAfterOverwrite);
    assertTrue(deleted);
    assertEquals(initialCount + datasetSize - 1L, countAfterDelete);
  }

  @Test
  void loadSmokeOnSeededDataset() {
    int datasetSize = Integer.getInteger("load.smoke.seed.size", 5_000);
    int iterations = Integer.getInteger("load.smoke.iterations", 1_000);
    String prefix = uniquePrefix("smoke");
    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    seedPrefix(prefix, datasetSize);
    long expectedCount = initialCount + datasetSize;

    for (int index = 0; index < iterations; index++) {
      String existingKey = indexedKey(prefix, index % datasetSize);
      GetResponse response = stub.get(GetRequest.newBuilder().setKey(existingKey).build());
      assertTrue(response.getFound());

      if (index % 10 == 0) {
        stub.put(
            PutRequest.newBuilder()
                .setKey(existingKey)
                .setValue(bytes(("updated-" + index).getBytes(StandardCharsets.UTF_8)))
                .build());
      }

      if (index % 25 == 0) {
        List<RangeEntry> entries =
            collectRange(indexedKey(prefix, 0), indexedKey(prefix, Math.min(datasetSize, 20)));
        assertFalse(entries.isEmpty());
      }

      if (index % 40 == 0) {
        String transientKey = prefix + "transient:" + index;
        stub.put(PutRequest.newBuilder().setKey(transientKey).setValue(bytes("extra")).build());
        expectedCount++;
        assertTrue(stub.delete(DeleteRequest.newBuilder().setKey(transientKey).build()).getDeleted());
        expectedCount--;
      }

      if (index % 50 == 0) {
        assertFalse(
            stub.delete(DeleteRequest.newBuilder().setKey(prefix + "missing:" + index).build())
                .getDeleted());
        assertEquals(expectedCount, stub.count(CountRequest.getDefaultInstance()).getCount());
      }
    }

    assertEquals(expectedCount, stub.count(CountRequest.getDefaultInstance()).getCount());
  }

  private static void seedPrefix(String prefix, int datasetSize) {
    for (int index = 0; index < datasetSize; index++) {
      seedingRepository.put(indexedKey(prefix, index), seedValue(index));
    }
  }

  private static byte[] seedValue(int index) {
    if (index == 0) {
      return null;
    }
    if (index == 1) {
      return new byte[0];
    }
    return ("value-" + index).getBytes(StandardCharsets.UTF_8);
  }

  private static List<RangeEntry> collectRange(String since, String to) {
    Iterator<RangeEntry> iterator =
        stub.range(RangeRequest.newBuilder().setKeySince(since).setKeyTo(to).build());
    List<RangeEntry> entries = new ArrayList<>();
    iterator.forEachRemaining(entries::add);
    return entries;
  }

  private static NullableBinaryValue bytes(String value) {
    return bytes(value.getBytes(StandardCharsets.UTF_8));
  }

  private static NullableBinaryValue bytes(byte[] value) {
    return NullableBinaryValue.newBuilder().setData(ByteString.copyFrom(value)).build();
  }

  private static String uniquePrefix(String testName) {
    return testName + ":" + UUID.randomUUID() + ":";
  }

  private static String indexedKey(String prefix, int index) {
    String raw = "0000000" + index;
    return prefix + raw.substring(raw.length() - 7);
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }
}
