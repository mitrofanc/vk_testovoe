package vk.kvstore.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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

@Testcontainers
class KvStoreIntegrationIT {

  private static final int TARANTOOL_PORT = 3301;
  private static final String TARANTOOL_PASSWORD = "integration-secret";
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
  private static long initialCountAtStartup;

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
            2);

    grpcServer = new GrpcServer(config);
    grpcServer.start();
    channel =
        NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.port()).usePlaintext().build();
    stub = KvStoreGrpc.newBlockingStub(channel);
    initialCountAtStartup = stub.count(CountRequest.getDefaultInstance()).getCount();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    if (grpcServer != null) {
      grpcServer.close();
    }
  }

  @Test
  void happyPathWithRealTarantool() {
    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    stub.put(PutRequest.newBuilder().setKey("happy:a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("happy:b").setValue(nullValue()).build());
    stub.put(PutRequest.newBuilder().setKey("happy:c").setValue(bytes(new byte[0])).build());

    GetResponse first = stub.get(GetRequest.newBuilder().setKey("happy:a").build());
    GetResponse second = stub.get(GetRequest.newBuilder().setKey("happy:b").build());
    GetResponse third = stub.get(GetRequest.newBuilder().setKey("missing").build());
    List<RangeEntry> rangeEntries = collectRange("happy:", "happy;");
    long countBeforeDelete = stub.count(CountRequest.getDefaultInstance()).getCount();
    boolean deleted =
        stub.delete(DeleteRequest.newBuilder().setKey("happy:a").build()).getDeleted();
    long countAfterDelete = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertTrue(first.getFound());
    assertEquals("one", first.getValue().getData().toStringUtf8());
    assertTrue(second.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, second.getValue().getKindCase());
    assertFalse(third.getFound());
    assertIterableEquals(
        List.of("happy:a", "happy:b", "happy:c"),
        rangeEntries.stream().map(RangeEntry::getKey).toList());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, rangeEntries.get(1).getValue().getKindCase());
    assertEquals(0, rangeEntries.get(2).getValue().getData().size());
    assertEquals(initialCount + 3L, countBeforeDelete);
    assertTrue(deleted);
    assertEquals(initialCount + 2L, countAfterDelete);
  }

  @Test
  void freshContainerStartsEmpty() {
    assertEquals(0L, initialCountAtStartup);
  }

  @Test
  void getPreservesBinaryPayload() {
    byte[] payload = new byte[] {0, 1, 2, 3, 4};
    stub.put(PutRequest.newBuilder().setKey("payload:bin").setValue(bytes(payload)).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("payload:bin").build());

    assertTrue(response.getFound());
    assertArrayEquals(payload, response.getValue().getData().toByteArray());
  }

  @Test
  void emptyKeyRoundTripsWithRealTarantool() {
    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    stub.put(PutRequest.newBuilder().setKey("").setValue(bytes("root")).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("").build());
    boolean deleted = stub.delete(DeleteRequest.newBuilder().setKey("").build()).getDeleted();
    long finalCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertTrue(response.getFound());
    assertEquals("root", response.getValue().getData().toStringUtf8());
    assertTrue(deleted);
    assertEquals(initialCount, finalCount);
  }

  @Test
  void getDistinguishesNullFromEmptyBytesWithRealTarantool() {
    String prefix = uniquePrefix("nullable");
    String nullKey = prefix + "null";
    String emptyKey = prefix + "empty";

    stub.put(PutRequest.newBuilder().setKey(nullKey).setValue(nullValue()).build());
    stub.put(PutRequest.newBuilder().setKey(emptyKey).setValue(bytes(new byte[0])).build());

    GetResponse nullResponse = stub.get(GetRequest.newBuilder().setKey(nullKey).build());
    GetResponse emptyResponse = stub.get(GetRequest.newBuilder().setKey(emptyKey).build());

    assertTrue(nullResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, nullResponse.getValue().getKindCase());
    assertTrue(emptyResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.DATA, emptyResponse.getValue().getKindCase());
    assertEquals(0, emptyResponse.getValue().getData().size());
  }

  @Test
  void overwriteAndRepeatedDeleteKeepCountCorrect() {
    String prefix = uniquePrefix("count");
    String key = prefix + "alpha";
    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();

    stub.put(PutRequest.newBuilder().setKey(key).setValue(bytes("one")).build());
    long countAfterInsert = stub.count(CountRequest.getDefaultInstance()).getCount();
    stub.put(PutRequest.newBuilder().setKey(key).setValue(bytes("two")).build());
    long countAfterOverwrite = stub.count(CountRequest.getDefaultInstance()).getCount();
    boolean deleted = stub.delete(DeleteRequest.newBuilder().setKey(key).build()).getDeleted();
    long countAfterDelete = stub.count(CountRequest.getDefaultInstance()).getCount();
    boolean deletedAgain = stub.delete(DeleteRequest.newBuilder().setKey(key).build()).getDeleted();
    long countAfterRepeatedDelete = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertEquals(initialCount + 1L, countAfterInsert);
    assertEquals(initialCount + 1L, countAfterOverwrite);
    assertTrue(deleted);
    assertEquals(initialCount, countAfterDelete);
    assertFalse(deletedAgain);
    assertEquals(initialCount, countAfterRepeatedDelete);
  }

  @Test
  void rangeKeepsLexicographicOrderAcrossBatches() {
    String prefix = uniquePrefix("range");
    String upperBound = upperBoundForPrefix(prefix);
    stub.put(PutRequest.newBuilder().setKey(prefix + "c").setValue(bytes("three")).build());
    stub.put(PutRequest.newBuilder().setKey(prefix + "a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey(prefix + "f").setValue(bytes("six")).build());
    stub.put(PutRequest.newBuilder().setKey(prefix + "b").setValue(bytes("two")).build());
    stub.put(PutRequest.newBuilder().setKey(prefix + "e").setValue(nullValue()).build());
    stub.put(PutRequest.newBuilder().setKey(prefix + "d").setValue(bytes(new byte[0])).build());

    List<RangeEntry> entries = collectRange(prefix, upperBound);

    assertIterableEquals(
        List.of(prefix + "a", prefix + "b", prefix + "c", prefix + "d", prefix + "e", prefix + "f"),
        entries.stream().map(RangeEntry::getKey).toList());
    assertEquals(NullableBinaryValue.KindCase.DATA, entries.get(3).getValue().getKindCase());
    assertEquals(0, entries.get(3).getValue().getData().size());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, entries.get(4).getValue().getKindCase());
  }

  @Test
  void rangeReturnsEmptyWhenLowerBoundIsGreaterThanUpperBound() {
    String prefix = uniquePrefix("range-empty");

    List<RangeEntry> entries = collectRange(prefix + "z", prefix + "a");

    assertTrue(entries.isEmpty());
  }

  @Test
  void longKeyAndPayloadRoundTrip() {
    String prefix = uniquePrefix("long");
    String key = prefix + "k".repeat(4096);
    byte[] payload = "v".repeat(262_144).getBytes(StandardCharsets.UTF_8);

    stub.put(PutRequest.newBuilder().setKey(key).setValue(bytes(payload)).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey(key).build());

    assertTrue(response.getFound());
    assertArrayEquals(payload, response.getValue().getData().toByteArray());
  }

  private static List<RangeEntry> collectRange(String since, String to) {
    Iterator<RangeEntry> iterator =
        stub.range(RangeRequest.newBuilder().setKeySince(since).setKeyTo(to).build());
    List<RangeEntry> entries = new ArrayList<>();
    iterator.forEachRemaining(entries::add);
    return entries;
  }

  private static NullableBinaryValue bytes(String value) {
    return bytes(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static NullableBinaryValue bytes(byte[] value) {
    return NullableBinaryValue.newBuilder().setData(ByteString.copyFrom(value)).build();
  }

  private static NullableBinaryValue nullValue() {
    return NullableBinaryValue.newBuilder().setNullValue(Empty.getDefaultInstance()).build();
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }

  private static String uniquePrefix(String testName) {
    return testName + ":" + UUID.randomUUID() + ":";
  }

  private static String upperBoundForPrefix(String prefix) {
    return prefix.substring(0, prefix.length() - 1) + ";";
  }
}
