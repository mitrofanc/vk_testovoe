package vk.kvstore.load;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import vk.kvstore.app.GrpcServer;
import vk.kvstore.config.ApplicationConfig;
import vk.kvstore.load.support.PreseededDatasetManager;
import vk.kvstore.proto.CountRequest;
import vk.kvstore.proto.DeleteRequest;
import vk.kvstore.proto.GetRequest;
import vk.kvstore.proto.GetResponse;
import vk.kvstore.proto.KvStoreGrpc;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.proto.PutRequest;
import vk.kvstore.proto.RangeEntry;
import vk.kvstore.proto.RangeRequest;

class KvStoreFullPreseededIT {

  private static final int SERVER_RANGE_BATCH_SIZE = 128;

  private static PreseededDatasetManager.PreparedRuntime preparedRuntime;
  private static GrpcServer grpcServer;
  private static ManagedChannel channel;
  private static KvStoreGrpc.KvStoreBlockingStub stub;
  private static String datasetPrefix;
  private static int datasetSize;

  @BeforeAll
  static void setUp() throws Exception {
    datasetSize = Integer.getInteger("load.full.dataset.size", 5_000_000);
    preparedRuntime = PreseededDatasetManager.startPreparedRuntime(datasetSize);
    datasetPrefix = preparedRuntime.datasetPrefix();

    int grpcPort = findFreePort();
    ApplicationConfig config =
        new ApplicationConfig(
            grpcPort,
            preparedRuntime.host(),
            preparedRuntime.port(),
            "kv_app",
            PreseededDatasetManager.tarantoolPassword(),
            "kv",
            SERVER_RANGE_BATCH_SIZE);

    grpcServer = new GrpcServer(config);
    grpcServer.start();
    channel =
        NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.port()).usePlaintext().build();
    stub = KvStoreGrpc.newBlockingStub(channel);
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
    if (preparedRuntime != null) {
      preparedRuntime.close();
    }
  }

  @Test
  void fullDatasetCorrectnessUsesPreseededDataset() {
    String nullKey = indexedKey(0);
    String emptyKey = indexedKey(1);
    String tailKey = indexedKey(datasetSize - 1);
    String insertedKey = datasetPrefix + "inserted";

    long initialCount = stub.count(CountRequest.getDefaultInstance()).getCount();
    GetResponse nullResponse = stub.get(GetRequest.newBuilder().setKey(nullKey).build());
    GetResponse emptyResponse = stub.get(GetRequest.newBuilder().setKey(emptyKey).build());

    stub.put(PutRequest.newBuilder().setKey(tailKey).setValue(bytes("updated-tail")).build());
    long countAfterOverwrite = stub.count(CountRequest.getDefaultInstance()).getCount();
    GetResponse tailResponse = stub.get(GetRequest.newBuilder().setKey(tailKey).build());

    stub.put(PutRequest.newBuilder().setKey(insertedKey).setValue(bytes("inserted")).build());
    long countAfterInsert = stub.count(CountRequest.getDefaultInstance()).getCount();
    List<RangeEntry> tailEntries = collectRange(indexedKey(datasetSize - 5), indexedKey(datasetSize));
    boolean insertedDeleted =
        stub.delete(DeleteRequest.newBuilder().setKey(insertedKey).build()).getDeleted();
    boolean tailDeleted = stub.delete(DeleteRequest.newBuilder().setKey(tailKey).build()).getDeleted();
    long countAfterDeletes = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertEquals(datasetSize, initialCount);
    assertTrue(nullResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, nullResponse.getValue().getKindCase());
    assertTrue(emptyResponse.getFound());
    assertEquals(NullableBinaryValue.KindCase.DATA, emptyResponse.getValue().getKindCase());
    assertEquals(0, emptyResponse.getValue().getData().size());
    assertEquals(datasetSize, countAfterOverwrite);
    assertTrue(tailResponse.getFound());
    assertArrayEquals(
        "updated-tail".getBytes(StandardCharsets.UTF_8), tailResponse.getValue().getData().toByteArray());
    assertEquals(5, tailEntries.size());
    assertEquals(indexedKey(datasetSize - 5), tailEntries.get(0).getKey());
    assertEquals(indexedKey(datasetSize - 1), tailEntries.get(4).getKey());
    assertEquals(datasetSize + 1L, countAfterInsert);
    assertTrue(insertedDeleted);
    assertTrue(tailDeleted);
    assertEquals(datasetSize - 1L, countAfterDeletes);
  }

  private static List<RangeEntry> collectRange(String since, String to) {
    Iterator<RangeEntry> iterator =
        stub.range(RangeRequest.newBuilder().setKeySince(since).setKeyTo(to).build());
    List<RangeEntry> entries = new ArrayList<>();
    iterator.forEachRemaining(entries::add);
    return entries;
  }

  private static NullableBinaryValue bytes(String value) {
    return NullableBinaryValue.newBuilder()
        .setData(ByteString.copyFrom(value, StandardCharsets.UTF_8))
        .build();
  }

  private static String indexedKey(int index) {
    return datasetPrefix + "%07d".formatted(index);
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      return serverSocket.getLocalPort();
    }
  }
}
