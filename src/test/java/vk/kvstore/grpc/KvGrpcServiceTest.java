package vk.kvstore.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.kvstore.proto.CountRequest;
import vk.kvstore.proto.DeleteRequest;
import vk.kvstore.proto.GetRequest;
import vk.kvstore.proto.GetResponse;
import vk.kvstore.proto.KvStoreGrpc;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.proto.PutRequest;
import vk.kvstore.proto.RangeEntry;
import vk.kvstore.proto.RangeRequest;
import vk.kvstore.service.DefaultKeyValueService;
import vk.kvstore.support.InMemoryKeyValueRepository;

class KvGrpcServiceTest {

  private InMemoryKeyValueRepository repository;
  private Server server;
  private ManagedChannel channel;
  private KvStoreGrpc.KvStoreBlockingStub stub;

  @BeforeEach
  void setUp() throws IOException {
    repository = new InMemoryKeyValueRepository();
    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new KvGrpcService(new DefaultKeyValueService(repository)))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    stub = KvStoreGrpc.newBlockingStub(channel);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
    if (server != null) {
      server.shutdownNow();
      server.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void putInsertsNewKey() {
    stub.put(PutRequest.newBuilder().setKey("alpha").setValue(bytes("one")).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("alpha").build());

    assertTrue(response.getFound());
    assertEquals(NullableBinaryValue.KindCase.DATA, response.getValue().getKindCase());
    assertEquals("one", response.getValue().getData().toStringUtf8());
    assertEquals(1L, stub.count(CountRequest.getDefaultInstance()).getCount());
  }

  @Test
  void putOverwritesExistingKey() {
    stub.put(PutRequest.newBuilder().setKey("alpha").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("alpha").setValue(bytes("two")).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("alpha").build());

    assertTrue(response.getFound());
    assertEquals("two", response.getValue().getData().toStringUtf8());
    assertEquals(1L, stub.count(CountRequest.getDefaultInstance()).getCount());
  }

  @Test
  void putSupportsNullValue() {
    stub.put(PutRequest.newBuilder().setKey("null-key").setValue(nullValue()).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("null-key").build());

    assertTrue(response.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, response.getValue().getKindCase());
  }

  @Test
  void getReturnsExistingNonNullValue() {
    byte[] bytes = new byte[] {1, 2, 3};
    stub.put(PutRequest.newBuilder().setKey("bytes").setValue(bytes(bytes)).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("bytes").build());

    assertTrue(response.getFound());
    assertArrayEquals(bytes, response.getValue().getData().toByteArray());
  }

  @Test
  void getReturnsExistingNullValue() {
    stub.put(PutRequest.newBuilder().setKey("null-key").setValue(nullValue()).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("null-key").build());

    assertTrue(response.getFound());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, response.getValue().getKindCase());
  }

  @Test
  void getReturnsMissingKey() {
    GetResponse response = stub.get(GetRequest.newBuilder().setKey("missing").build());

    assertFalse(response.getFound());
    assertFalse(response.hasValue());
  }

  @Test
  void deleteReturnsTrueForExistingKey() {
    stub.put(PutRequest.newBuilder().setKey("delete-me").setValue(bytes("value")).build());

    boolean deleted = stub.delete(DeleteRequest.newBuilder().setKey("delete-me").build()).getDeleted();

    assertTrue(deleted);
    assertFalse(stub.get(GetRequest.newBuilder().setKey("delete-me").build()).getFound());
  }

  @Test
  void deleteReturnsFalseForMissingKey() {
    boolean deleted = stub.delete(DeleteRequest.newBuilder().setKey("missing").build()).getDeleted();

    assertFalse(deleted);
  }

  @Test
  void countReflectsPutAndDelete() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("three")).build());
    stub.delete(DeleteRequest.newBuilder().setKey("a").build());

    long count = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertEquals(1L, count);
  }

  @Test
  void countReturnsZeroForEmptyStore() {
    long count = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertEquals(0L, count);
  }

  @Test
  void rangeReturnsEmptyStream() {
    List<RangeEntry> entries = collectRange("a", "z");

    assertTrue(entries.isEmpty());
  }

  @Test
  void rangeStreamsSeveralEntries() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());
    stub.put(PutRequest.newBuilder().setKey("c").setValue(bytes("three")).build());
    stub.put(PutRequest.newBuilder().setKey("d").setValue(bytes("four")).build());

    List<RangeEntry> entries = collectRange("b", "d");

    assertEquals(List.of("b", "c"), entries.stream().map(RangeEntry::getKey).toList());
    assertEquals("two", entries.get(0).getValue().getData().toStringUtf8());
    assertEquals("three", entries.get(1).getValue().getData().toStringUtf8());
  }

  @Test
  void rangeStreamsNullValue() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(nullValue()).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes(new byte[0])).build());

    List<RangeEntry> entries = collectRange("a", "c");

    assertEquals(2, entries.size());
    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, entries.get(0).getValue().getKindCase());
    assertEquals(NullableBinaryValue.KindCase.DATA, entries.get(1).getValue().getKindCase());
    assertEquals(0, entries.get(1).getValue().getData().size());
  }

  @Test
  void getDistinguishesNullFromEmptyBytes() {
    stub.put(PutRequest.newBuilder().setKey("null-key").setValue(nullValue()).build());
    stub.put(PutRequest.newBuilder().setKey("empty-key").setValue(bytes(new byte[0])).build());

    GetResponse nullResponse = stub.get(GetRequest.newBuilder().setKey("null-key").build());
    GetResponse emptyResponse = stub.get(GetRequest.newBuilder().setKey("empty-key").build());

    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, nullResponse.getValue().getKindCase());
    assertEquals(NullableBinaryValue.KindCase.DATA, emptyResponse.getValue().getKindCase());
    assertEquals(0, emptyResponse.getValue().getData().size());
  }

  @Test
  void rangeReturnsSingleElement() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());
    stub.put(PutRequest.newBuilder().setKey("c").setValue(bytes("three")).build());

    List<RangeEntry> entries = collectRange("b", "c");

    assertEquals(1, entries.size());
    assertEquals("b", entries.get(0).getKey());
  }

  @Test
  void rangeSortsKeysLexicographically() {
    stub.put(PutRequest.newBuilder().setKey("c").setValue(bytes("three")).build());
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());

    List<RangeEntry> entries = collectRange("a", "d");

    assertEquals(List.of("a", "b", "c"), entries.stream().map(RangeEntry::getKey).toList());
  }

  @Test
  void rangeUsesInclusiveLowerAndExclusiveUpperBounds() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());
    stub.put(PutRequest.newBuilder().setKey("c").setValue(bytes("three")).build());

    List<RangeEntry> entries = collectRange("b", "c");
    List<RangeEntry> emptyWhenBoundsEqual = collectRange("b", "b");

    assertEquals(List.of("b"), entries.stream().map(RangeEntry::getKey).toList());
    assertTrue(emptyWhenBoundsEqual.isEmpty());
  }

  @Test
  void rangeReturnsEmptyWhenLowerBoundIsGreaterThanUpperBound() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());
    stub.put(PutRequest.newBuilder().setKey("b").setValue(bytes("two")).build());

    List<RangeEntry> entries = collectRange("c", "a");

    assertTrue(entries.isEmpty());
  }

  @Test
  void emptyKeyIsAccepted() {
    stub.put(PutRequest.newBuilder().setKey("").setValue(bytes("root")).build());

    GetResponse response = stub.get(GetRequest.newBuilder().setKey("").build());
    long count = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertTrue(response.getFound());
    assertEquals("root", response.getValue().getData().toStringUtf8());
    assertEquals(1L, count);
  }

  @Test
  void deletingMissingKeyDoesNotChangeCount() {
    stub.put(PutRequest.newBuilder().setKey("a").setValue(bytes("one")).build());

    boolean deleted = stub.delete(DeleteRequest.newBuilder().setKey("missing").build()).getDeleted();
    long count = stub.count(CountRequest.getDefaultInstance()).getCount();

    assertFalse(deleted);
    assertEquals(1L, count);
  }

  @Test
  void putRejectsUnsetValue() {
    StatusRuntimeException exception =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.put(PutRequest.newBuilder().setKey("broken").build()));

    assertEquals(Status.Code.INVALID_ARGUMENT, exception.getStatus().getCode());
  }

  private List<RangeEntry> collectRange(String since, String to) {
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

  private static NullableBinaryValue nullValue() {
    return NullableBinaryValue.newBuilder().setNullValue(Empty.getDefaultInstance()).build();
  }
}
