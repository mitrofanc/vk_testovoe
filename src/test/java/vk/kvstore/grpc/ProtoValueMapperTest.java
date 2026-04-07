package vk.kvstore.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.Test;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.service.InvalidRequestException;

class ProtoValueMapperTest {

  @Test
  void toProtoMapsNullToExplicitNullValue() {
    NullableBinaryValue value = ProtoValueMapper.toProto(null);

    assertEquals(NullableBinaryValue.KindCase.NULL_VALUE, value.getKindCase());
  }

  @Test
  void toProtoPreservesEmptyBinaryPayload() {
    NullableBinaryValue value = ProtoValueMapper.toProto(new byte[0]);

    assertEquals(NullableBinaryValue.KindCase.DATA, value.getKindCase());
    assertEquals(0, value.getData().size());
  }

  @Test
  void fromProtoReturnsBinaryPayload() {
    byte[] expected = new byte[] {0, 1, 2, 3};
    NullableBinaryValue value =
        NullableBinaryValue.newBuilder().setData(ByteString.copyFrom(expected)).build();

    byte[] actual = ProtoValueMapper.fromProto(value);

    assertArrayEquals(expected, actual);
  }

  @Test
  void fromProtoReturnsNullForExplicitNullValue() {
    NullableBinaryValue value =
        NullableBinaryValue.newBuilder().setNullValue(Empty.getDefaultInstance()).build();

    assertNull(ProtoValueMapper.fromProto(value));
  }

  @Test
  void fromProtoRejectsUnsetValue() {
    InvalidRequestException exception =
        assertThrows(
            InvalidRequestException.class,
            () -> ProtoValueMapper.fromProto(NullableBinaryValue.getDefaultInstance()));

    assertEquals("value must be set explicitly to bytes or null", exception.getMessage());
  }
}
