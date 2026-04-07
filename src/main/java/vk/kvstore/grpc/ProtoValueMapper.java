package vk.kvstore.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import lombok.experimental.UtilityClass;
import vk.kvstore.proto.NullableBinaryValue;
import vk.kvstore.service.InvalidRequestException;

@UtilityClass
public class ProtoValueMapper {

  public NullableBinaryValue toProto(byte[] value) {
    NullableBinaryValue.Builder builder = NullableBinaryValue.newBuilder();
    if (value == null) {
      builder.setNullValue(Empty.getDefaultInstance());
    } else {
      builder.setData(ByteString.copyFrom(value));
    }
    return builder.build();
  }

  public byte[] fromProto(NullableBinaryValue value) {
    return switch (value.getKindCase()) {
      case DATA -> value.getData().toByteArray();
      case NULL_VALUE -> null;
      case KIND_NOT_SET ->
          throw new InvalidRequestException("value must be set explicitly to bytes or null");
    };
  }
}
