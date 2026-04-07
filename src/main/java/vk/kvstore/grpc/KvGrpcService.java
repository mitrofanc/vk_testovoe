package vk.kvstore.grpc;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.proto.CountRequest;
import vk.kvstore.proto.CountResponse;
import vk.kvstore.proto.DeleteRequest;
import vk.kvstore.proto.DeleteResponse;
import vk.kvstore.proto.GetRequest;
import vk.kvstore.proto.GetResponse;
import vk.kvstore.proto.KvStoreGrpc;
import vk.kvstore.proto.PutRequest;
import vk.kvstore.proto.PutResponse;
import vk.kvstore.proto.RangeEntry;
import vk.kvstore.proto.RangeRequest;
import vk.kvstore.service.KeyValueService;

public final class KvGrpcService extends KvStoreGrpc.KvStoreImplBase {

  private final KeyValueService keyValueService;

  public KvGrpcService(KeyValueService keyValueService) {
    this.keyValueService = keyValueService;
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      keyValueService.put(request.getKey(), ProtoValueMapper.fromProto(request.getValue()));
      responseObserver.onNext(PutResponse.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      responseObserver.onError(GrpcExceptionMapper.toStatusRuntimeException(exception));
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    try {
      Optional<KeyValueEntry> entry = keyValueService.get(request.getKey());
      GetResponse.Builder response = GetResponse.newBuilder().setFound(entry.isPresent());
      entry.ifPresent(value -> response.setValue(ProtoValueMapper.toProto(value.value())));
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      responseObserver.onError(GrpcExceptionMapper.toStatusRuntimeException(exception));
    }
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    try {
      boolean deleted = keyValueService.delete(request.getKey());
      responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      responseObserver.onError(GrpcExceptionMapper.toStatusRuntimeException(exception));
    }
  }

  @Override
  public void range(RangeRequest request, StreamObserver<RangeEntry> responseObserver) {
    ServerCallStreamObserver<RangeEntry> serverObserver =
        responseObserver instanceof ServerCallStreamObserver<RangeEntry> observer ? observer : null;
    try {
      keyValueService.range(
          request.getKeySince(),
          request.getKeyTo(),
          entry -> {
            if (serverObserver != null && serverObserver.isCancelled()) {
              throw new StreamCancelledException();
            }
            responseObserver.onNext(
                RangeEntry.newBuilder()
                    .setKey(entry.key())
                    .setValue(ProtoValueMapper.toProto(entry.value()))
                    .build());
          });
      if (serverObserver == null || !serverObserver.isCancelled()) {
        responseObserver.onCompleted();
      }
    } catch (StreamCancelledException ignored) {
      // Client cancelled the stream; stop without sending extra frames.
    } catch (Exception exception) {
      responseObserver.onError(GrpcExceptionMapper.toStatusRuntimeException(exception));
    }
  }

  @Override
  public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
    try {
      responseObserver.onNext(
          CountResponse.newBuilder().setCount(keyValueService.count()).build());
      responseObserver.onCompleted();
    } catch (Exception exception) {
      responseObserver.onError(GrpcExceptionMapper.toStatusRuntimeException(exception));
    }
  }

  private static final class StreamCancelledException extends RuntimeException {}
}
