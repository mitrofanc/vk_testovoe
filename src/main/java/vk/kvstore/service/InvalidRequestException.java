package vk.kvstore.service;

public class InvalidRequestException extends RuntimeException {

  public InvalidRequestException(String message) {
    super(message);
  }
}
