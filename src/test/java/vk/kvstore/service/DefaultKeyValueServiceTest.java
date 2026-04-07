package vk.kvstore.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import vk.kvstore.model.KeyValueEntry;
import vk.kvstore.support.InMemoryKeyValueRepository;

class DefaultKeyValueServiceTest {

  private DefaultKeyValueService service;

  @BeforeEach
  void setUp() {
    service = new DefaultKeyValueService(new InMemoryKeyValueRepository());
  }

  @Test
  void putRejectsNullKey() {
    InvalidRequestException exception =
        assertThrows(InvalidRequestException.class, () -> service.put(null, new byte[] {1}));

    assertEquals("key must not be null", exception.getMessage());
  }

  @Test
  void getRejectsNullKey() {
    InvalidRequestException exception =
        assertThrows(InvalidRequestException.class, () -> service.get(null));

    assertEquals("key must not be null", exception.getMessage());
  }

  @Test
  void deleteRejectsNullKey() {
    InvalidRequestException exception =
        assertThrows(InvalidRequestException.class, () -> service.delete(null));

    assertEquals("key must not be null", exception.getMessage());
  }

  @Test
  void rangeRejectsNullBounds() {
    InvalidRequestException lowerBoundException =
        assertThrows(InvalidRequestException.class, () -> service.range(null, "b", entry -> {}));
    InvalidRequestException upperBoundException =
        assertThrows(InvalidRequestException.class, () -> service.range("a", null, entry -> {}));

    assertEquals("keySince must not be null", lowerBoundException.getMessage());
    assertEquals("keyTo must not be null", upperBoundException.getMessage());
  }

  @Test
  void emptyKeyIsAcceptedAndRoundTrips() {
    byte[] value = "root".getBytes(StandardCharsets.UTF_8);

    service.put("", value);
    Optional<KeyValueEntry> entry = service.get("");
    boolean deleted = service.delete("");

    assertTrue(entry.isPresent());
    assertEquals("", entry.get().key());
    assertArrayEquals(value, entry.get().value());
    assertTrue(deleted);
    assertEquals(0L, service.count());
  }
}
