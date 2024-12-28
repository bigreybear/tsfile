package optimize.exception;

import java.nio.charset.StandardCharsets;

// todo remove it when dev finished.
public class KeyNotFound extends RuntimeException {
  public KeyNotFound(byte[] k) {
    super("Key not found: " + new String(k, StandardCharsets.UTF_8));
  }
}
