package optimize.exception;

// todo remove it when dev finished.
public class PartialKeyCheckException extends RuntimeException {
  public PartialKeyCheckException() {
    super("Partial Key not consistent.");
  }
}
