package io.weir.api;

/** Checked failure with optional SQL state for diagnostics. */
public class WeirException extends RuntimeException {
  public WeirException(String message) {
    super(message);
  }

  public WeirException(String message, Throwable cause) {
    super(message, cause);
  }
}
