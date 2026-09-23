package io.weir.core.util;

import java.time.Duration;

public final class Durations {
  private Durations() {}

  /** Accepts ISO-8601 (PT5M) or simple forms: 5m, 30s, 2h, 1d. */
  public static Duration parse(String text) {
    if (text == null || text.isBlank()) {
      return Duration.ZERO;
    }
    String s = text.trim();
    try {
      return Duration.parse(s);
    } catch (Exception ignored) {
      // fall through
    }
    String lower = s.toLowerCase();
    try {
      if (lower.endsWith("ms")) {
        return Duration.ofMillis(Long.parseLong(lower.substring(0, lower.length() - 2).trim()));
      }
      if (lower.endsWith("s")) {
        return Duration.ofSeconds(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
      if (lower.endsWith("m")) {
        return Duration.ofMinutes(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
      if (lower.endsWith("h")) {
        return Duration.ofHours(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
      if (lower.endsWith("d")) {
        return Duration.ofDays(Long.parseLong(lower.substring(0, lower.length() - 1).trim()));
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid duration: " + text, e);
    }
    throw new IllegalArgumentException("Invalid duration: " + text);
  }
}
