package io.weir.model;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Canonical form for row values, used by PK diff / content compare / sample hashing.
 *
 * <p>Without this, comparing a source-side {@code Instant} against a target-side {@code Timestamp}
 * (or {@code 1} against {@code 1.0}) reports every row as changed, which makes every diff pass
 * rewrite the whole table.
 */
public final class RowValues {
  private RowValues() {}

  /**
   * Canonicalise a value so that semantically equal values compare equal:
   * temporal types collapse to epoch millis, numbers to a stripped {@link BigDecimal}, everything
   * else to its trimmed string form.
   */
  public static Object normalize(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Instant i) {
      return i.toEpochMilli();
    }
    if (v instanceof Timestamp ts) {
      return ts.toInstant().toEpochMilli();
    }
    if (v instanceof java.sql.Date d) {
      return d.toLocalDate().toEpochDay();
    }
    if (v instanceof LocalDateTime ldt) {
      return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    if (v instanceof LocalDate ld) {
      return ld.toEpochDay();
    }
    if (v instanceof Boolean) {
      return v;
    }
    if (v instanceof Number n) {
      // 1, 1L, 1.0 and BigDecimal("1.00") must all land on the same canonical string.
      return canonicalNumber(n);
    }
    if (v instanceof byte[] b) {
      return Arrays.toString(b);
    }
    String s = String.valueOf(v);
    // Timestamps frequently arrive as strings ("2024-06-01 00:00:00.0" vs "2024-06-01T00:00:00Z").
    Instant parsed = tryInstant(s);
    if (parsed != null) {
      return parsed.toEpochMilli();
    }
    return s;
  }

  /**
   * One canonical string for every numeric width. Going through {@code toPlainString()} matters:
   * {@code stripTrailingZeros()} alone renders 100 as {@code 1E+2}, which would not compare equal to
   * the integer 100.
   */
  private static String canonicalNumber(Number n) {
    if (n instanceof Double d && (d.isNaN() || d.isInfinite())) {
      return String.valueOf(n);
    }
    if (n instanceof Float f && (f.isNaN() || f.isInfinite())) {
      return String.valueOf(n);
    }
    BigDecimal bd = n instanceof BigDecimal b ? b : new BigDecimal(n.toString());
    return bd.stripTrailingZeros().toPlainString();
  }

  private static Instant tryInstant(String s) {
    if (s.isBlank()) {
      return null;
    }
    try {
      return Instant.parse(s);
    } catch (DateTimeParseException ignored) {
      // fall through
    }
    try {
      return Timestamp.valueOf(s).toInstant();
    } catch (IllegalArgumentException ignored) {
      // fall through
    }
    try {
      return LocalDateTime.parse(s).toInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      return null;
    }
  }

  /** Whether two values are semantically equal under {@link #normalize}. */
  public static boolean equal(Object a, Object b) {
    Object na = normalize(a);
    Object nb = normalize(b);
    if (na == null || nb == null) {
      return na == nb;
    }
    if (na instanceof BigDecimal ba && nb instanceof BigDecimal bb) {
      return ba.compareTo(bb) == 0;
    }
    return na.equals(nb);
  }

  /**
   * Stable 64-bit hash over the non-key columns, used for content compare and fingerprinting.
   * Column order is fixed by {@code columns}; nulls hash as a sentinel.
   */
  public static long hash(DataRow row, List<String> columns) {
    long[] out = new long[2];
    for (String c : columns) {
      Object n = normalize(row.get(c));
      byte[] bytes = n == null ? new byte[] {0} : String.valueOf(n).getBytes(StandardCharsets.UTF_8);
      // FNV-1a 64-bit: no dependencies, stable across JVMs.
      long h = 0xcbf29ce484222325L;
      for (byte b : bytes) {
        h ^= (b & 0xff);
        h *= 0x100000001b3L;
      }
      out[0] = (out[0] * 31L) ^ h;
      out[1] += h;
    }
    return out[0] ^ (out[1] << 1);
  }

  /** Case-insensitive column lookup helper mirroring {@link DataRow#get}. */
  public static boolean hasColumn(DataRow row, String column) {
    if (row.values().containsKey(column)) {
      return true;
    }
    for (String k : row.values().keySet()) {
      if (k != null && k.equalsIgnoreCase(column)) {
        return true;
      }
    }
    return false;
  }

  /** Lower-cased view of a column list, for case-insensitive set membership. */
  public static boolean containsIgnoreCase(List<String> columns, String column) {
    for (String c : columns) {
      if (c != null && c.equalsIgnoreCase(column)) {
        return true;
      }
    }
    return false;
  }

  public static String lower(String s) {
    return s == null ? null : s.toLowerCase(Locale.ROOT);
  }
}
