package io.weir.model;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * How a full snapshot (or a big catch-up) is cut into shards.
 *
 * <ul>
 *   <li>{@link #AUTO} — numeric half-open ranges when the split column is numeric, otherwise
 *       modulus sharding. This is the safe default.
 *   <li>{@link #RANGE} — {@code [lo, hi)} ranges from MIN/MAX. Requires a numeric split column.
 *   <li>{@link #MOD} — {@code MOD(col, n) = r}. Best for sparse numeric ids where uniform ranges
 *       would skew badly.
 *   <li>{@link #HASH} — vendor hash of the column, then mod. Works for string / UUID keys.
 *   <li>{@link #NONE} — single connection, no parallelism.
 * </ul>
 */
public enum SplitMode {
  AUTO,
  RANGE,
  MOD,
  HASH,
  NONE;

  @JsonCreator
  public static SplitMode from(String value) {
    if (value == null || value.isBlank()) {
      return AUTO;
    }
    return SplitMode.valueOf(value.trim().toUpperCase());
  }
}
