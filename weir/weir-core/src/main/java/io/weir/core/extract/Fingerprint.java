package io.weir.core.extract;

import io.weir.model.DataRow;
import io.weir.model.RowValues;
import java.util.List;
import java.util.Map;

/**
 * Cheap summary of a set of rows: count plus a commutative sum of per-row hashes.
 *
 * <p>The design's "partition fingerprint" idea (§5.3): compare these first, and only drill into
 * row-level diffing for windows whose fingerprint actually moved. Without it, every diff pass over
 * a large table costs a full two-sided scan.
 */
public record Fingerprint(long count, long hashSum) {

  public static final Fingerprint EMPTY = new Fingerprint(0L, 0L);

  public static Fingerprint of(List<DataRow> rows, List<String> columns) {
    long sum = 0L;
    for (DataRow r : rows) {
      sum += RowValues.hash(r, hashColumns(r, columns));
    }
    return new Fingerprint(rows.size(), sum);
  }

  public static Fingerprint of(Map<String, DataRow> rows, List<String> columns) {
    long sum = 0L;
    for (DataRow r : rows.values()) {
      sum += RowValues.hash(r, hashColumns(r, columns));
    }
    return new Fingerprint(rows.size(), sum);
  }

  /** Hash every column present in the row (fallback when no projection is configured). */
  private static List<String> hashColumns(DataRow row, List<String> configured) {
    if (configured != null && !configured.isEmpty()) {
      return configured;
    }
    return List.copyOf(row.values().keySet());
  }

  public boolean matches(Fingerprint other) {
    return other != null && count == other.count && hashSum == other.hashSum;
  }

  /** True when a drill-down is worth the cost. */
  public boolean differsFrom(Fingerprint other) {
    return !matches(other);
  }

  @Override
  public String toString() {
    return "Fingerprint{count=" + count + ", hashSum=" + hashSum + '}';
  }
}
