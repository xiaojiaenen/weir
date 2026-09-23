package io.weir.core.diff;

import io.weir.model.DataRow;
import io.weir.model.RowValues;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Git-style reconciliation between a source snapshot and the current target contents:
 * keys only in source → insert, keys only in target → delete, keys in both with different content →
 * rewrite.
 *
 * <p>Comparisons go through {@link RowValues#normalize} so that a source {@code Instant} and a
 * target {@code Timestamp} holding the same moment are not reported as a change.
 */
public final class PkDiff {

  /** Outcome of one reconciliation pass. */
  public record Result(
      List<Object[]> sourceOnly,
      List<Object[]> targetOnly,
      List<Object[]> changed,
      long unchanged) {}

  private PkDiff() {}

  public static Result diffKeys(List<Object[]> sourceKeys, List<Object[]> targetKeys) {
    Map<String, Object[]> src = index(sourceKeys);
    Map<String, Object[]> tgt = index(targetKeys);
    List<Object[]> sourceOnly = new ArrayList<>();
    List<Object[]> targetOnly = new ArrayList<>();
    long unchanged = 0;
    for (var e : src.entrySet()) {
      if (!tgt.containsKey(e.getKey())) {
        sourceOnly.add(e.getValue());
      } else {
        unchanged++;
      }
    }
    for (var e : tgt.entrySet()) {
      if (!src.containsKey(e.getKey())) {
        targetOnly.add(e.getValue());
      }
    }
    return new Result(sourceOnly, targetOnly, new ArrayList<>(), unchanged);
  }

  /**
   * True when any non-key column differs.
   *
   * <p>Columns missing on either side are compared as null, which is correct for additive schema
   * evolution but means a target that was never loaded with a column will be rewritten once — the
   * honest behaviour.
   */
  public static boolean rowDiffers(
      DataRow s, DataRow t, List<String> pk, List<String> compareCols) {
    if (t == null) {
      return true;
    }
    for (String c : compareCols) {
      if (RowValues.containsIgnoreCase(pk, c)) {
        continue;
      }
      if (!RowValues.equal(s.get(c), t.get(c))) {
        return true;
      }
    }
    return false;
  }

  /** Columns worth comparing: everything on either side except the key and the op marker. */
  public static List<String> compareColumns(DataRow sourceSample, DataRow targetSample) {
    Map<String, Object> merged = new LinkedHashMap<>();
    if (targetSample != null) {
      merged.putAll(targetSample.values());
    }
    if (sourceSample != null) {
      merged.putAll(sourceSample.values());
    }
    merged.remove("_op");
    return List.copyOf(merged.keySet());
  }

  public static String keyOf(DataRow row, List<String> pk) {
    StringBuilder sb = new StringBuilder();
    for (String c : pk) {
      sb.append(canonical(row.get(c))).append('\u0000');
    }
    return sb.toString();
  }

  public static String keyOf(Object[] key) {
    if (key.length == 1) {
      return canonical(key[0]);
    }
    StringBuilder sb = new StringBuilder();
    for (Object o : key) {
      sb.append(canonical(o)).append('|');
    }
    return sb.toString();
  }

  /**
   * Key string built from raw values. Uses the canonical form so that {@code 1} and {@code 1L}
   * select the same row — previously the two rendered differently and produced phantom diffs.
   */
  private static String canonical(Object v) {
    Object n = RowValues.normalize(v);
    return n == null ? "\u0001NULL" : String.valueOf(n);
  }

  public static Object[] pkValues(DataRow row, List<String> pk) {
    Object[] out = new Object[pk.size()];
    for (int i = 0; i < pk.size(); i++) {
      out[i] = row.get(pk.get(i));
    }
    return out;
  }

  /** Delete marker row: only the key columns plus {@code _op=d}. */
  public static DataRow deleteMarker(Object[] key, List<String> pk) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < pk.size(); i++) {
      map.put(pk.get(i), key[i]);
    }
    map.put("_op", "d");
    return new DataRow(map);
  }

  public static boolean isDeleted(DataRow row) {
    if (row == null) {
      return false;
    }
    Object op = row.get("_op");
    if (op == null) {
      return false;
    }
    String s = String.valueOf(op);
    return "d".equalsIgnoreCase(s) || "delete".equalsIgnoreCase(s);
  }

  /**
   * Whether a soft-delete column says this row is gone.
   *
   * <p>When {@code trueValues} is configured it wins outright — a column like {@code amount} is
   * numeric but is not a flag, and treating "any non-zero number" as deleted would wipe the table.
   * The numeric fallback only applies when the operator configured no explicit values.
   */
  public static boolean isSoftDeleted(Object flag, List<String> trueValues) {
    if (flag == null) {
      return false;
    }
    String s = String.valueOf(flag).trim();
    if (trueValues != null && !trueValues.isEmpty()) {
      for (String tv : trueValues) {
        if (tv != null && s.equalsIgnoreCase(tv.trim())) {
          return true;
        }
      }
      return false;
    }
    if (flag instanceof Boolean b) {
      return b;
    }
    if (flag instanceof Number n) {
      return n.longValue() != 0L;
    }
    return "1".equals(s) || "true".equalsIgnoreCase(s) || "Y".equalsIgnoreCase(s);
  }

  private static Map<String, Object[]> index(List<Object[]> keys) {
    Map<String, Object[]> map = new LinkedHashMap<>();
    for (Object[] k : keys) {
      map.put(keyOf(k), k);
    }
    return map;
  }

  /** Index rows by key; later rows win, which is the keep-latest behaviour we want. */
  public static Map<String, DataRow> indexRows(Iterable<DataRow> rows, List<String> pk) {
    Map<String, DataRow> map = new LinkedHashMap<>();
    for (DataRow r : rows) {
      map.put(keyOf(r, pk), r);
    }
    return map;
  }

  /** Convenience for tests / callers that only need equality of a single column pair. */
  public static boolean sameKey(Object a, Object b) {
    return Objects.equals(RowValues.normalize(a), RowValues.normalize(b));
  }
}
