package io.weir.core.diff;

import io.weir.config.JobConfig;
import io.weir.core.util.Jdbc;
import io.weir.core.util.Sql;
import io.weir.dialect.Dialects;
import io.weir.dialect.JdbcDialect;
import io.weir.model.DataRow;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the current contents of a JDBC target so a diff pass can compare against the source.
 *
 * <p>The projection is passed in by the caller: a set-diff needs only the key columns, while a
 * content diff needs every comparable column. Reading only the key columns here and then comparing
 * full rows is what made every previous diff pass report every row as changed.
 */
public final class TargetReader {
  private TargetReader() {}

  /**
   * Read rows from the target table.
   *
   * @param columns columns to select; empty means {@code SELECT *}
   * @return rows keyed by {@link PkDiff#keyOf}
   */
  public static Map<String, DataRow> read(
      JobConfig config, List<String> pkColumns, List<String> columns) throws SQLException {
    if (!"jdbc".equalsIgnoreCase(config.target.type) || config.target.url == null) {
      return Map.of();
    }
    JdbcDialect dialect = Dialects.forUrl(config.target.url);
    String where = null;
    String sql =
        dialect.buildSelect(
            columns.isEmpty() ? List.of() : columns.stream().map(dialect::quote).toList(),
            config.target.table,
            where,
            null,
            null);
    Map<String, DataRow> out = new LinkedHashMap<>();
    try (Connection conn = Jdbc.open(config.target.url, config.target.user, config.target.password);
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData md = rs.getMetaData();
      int n = md.getColumnCount();
      while (rs.next()) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 1; i <= n; i++) {
          map.put(md.getColumnLabel(i), rs.getObject(i));
        }
        DataRow row = new DataRow(map);
        out.put(PkDiff.keyOf(row, pkColumns), row);
      }
    }
    return out;
  }

  /**
   * Stream the target table in primary-key order using keyset pagination
   * ({@code WHERE pk > ? ORDER BY pk LIMIT n}). Memory stays O(batchRows) no matter how large the
   * table is — this is what lets a diff pass reconcile an arbitrarily big table.
   *
   * <p>Single-column keys only: a composite keyset predicate is dialect-specific and the legacy
   * full-scan path remains available for that case.
   *
   * @param columns columns to select; empty means {@code SELECT *}
   */
  public static void streamByPk(
      JobConfig config,
      String pkColumn,
      List<String> columns,
      int batchRows,
      java.util.function.Consumer<List<DataRow>> batches)
      throws SQLException {
    if (!canRead(config)) {
      throw new SQLException("streamByPk requires a readable jdbc target");
    }
    JdbcDialect dialect = Dialects.forUrl(config.target.url);
    String pk = dialect.quote(pkColumn);
    List<String> proj = columns.isEmpty() ? List.of() : columns.stream().map(dialect::quote).toList();
    int limit = Math.max(1, batchRows);
    Object lastKey = null;
    boolean firstPage = true;
    while (true) {
      String where =
          firstPage ? null : pk + " > ?";
      String sql = dialect.buildSelect(proj, config.target.table, where, pk, limit);
      List<DataRow> page = new ArrayList<>();
      try (Connection conn = Jdbc.open(config.target.url, config.target.user, config.target.password);
          PreparedStatement ps = conn.prepareStatement(sql)) {
        if (!firstPage) {
          ps.setObject(1, lastKey);
        }
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData md = rs.getMetaData();
          int n = md.getColumnCount();
          while (rs.next()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 1; i <= n; i++) {
              map.put(md.getColumnLabel(i), rs.getObject(i));
            }
            page.add(new DataRow(map));
          }
        }
      }
      if (page.isEmpty()) {
        return;
      }
      batches.accept(page);
      lastKey = page.get(page.size() - 1).get(pkColumn);
      firstPage = false;
      if (page.size() < limit) {
        return;
      }
    }
  }

  /** COUNT(*) on the target — the cheapest possible reconciliation. */
  public static long count(JobConfig config) throws SQLException {
    if (!"jdbc".equalsIgnoreCase(config.target.type) || config.target.url == null) {
      return -1L;
    }
    String sql = "SELECT COUNT(*) FROM " + config.target.table;
    try (Connection conn = Jdbc.open(config.target.url, config.target.user, config.target.password);
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      return rs.next() ? rs.getLong(1) : 0L;
    }
  }

  /**
   * Whether this target can be scanned back. Set-based diffing is only meaningful when it can —
   * against a write-only sink (file, Kafka, Doris) an "empty target" is indistinguishable from
   * "target we cannot read", and treating it as empty would re-insert the entire source.
   */
  public static boolean canRead(JobConfig config) {
    return "jdbc".equalsIgnoreCase(config.target.type)
        && config.target.url != null
        && !config.target.url.isBlank();
  }

  /** Whether the target table exists and is readable — used by preflight. */
  public static boolean reachable(JobConfig config) {
    try {
      count(config);
      return true;
    } catch (SQLException e) {
      return false;
    }
  }

  /** Build the protected {@code SELECT *} used when no projection is configured. */
  public static String selectAllSql(JobConfig config) {
    JdbcDialect dialect = Dialects.forUrl(config.target.url);
    return dialect.buildSelect(List.of(), config.target.table, Sql.and(List.of()), null, null);
  }
}
