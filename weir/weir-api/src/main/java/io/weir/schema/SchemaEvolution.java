package io.weir.schema;

import io.weir.dialect.JdbcDialect;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Detects columns present on the source but missing on a JDBC target, and adds them.
 *
 * <p>Only additive evolution is automatic: a dropped column or a narrowing type change is reported
 * but never applied silently.
 */
public final class SchemaEvolution {
  private SchemaEvolution() {}

  /** Columns to add, with the SQL type inferred from a sample value. */
  public record Plan(List<String> missingColumns, Map<String, String> sqlTypes) {}

  public static Plan planAddColumns(
      Connection targetConn, String targetTable, Map<String, Object> sampleRow) {
    Map<String, String> existing = existingColumns(targetConn, targetTable);
    List<String> missing = new ArrayList<>();
    Map<String, String> types = new LinkedHashMap<>();
    for (var e : sampleRow.entrySet()) {
      String col = e.getKey();
      if (col == null || "_op".equals(col)) {
        continue;
      }
      if (existing.containsKey(col.toLowerCase(Locale.ROOT))) {
        continue;
      }
      missing.add(col);
      types.put(col, sqlTypeOf(e.getValue()));
    }
    return new Plan(missing, types);
  }

  /** Applies the plan using vendor-specific DDL. Returns the columns actually added. */
  public static List<String> applyAddColumns(
      Connection targetConn, String targetTable, Plan plan, JdbcDialect dialect)
      throws SQLException {
    List<String> added = new ArrayList<>();
    for (String col : plan.missingColumns()) {
      String type = plan.sqlTypes().getOrDefault(col, "VARCHAR(1024)");
      String sql = dialect.alterAddColumn(targetTable, col, type);
      try (PreparedStatement ps = targetConn.prepareStatement(sql)) {
        ps.execute();
        added.add(col);
      } catch (SQLException e) {
        String msg = String.valueOf(e.getMessage());
        if (msg.contains("Duplicate column")
            || msg.contains("duplicate column name")
            || msg.contains("already exists")) {
          continue;
        }
        throw e;
      }
    }
    return added;
  }

  private static Map<String, String> existingColumns(Connection conn, String table) {
    Map<String, String> map = new LinkedHashMap<>();
    try {
      DatabaseMetaData md = conn.getMetaData();
      String simple = table;
      int dot = table.lastIndexOf('.');
      if (dot > 0) {
        simple = table.substring(dot + 1);
      }
      for (String pattern :
          new String[] {simple, simple.toUpperCase(Locale.ROOT), simple.toLowerCase(Locale.ROOT)}) {
        try (ResultSet rs = md.getColumns(null, null, pattern, null)) {
          while (rs.next()) {
            String col = rs.getString("COLUMN_NAME");
            map.put(col.toLowerCase(Locale.ROOT), rs.getString("TYPE_NAME"));
          }
        }
        if (!map.isEmpty()) {
          break;
        }
      }
    } catch (SQLException ignored) {
      // fall through — ALTER will ignore duplicates
    }
    return map;
  }

  private static String sqlTypeOf(Object v) {
    if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
      return "INT";
    }
    if (v instanceof Long) {
      return "BIGINT";
    }
    if (v instanceof Double || v instanceof Float) {
      return "DOUBLE";
    }
    if (v instanceof java.math.BigDecimal) {
      return "DECIMAL(38,10)";
    }
    if (v instanceof Boolean) {
      return "BOOLEAN";
    }
    if (v instanceof java.time.Instant || v instanceof java.sql.Timestamp) {
      return "TIMESTAMP";
    }
    return "VARCHAR(1024)";
  }
}
