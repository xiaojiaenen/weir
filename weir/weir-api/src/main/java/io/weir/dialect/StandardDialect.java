package io.weir.dialect;

import io.weir.model.IncrementalStrategy;
import io.weir.model.SplitRange;
import io.weir.model.Watermark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ANSI-leaning dialect: double-quoted identifiers, {@code LIMIT} pagination, {@code MOD()} sharding
 * and an INSERT-only sink. Vendors override what differs.
 */
public class StandardDialect implements JdbcDialect {
  private final String name;

  public StandardDialect(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  public String buildSelect(
      List<String> columns, String fromClause, String where, String orderBy, Integer limit) {
    String cols =
        columns.isEmpty()
            ? "*"
            : columns.stream().map(this::quote).collect(Collectors.joining(", "));
    StringBuilder sb = new StringBuilder("SELECT ").append(cols).append(" FROM ").append(fromClause);
    if (where != null && !where.isBlank()) {
      sb.append(" WHERE ").append(where);
    }
    if (orderBy != null && !orderBy.isBlank()) {
      sb.append(" ORDER BY ").append(orderBy);
    }
    if (limit != null && limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }
    return sb.toString();
  }

  @Override
  public String buildIncrementWhere(
      IncrementalStrategy strategy,
      Watermark watermark,
      String tsColumn,
      String idColumn,
      Duration overlap) {
    if (strategy == null || strategy == IncrementalStrategy.FULL) {
      return "";
    }
    return switch (strategy) {
      case ID -> {
        Long id = watermark == null ? null : watermark.id();
        if (id == null) {
          yield "";
        }
        yield quote(idColumn) + " > " + id;
      }
      case UPDATE_TIME -> {
        Instant ts = watermark == null ? null : watermark.ts();
        if (ts == null) {
          yield "";
        }
        // Overlap re-reads a safety window so rows committed just before the cursor are not lost.
        Instant from = ts.minus(overlap == null ? Duration.ZERO : overlap);
        yield quote(tsColumn) + " >= " + tsLiteral(from);
      }
      case UPDATE_TIME_ID -> {
        Instant ts = watermark == null ? null : watermark.ts();
        Long id = watermark == null ? null : watermark.id();
        if (ts == null) {
          yield id == null ? "" : quote(idColumn) + " > " + id;
        }
        Instant from = ts.minus(overlap == null ? Duration.ZERO : overlap);
        String qTs = quote(tsColumn);
        String qId = quote(idColumn);
        yield "("
            + qTs
            + " > "
            + tsLiteral(from)
            + " OR ("
            + qTs
            + " = "
            + tsLiteral(ts)
            + " AND "
            + qId
            + " > "
            + (id == null ? -1L : id)
            + "))";
      }
      default -> "";
    };
  }

  @Override
  public String buildSplitWhere(SplitRange range, String splitColumn) {
    if (range == null) {
      return "";
    }
    List<String> parts = new ArrayList<>();
    String col = quote(splitColumn);
    if (range.lo() != null) {
      parts.add(col + " >= " + range.lo());
    }
    if (range.hi() != null) {
      parts.add(col + " < " + range.hi());
    }
    return String.join(" AND ", parts);
  }

  @Override
  public String buildModSplitWhere(String splitColumn, int modulus, int remainder) {
    return "MOD(" + quote(splitColumn) + ", " + modulus + ") = " + remainder;
  }

  @Override
  public String buildHashSplitWhere(String splitColumn, int modulus, int remainder) {
    throw new UnsupportedOperationException(
        name() + " cannot hash-partition a non-numeric split column; use one shard or a numeric PK");
  }

  /**
   * Probe MIN/MAX. Returns {@code {Long.MIN_VALUE, Long.MIN_VALUE}} when the split column is not
   * numeric — the planner must fall back to {@code MOD} sharding in that case.
   */
  @Override
  public long[] minMax(Connection conn, String fromClause, String splitColumn) throws SQLException {
    String sql =
        "SELECT MIN("
            + quote(splitColumn)
            + "), MAX("
            + quote(splitColumn)
            + ") FROM "
            + fromClause;
    try (PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      if (rs.next()) {
        Object lo = rs.getObject(1);
        Object hi = rs.getObject(2);
        if (lo == null || hi == null) {
          return new long[] {0L, 0L};
        }
        if (!(lo instanceof Number) || !(hi instanceof Number)) {
          return new long[] {Long.MIN_VALUE, Long.MIN_VALUE};
        }
        return new long[] {((Number) lo).longValue(), ((Number) hi).longValue()};
      }
    }
    return new long[] {0L, 0L};
  }

  @Override
  public boolean supportsMerge() {
    return false;
  }

  @Override
  public String buildMerge(String targetTable, List<String> pk, List<String> allColumns) {
    throw new UnsupportedOperationException(name() + " has no native upsert; use writeMode=append");
  }

  @Override
  public String buildInsert(String targetTable, List<String> allColumns) {
    String cols = allColumns.stream().map(this::quote).collect(Collectors.joining(", "));
    String placeholders = allColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    return "INSERT INTO " + targetTable + " (" + cols + ") VALUES (" + placeholders + ")";
  }

  @Override
  public String buildDelete(String targetTable, List<String> pk) {
    return "DELETE FROM "
        + targetTable
        + " WHERE "
        + pk.stream().map(c -> quote(c) + " = ?").collect(Collectors.joining(" AND "));
  }

  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD COLUMN " + quote(column) + " " + sqlType;
  }

  @Override
  public String tsLiteral(Instant instant) {
    // Match JDBC Timestamp string form (local zone) so comparisons line up with stored values.
    return "'" + java.sql.Timestamp.from(instant) + "'";
  }
}
