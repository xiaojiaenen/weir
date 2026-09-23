package io.weir.dialect;

import java.util.List;
import java.util.stream.Collectors;

/** Oracle: {@code MERGE INTO} upsert, {@code FETCH FIRST} pagination, {@code ORA_HASH} sharding. */
public class OracleDialect extends StandardDialect {
  public OracleDialect() {
    super("oracle");
  }

  @Override
  public boolean supportsMerge() {
    return true;
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
      sb.append(" FETCH FIRST ").append(limit).append(" ROWS ONLY");
    }
    return sb.toString();
  }

  @Override
  public String buildMerge(String targetTable, List<String> pk, List<String> allColumns) {
    String cols = allColumns.stream().map(this::quote).collect(Collectors.joining(", "));
    String placeholders = allColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    String onCond =
        pk.stream()
            .map(c -> "t." + quote(c) + " = s." + quote(c))
            .collect(Collectors.joining(" AND "));
    List<String> updates =
        allColumns.stream()
            .filter(c -> pk.stream().noneMatch(p -> p.equalsIgnoreCase(c)))
            .map(c -> quote(c) + " = s." + quote(c))
            .toList();
    String setClause =
        updates.isEmpty()
            ? quote(pk.get(0)) + " = s." + quote(pk.get(0))
            : String.join(", ", updates);
    return "MERGE INTO "
        + targetTable
        + " t USING (SELECT "
        + placeholders
        + " FROM dual) s("
        + cols
        + ") ON "
        + onCond
        + " WHEN MATCHED THEN UPDATE SET "
        + setClause
        + " WHEN NOT MATCHED THEN INSERT ("
        + cols
        + ") VALUES ("
        + allColumns.stream().map(c -> "s." + quote(c)).collect(Collectors.joining(", "))
        + ")";
  }

  @Override
  public boolean supportsHashSplit() {
    return true;
  }

  @Override
  public String buildHashSplitWhere(String splitColumn, int modulus, int remainder) {
    // ORA_HASH(expr, max_bucket) returns 0..max_bucket inclusive.
    return "ORA_HASH(" + quote(splitColumn) + ", " + (modulus - 1) + ") = " + remainder;
  }

  /** Oracle requires parenthesised column definitions on ALTER ... ADD. */
  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD (" + quote(column) + " " + sqlType + ")";
  }

  /** Oracle has no TIMESTAMP literal keyword; use an explicit conversion. */
  @Override
  public String tsLiteral(java.time.Instant instant) {
    return "TIMESTAMP '" + java.sql.Timestamp.from(instant) + "'";
  }
}
