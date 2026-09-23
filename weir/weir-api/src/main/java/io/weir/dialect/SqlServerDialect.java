package io.weir.dialect;

import java.util.List;
import java.util.stream.Collectors;

/** SQL Server: bracketed identifiers, {@code TOP} pagination, {@code MERGE}, {@code CHECKSUM}. */
public class SqlServerDialect extends StandardDialect {
  public SqlServerDialect() {
    super("sqlserver");
  }

  @Override
  public String quote(String identifier) {
    return "[" + identifier.replace("]", "]]") + "]";
  }

  @Override
  public boolean supportsMerge() {
    return true;
  }

  @Override
  public boolean limitBeforeProjection() {
    return true;
  }

  @Override
  public String buildSelect(
      List<String> columns, String fromClause, String where, String orderBy, Integer limit) {
    String cols =
        columns.isEmpty()
            ? "*"
            : columns.stream().map(this::quote).collect(Collectors.joining(", "));
    StringBuilder sb = new StringBuilder("SELECT ");
    if (limit != null && limit > 0) {
      sb.append("TOP (").append(limit).append(") ");
    }
    sb.append(cols).append(" FROM ").append(fromClause);
    if (where != null && !where.isBlank()) {
      sb.append(" WHERE ").append(where);
    }
    if (orderBy != null && !orderBy.isBlank()) {
      sb.append(" ORDER BY ").append(orderBy);
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
        + " AS t USING (SELECT "
        + placeholders
        + ") AS s ("
        + cols
        + ") ON "
        + onCond
        + " WHEN MATCHED THEN UPDATE SET "
        + setClause
        + " WHEN NOT MATCHED THEN INSERT ("
        + cols
        + ") VALUES ("
        + allColumns.stream().map(c -> "s." + quote(c)).collect(Collectors.joining(", "))
        + ");";
  }

  @Override
  public String buildModSplitWhere(String splitColumn, int modulus, int remainder) {
    return "(" + quote(splitColumn) + " % " + modulus + ") = " + remainder;
  }

  @Override
  public boolean supportsHashSplit() {
    return true;
  }

  @Override
  public String buildHashSplitWhere(String splitColumn, int modulus, int remainder) {
    return "ABS(CHECKSUM(" + quote(splitColumn) + ")) % " + modulus + " = " + remainder;
  }

  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD " + quote(column) + " " + sqlType;
  }

  /** SQL Server prefers an ODBC-style literal for datetime2 comparisons. */
  @Override
  public String tsLiteral(java.time.Instant instant) {
    return "'" + java.sql.Timestamp.from(instant) + "'";
  }
}
