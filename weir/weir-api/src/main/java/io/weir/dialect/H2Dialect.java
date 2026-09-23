package io.weir.dialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * H2 — used for tests and local demos. Identifiers are left unquoted because H2 folds unquoted
 * names to upper case, so quoting configured lower-case names would break against tables created
 * the usual way.
 */
public class H2Dialect extends StandardDialect {
  public H2Dialect() {
    super("h2");
  }

  @Override
  public String quote(String identifier) {
    return identifier;
  }

  @Override
  public boolean supportsMerge() {
    return true;
  }

  @Override
  public String buildMerge(String targetTable, List<String> pk, List<String> allColumns) {
    String cols = allColumns.stream().map(this::quote).collect(Collectors.joining(", "));
    String placeholders = allColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    String key = pk.stream().map(this::quote).collect(Collectors.joining(", "));
    return "MERGE INTO "
        + targetTable
        + " ("
        + cols
        + ") KEY("
        + key
        + ") VALUES ("
        + placeholders
        + ")";
  }

  @Override
  public String buildDelete(String targetTable, List<String> pk) {
    return "DELETE FROM "
        + targetTable
        + " WHERE "
        + pk.stream().map(c -> c + " = ?").collect(Collectors.joining(" AND "));
  }

  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD COLUMN " + column + " " + sqlType;
  }
}
