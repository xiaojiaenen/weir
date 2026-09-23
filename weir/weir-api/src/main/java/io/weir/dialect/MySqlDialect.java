package io.weir.dialect;

import java.util.List;
import java.util.stream.Collectors;

/** MySQL / MariaDB: backticks, {@code INSERT ... ON DUPLICATE KEY UPDATE} upsert, CRC32 sharding. */
public class MySqlDialect extends StandardDialect {
  public MySqlDialect() {
    super("mysql");
  }

  @Override
  public String quote(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  @Override
  public boolean supportsMerge() {
    return true;
  }

  @Override
  public String buildMerge(String targetTable, List<String> pk, List<String> allColumns) {
    String cols = allColumns.stream().map(this::quote).collect(Collectors.joining(", "));
    String placeholders = allColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
    List<String> updates =
        allColumns.stream()
            .filter(c -> pk.stream().noneMatch(p -> p.equalsIgnoreCase(c)))
            .map(c -> quote(c) + " = VALUES(" + quote(c) + ")")
            .toList();
    String updateClause =
        updates.isEmpty()
            ? quote(pk.get(0)) + " = " + quote(pk.get(0))
            : String.join(", ", updates);
    return "INSERT INTO "
        + targetTable
        + " ("
        + cols
        + ") VALUES ("
        + placeholders
        + ") ON DUPLICATE KEY UPDATE "
        + updateClause;
  }

  @Override
  public boolean supportsHashSplit() {
    return true;
  }

  @Override
  public String buildHashSplitWhere(String splitColumn, int modulus, int remainder) {
    // CRC32(col) is stable across MySQL versions and cheap enough to run as a predicate.
    return "MOD(CRC32(" + quote(splitColumn) + "), " + modulus + ") = " + remainder;
  }

  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD COLUMN " + quote(column) + " " + sqlType;
  }
}
