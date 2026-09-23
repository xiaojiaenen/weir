package io.weir.dialect;

import java.util.List;
import java.util.stream.Collectors;

/** PostgreSQL: {@code ON CONFLICT} upsert, {@code %} modulo, {@code hashtext} sharding. */
public class PostgresDialect extends StandardDialect {
  public PostgresDialect() {
    super("postgres");
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
            .map(c -> quote(c) + " = EXCLUDED." + quote(c))
            .toList();
    String updateClause =
        updates.isEmpty()
            ? quote(pk.get(0)) + " = EXCLUDED." + quote(pk.get(0))
            : String.join(", ", updates);
    String conflict = pk.stream().map(this::quote).collect(Collectors.joining(", "));
    return "INSERT INTO "
        + targetTable
        + " ("
        + cols
        + ") VALUES ("
        + placeholders
        + ") ON CONFLICT ("
        + conflict
        + ") DO UPDATE SET "
        + updateClause;
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
    // hashtext() is a built-in (undocumented but stable) hash; ABS guards the signed result.
    return "MOD(ABS(hashtext(" + quote(splitColumn) + "::text)), " + modulus + ") = " + remainder;
  }

  @Override
  public String alterAddColumn(String table, String column, String sqlType) {
    return "ALTER TABLE " + table + " ADD COLUMN " + quote(column) + " " + sqlType;
  }
}
