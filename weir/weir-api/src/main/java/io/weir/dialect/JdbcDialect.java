package io.weir.dialect;

import io.weir.model.IncrementalStrategy;
import io.weir.model.SplitMode;
import io.weir.model.SplitRange;
import io.weir.model.Watermark;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Vendor-specific SQL building and metadata — the single place where Weir knows how a database
 * spells things.
 *
 * <p>One dialect instance covers <em>both</em> sides of a sync:
 *
 * <ul>
 *   <li><b>source</b> — SELECT / incremental predicate / split predicate / MIN-MAX probing
 *   <li><b>sink</b> — upsert (MERGE), plain INSERT, DELETE by PK, and ALTER TABLE ADD COLUMN
 * </ul>
 *
 * <p>Keeping sink SQL here avoids every writer re-sniffing JDBC URLs (and drifting out of sync).
 */
public interface JdbcDialect {

  String name();

  /** Quote an identifier for this vendor. */
  String quote(String identifier);

  /**
   * Build SELECT for one batch.
   *
   * @param columns projection (implementations quote them)
   * @param fromClause table name, or {@code (subquery) alias} in query mode
   * @param where extra predicates joined by AND (may be null/blank)
   */
  String buildSelect(
      List<String> columns, String fromClause, String where, String orderBy, Integer limit);

  /** Predicate selecting rows strictly after {@code watermark}, honouring {@code overlap}. */
  String buildIncrementWhere(
      IncrementalStrategy strategy,
      Watermark watermark,
      String tsColumn,
      String idColumn,
      Duration overlap);

  /** Half-open numeric range predicate {@code [lo, hi)}. */
  String buildSplitWhere(SplitRange range, String splitColumn);

  /**
   * Predicate selecting one shard by modulus: {@code MOD(col, n) = r}. Used when the split column
   * is numeric but sparse (uniform ranges would produce heavily skewed shards).
   */
  String buildModSplitWhere(String splitColumn, int modulus, int remainder);

  /**
   * Predicate selecting one shard by hashing the split column. Used when the split column is
   * non-numeric (string / UUID business keys).
   */
  String buildHashSplitWhere(String splitColumn, int modulus, int remainder);

  /** Whether {@link #buildHashSplitWhere} is available on this vendor. */
  default boolean supportsHashSplit() {
    return false;
  }

  /**
   * Probe MIN/MAX of the split column. Returns {@code {0,0}} when the table is empty or the column
   * is not numeric — callers should fall back to {@link SplitMode#MOD} in that case.
   */
  long[] minMax(Connection conn, String fromClause, String splitColumn) throws SQLException;

  /** Literal for an instant, comparable against the vendor's timestamp column. */
  String tsLiteral(Instant instant);

  // ---------------------------------------------------------------- sink side

  /** Whether this vendor can express an idempotent upsert. */
  boolean supportsMerge();

  /** Idempotent upsert (INSERT ... ON DUPLICATE / ON CONFLICT / MERGE INTO). */
  String buildMerge(String targetTable, List<String> pk, List<String> allColumns);

  /** Plain batch insert, used when writeMode is not merge. */
  String buildInsert(String targetTable, List<String> allColumns);

  /** Delete by primary key — how {@code _op=d} rows land on this vendor. */
  String buildDelete(String targetTable, List<String> pk);

  /** DDL for schema evolution (additive columns only). */
  String alterAddColumn(String table, String column, String sqlType);

  /** Whether this vendor needs the LIMIT clause emitted before the projection (SQL Server TOP). */
  default boolean limitBeforeProjection() {
    return false;
  }

  /** Timeout hint applied to statements, in seconds; 0 means "leave unset". */
  default int defaultQueryTimeoutSeconds() {
    return 0;
  }
}
