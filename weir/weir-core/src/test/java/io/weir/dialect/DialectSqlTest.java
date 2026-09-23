package io.weir.dialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.model.IncrementalStrategy;
import io.weir.model.SplitRange;
import io.weir.model.Watermark;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/** SQL generation for each vendor — both the source (select/split) and sink (upsert/DDL) sides. */
class DialectSqlTest {

  private static final List<String> PK = List.of("id");
  private static final List<String> COLS = List.of("id", "n");

  @Test
  void vendorUpsertForms() {
    assertTrue(
        Dialects.forType("mysql").buildMerge("t", PK, COLS).contains("ON DUPLICATE KEY"),
        "mysql uses INSERT ... ON DUPLICATE KEY UPDATE");
    assertTrue(
        Dialects.forType("postgres").buildMerge("t", PK, COLS).contains("ON CONFLICT"),
        "postgres uses ON CONFLICT DO UPDATE");
    assertTrue(
        Dialects.forType("oracle").buildMerge("t", PK, COLS).contains("MERGE INTO"),
        "oracle uses MERGE INTO");
    assertTrue(
        Dialects.forType("sqlserver").buildMerge("t", PK, COLS).contains("MERGE INTO"),
        "sqlserver uses MERGE INTO");
    assertTrue(Dialects.forType("h2").buildMerge("t", PK, COLS).contains("KEY("));

    // Upsert must never rewrite the key columns it conflicts on.
    String mysql = Dialects.forType("mysql").buildMerge("t", PK, COLS);
    assertTrue(mysql.contains("`n` = VALUES(`n`)"), "non-key columns are updated");
    assertFalse(mysql.contains("`id` = VALUES(`id`)"), "key columns are not reassigned");
  }

  @Test
  void vendorPagination() {
    assertTrue(
        Dialects.forType("oracle").buildSelect(List.of("id"), "t", "id > 0", "id", 10)
            .contains("FETCH FIRST 10 ROWS ONLY"));
    assertTrue(
        Dialects.forType("sqlserver").buildSelect(List.of("id"), "t", "id > 0", "id", 10)
            .contains("TOP (10)"));
    assertTrue(
        Dialects.forType("mysql").buildSelect(List.of("id"), "t", "id > 0", "id", 10)
            .contains("LIMIT 10"));
  }

  @Test
  void vendorQuoting() {
    assertEquals("`a b`", Dialects.forType("mysql").quote("a b"));
    assertEquals("[a b]", Dialects.forType("sqlserver").quote("a b"));
    assertEquals("\"a b\"", Dialects.forType("postgres").quote("a b"));
    // H2 stays unquoted: unquoted identifiers fold to upper case, quoting breaks that.
    assertEquals("a b", Dialects.forType("h2").quote("a b"));
  }

  @Test
  void halfOpenSplitRanges() {
    String sql = Dialects.forType("mysql").buildSplitWhere(new SplitRange(10L, 20L), "id");
    assertTrue(sql.contains(">= 10") && sql.contains("< 20"), "range is [lo, hi)");
    assertEquals("", Dialects.forType("mysql").buildSplitWhere(null, "id"));

    // An open-ended last shard must not emit an upper bound.
    String last = Dialects.forType("mysql").buildSplitWhere(new SplitRange(10L, null), "id");
    assertTrue(last.contains(">= 10"));
    assertFalse(last.contains("<"));
  }

  @Test
  void modulusAndHashSharding() {
    assertTrue(Dialects.forType("mysql").buildModSplitWhere("id", 4, 1).contains("MOD("));
    assertTrue(Dialects.forType("postgres").buildModSplitWhere("id", 4, 1).contains("%"));
    assertTrue(Dialects.forType("sqlserver").buildModSplitWhere("id", 4, 1).contains("%"));

    assertTrue(Dialects.forType("mysql").supportsHashSplit());
    assertTrue(Dialects.forType("mysql").buildHashSplitWhere("code", 4, 1).contains("CRC32"));
    assertTrue(Dialects.forType("postgres").buildHashSplitWhere("code", 4, 1).contains("hashtext"));
    assertTrue(Dialects.forType("oracle").buildHashSplitWhere("code", 4, 1).contains("ORA_HASH"));
    assertTrue(Dialects.forType("sqlserver").buildHashSplitWhere("code", 4, 1).contains("CHECKSUM"));

    // A generic dialect has no portable hash: the planner must degrade, not emit broken SQL.
    assertFalse(Dialects.forType("standard").supportsHashSplit());
    assertThrows(
        UnsupportedOperationException.class,
        () -> Dialects.forType("standard").buildHashSplitWhere("code", 4, 1));
  }

  @Test
  void incrementalPredicates() {
    JdbcDialect d = Dialects.forType("mysql");
    Instant ts = Instant.parse("2024-06-01T00:00:00Z");

    String idPred = d.buildIncrementWhere(IncrementalStrategy.ID, Watermark.ofId(5L), null, "id", Duration.ZERO);
    assertTrue(idPred.contains("id") && idPred.contains("> 5"), "id cursor is strictly greater");

    // With an empty watermark there is nothing to filter — the run bootstraps from scratch.
    assertEquals(
        "", d.buildIncrementWhere(IncrementalStrategy.ID, Watermark.empty(), null, "id", Duration.ZERO));

    String tsPred =
        d.buildIncrementWhere(
            IncrementalStrategy.UPDATE_TIME, Watermark.ofTs(ts), "update_time", "id", Duration.ofMinutes(5));
    assertTrue(tsPred.contains(">="), "update_time re-reads an overlap window inclusively");

    String composite =
        d.buildIncrementWhere(
            IncrementalStrategy.UPDATE_TIME_ID,
            Watermark.ofTsId(ts, 7L),
            "update_time",
            "id",
            Duration.ZERO);
    assertTrue(composite.contains("OR"), "composite cursor has a tie-break branch");
    assertTrue(composite.contains("id"), "tie-break uses the id column");
  }

  @Test
  void sinkDeleteAndDdl() {
    assertEquals(
        "DELETE FROM t WHERE `id` = ?", Dialects.forType("mysql").buildDelete("t", PK));
    assertTrue(
        Dialects.forType("oracle").alterAddColumn("t", "x", "VARCHAR(10)").contains("ADD ("),
        "Oracle needs parenthesised column definitions");
    assertTrue(Dialects.forType("sqlserver").alterAddColumn("t", "x", "INT").contains("ADD [x]"));
    assertTrue(Dialects.forType("mysql").alterAddColumn("t", "x", "INT").contains("ADD COLUMN"));
  }

  @Test
  void resolvesFromJdbcUrl() {
    assertEquals("mysql", Dialects.forUrl("jdbc:mysql://h:3306/db").name());
    assertEquals("postgres", Dialects.forUrl("jdbc:postgresql://h:5432/db").name());
    assertEquals("oracle", Dialects.forUrl("jdbc:oracle:thin:@h:1521/x").name());
    assertEquals("sqlserver", Dialects.forUrl("jdbc:sqlserver://h:1433;db=x").name());
    assertEquals("h2", Dialects.forUrl("jdbc:h2:mem:test").name());
    assertEquals("standard", Dialects.forUrl("jdbc:sqlite:x").name());
  }
}
