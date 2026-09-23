package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.model.RunMode;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The diff pass must converge: a second run over an already-reconciled table has to be a no-op.
 *
 * <p>The previous implementation read only the key columns from the target but compared every
 * column, so every row looked "changed" and the whole table was rewritten on every pass.
 */
class PkDiffConvergenceTest {

  @TempDir Path tmp;

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(
        "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL", "sa", "");
  }

  private String yaml(String srcUrl, String dstUrl, Path state) {
    return """
        name: diff-converge
        mode: diff
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name, amount, update_time]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            deleteDetect:
              mode: pk_diff
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        quality:
          compareRowContent: true
        state:
          type: file
          path: %s
        """
        .formatted(srcUrl, dstUrl, state.toAbsolutePath());
  }

  @Test
  void secondDiffPassIsANoOp() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("dst").toAbsolutePath() + ";MODE=MySQL";
    Instant t0 = Instant.parse("2024-05-01T00:00:00Z");

    try (Connection src = open("src");
        Connection dst = open("dst")) {
      try (Statement st = src.createStatement()) {
        st.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2), update_time TIMESTAMP)");
        st.execute(
            "INSERT INTO t VALUES (1, 'a', 1.10, TIMESTAMP '"
                + Timestamp.from(t0)
                + "'), (2, 'b', 2.20, TIMESTAMP '"
                + Timestamp.from(t0.plusSeconds(60))
                + "')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2), update_time TIMESTAMP)");
        // id=1 identical, id=2 stale content, id=9 must be deleted.
        st.execute(
            "INSERT INTO t VALUES (1, 'a', 1.10, TIMESTAMP '"
                + Timestamp.from(t0)
                + "'), (2, 'OLD', 9.99, TIMESTAMP '"
                + Timestamp.from(t0)
                + "'), (9, 'gone', 0.00, TIMESTAMP '"
                + Timestamp.from(t0)
                + "')");
      }
    }

    JobConfig config = ConfigLoader.fromYaml(yaml(srcUrl, dstUrl, tmp.resolve("st")));

    SyncResult first = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(first.success(), first.message());
    String firstMsg = first.message();
    assertTrue(firstMsg.contains("delete=1"), "id=9 removed: " + firstMsg);
    assertTrue(firstMsg.contains("rewrite=1"), "id=2 stale content corrected: " + firstMsg);
    // id=1 is byte-identical, so it must NOT be rewritten — that was the whole-table rewrite bug.
    assertEquals(1, first.report().rowsRewritten(), "only the changed row is rewritten: " + firstMsg);

    assertEquals(2, countRows(dstUrl), "target converges to the source's 2 rows");
    assertEquals("b", name(dstUrl, 2), "stale content is corrected");

    // Second pass over an already-converged table must do nothing at all.
    SyncResult second = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(second.success(), second.message());
    assertEquals(
        0, second.report().rowsRewritten(), "no rewrites on a converged table: " + second.message());
    assertEquals(0, second.report().rowsDeleted(), "no deletes on a converged table");
    assertEquals(0, second.report().rowsWritten(), "converged table produces no writes at all");
    assertEquals(2, countRows(dstUrl));
  }

  @Test
  void timestampValuesWithDifferentTypesAreNotTreatedAsChanges() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("s2").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("d2").toAbsolutePath() + ";MODE=MySQL";
    Instant t0 = Instant.parse("2024-05-01T00:00:00Z");
    try (Connection src = open("s2");
        Connection dst = open("d2")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
        st.execute("INSERT INTO t VALUES (1, 'a', TIMESTAMP '" + Timestamp.from(t0) + "')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
        st.execute("INSERT INTO t VALUES (1, 'a', TIMESTAMP '" + Timestamp.from(t0) + "')");
      }
    }
    String yml =
        """
        name: ts-diff
        mode: diff
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name, update_time]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            deleteDetect:
              mode: pk_diff
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        quality:
          compareRowContent: true
        state:
          type: file
          path: %s
        """
            .formatted(srcUrl, dstUrl, tmp.resolve("st2").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yml);
    // The target row comes back as java.sql.Timestamp while the source row is an Instant.
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    assertEquals(
        0,
        result.report().rowsRewritten(),
        "same instant in different Java types is not a change: " + result.message());
  }

  @Test
  void softDeleteUsesDefaultTruthinessForFlagColumns() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("s3").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("d3").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = open("s3");
        Connection dst = open("d3")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), is_deleted INT)");
        st.execute("INSERT INTO t VALUES (1,'a',0),(2,'b',1),(3,'c',0)");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), is_deleted INT)");
        st.execute("INSERT INTO t VALUES (1,'a',0),(2,'b',1),(3,'c',0)");
      }
    }
    JobConfig config = ConfigLoader.fromYaml(softYaml(srcUrl, dstUrl, tmp.resolve("st3"), "is_deleted", null));
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    assertEquals(1, result.report().rowsDeleted(), "exactly one row is flagged: " + result.message());
    assertEquals(2, countRows(dstUrl), "only id=2 is deleted");
    assertFalse(exists(dstUrl, 2), "the soft-deleted row is gone from the target");
    assertTrue(exists(dstUrl, 1), "untouched rows stay");
    assertTrue(exists(dstUrl, 3), "untouched rows stay");
  }

  @Test
  void softDeleteHonoursExplicitTrueValues() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("s4").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("d4").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = open("s4");
        Connection dst = open("d4")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), state VARCHAR(10))");
        st.execute("INSERT INTO t VALUES (1,'a','ACTIVE'),(2,'b','GONE'),(3,'c','ACTIVE')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), state VARCHAR(10))");
        st.execute("INSERT INTO t VALUES (1,'a','ACTIVE'),(2,'b','GONE'),(3,'c','ACTIVE')");
      }
    }
    JobConfig config =
        ConfigLoader.fromYaml(
            softYaml(srcUrl, dstUrl, tmp.resolve("st4"), "state", List.of("GONE")));
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    assertEquals(1, result.report().rowsDeleted(), "only the GONE row is deleted: " + result.message());
    assertEquals(2, countRows(dstUrl));
    assertFalse(exists(dstUrl, 2));
  }

  private static String softYaml(
      String srcUrl, String dstUrl, Path state, String softColumn, List<String> trueValues) {
    String extra =
        (trueValues == null || trueValues.isEmpty())
            ? ""
            : trueValues.stream()
                .map(v -> "\"" + v + "\"")
                .collect(java.util.stream.Collectors.joining(", ", "\n      softTrueValues: [", "]"));
    return """
        name: soft-del
        mode: diff
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name, %s]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            deleteDetect:
              mode: soft_column
              softColumn: %s%s
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        state:
          type: file
          path: %s
        """
        .formatted(srcUrl, softColumn, softColumn, extra, dstUrl, state.toAbsolutePath());
  }

  private int countRows(String url) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "sa", "");
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private boolean exists(String url, long id) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "sa", "");
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1 FROM t WHERE id = " + id)) {
      return rs.next();
    }
  }

  private String name(String url, long id) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "sa", "");
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT name FROM t WHERE id = " + id)) {
      return rs.next() ? rs.getString(1) : null;
    }
  }
}
