package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.model.RunMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cursor is where "no CDC" either works or silently loses rows. These tests pin the two rules
 * that matter: rows sharing a timestamp must all be captured, and a back-dated write inside the
 * overlap window must still be picked up.
 */
class IncrementalBoundaryTest {

  @TempDir Path tmp;

  private String db(String name) {
    return "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL";
  }

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(db(name), "sa", "");
  }

  private String yaml(String url, String strategy, String overlap, int batchRows) {
    return yaml(url, strategy, overlap, batchRows, 0L);
  }

  private String yaml(
      String url, String strategy, String overlap, int batchRows, long maxRowsPerRun) {
    return """
        name: boundary
        mode: incremental
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
              strategy: %s
              column: update_time
              idColumn: id
              overlap: %s
              batchRows: %d
              maxRowsPerRun: %d
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        """
        .formatted(
            url,
            strategy,
            overlap,
            batchRows,
            maxRowsPerRun,
            tmp.resolve("out.jsonl").toAbsolutePath(),
            tmp.resolve("st").toAbsolutePath());
  }

  @Test
  void rowsSharingATimestampAreAllCaptured() throws Exception {
    String name = "same-ts";
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
    }
    Instant t0 = Instant.parse("2024-03-01T00:00:00Z");
    JobConfig config = ConfigLoader.fromYaml(yaml(db(name), "update_time_id", "0s", 2));

    // Six rows all stamped with the same second, paged two at a time.
    try (Connection c = open(name); Statement st = c.createStatement()) {
      for (int i = 1; i <= 6; i++) {
        st.execute(
            "INSERT INTO t VALUES (" + i + ",'n" + i + "',TIMESTAMP '" + Timestamp.from(t0) + "')");
      }
    }
    SyncResult first = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(first.success(), first.message());
    assertEquals(6, first.rowsRead(), "paging must not drop rows that share a timestamp");
    assertEquals(6, first.rowsWritten());

    // A further row at the same timestamp but a higher id: the tie-break must find it.
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("INSERT INTO t VALUES (7,'n7',TIMESTAMP '" + Timestamp.from(t0) + "')");
    }
    SyncResult second = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertEquals(1, second.rowsRead(), "id tie-break catches rows at the exact watermark: " + second.message());
  }

  @Test
  void overlapRecoversBackDatedWrites() throws Exception {
    String name = "overlap";
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
    }
    // overlap=1h so a row stamped before the current watermark is still inside the window.
    JobConfig config = ConfigLoader.fromYaml(yaml(db(name), "update_time", "1h", 100));
    Instant t0 = Instant.parse("2024-04-01T12:00:00Z");
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("INSERT INTO t VALUES (1,'a',TIMESTAMP '" + Timestamp.from(t0) + "')");
    }
    assertEquals(1, new WeirRunner(config).run(RunMode.INCREMENTAL).rowsRead());

    // A late-arriving row stamped 10 minutes *before* the watermark.
    Instant backDated = t0.minusSeconds(600);
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("INSERT INTO t VALUES (2,'late',TIMESTAMP '" + Timestamp.from(backDated) + "')");
    }
    SyncResult second = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(
        second.rowsRead() >= 1,
        "the overlap window re-reads back-dated writes: " + second.message());
    assertTrue(
        Files.readString(tmp.resolve("out.jsonl")).contains("late"),
        "the back-dated row actually reached the target");
    // Overlap deliberately re-reads rows near the cursor; the sink's merge absorbs them.
    assertEquals(1, distinctIds(tmp.resolve("out.jsonl"), 2L), "row 2 appears once after merge");

    // Without overlap the same row is missed — which is why overlap defaults to non-zero.
    Path noOverlapOut = tmp.resolve("out-strict.jsonl");
    String strictYaml =
        yaml(db("overlap2"), "update_time", "0s", 100)
            .replace(
                tmp.resolve("out.jsonl").toAbsolutePath().toString(),
                noOverlapOut.toAbsolutePath().toString())
            .replace(
                tmp.resolve("st").toAbsolutePath().toString(),
                tmp.resolve("st-strict").toAbsolutePath().toString());
    JobConfig noOverlap = ConfigLoader.fromYaml(strictYaml);
    try (Connection c = open("overlap2");
        Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      st.execute("INSERT INTO t VALUES (1,'a',TIMESTAMP '" + Timestamp.from(t0) + "')");
    }
    new WeirRunner(noOverlap).run(RunMode.INCREMENTAL);
    try (Connection c = open("overlap2");
        Statement st = c.createStatement()) {
      st.execute("INSERT INTO t VALUES (2,'late',TIMESTAMP '" + Timestamp.from(backDated) + "')");
    }
    new WeirRunner(noOverlap).run(RunMode.INCREMENTAL);
    assertEquals(
        0,
        distinctIds(noOverlapOut, 2L),
        "zero overlap really does miss the back-dated row — do not set 0 on a live table");
  }

  @Test
  void replayingAfterAFailureDoesNotDuplicate() throws Exception {
    String name = "replay";
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      st.execute("INSERT INTO t VALUES (1,'a',TIMESTAMP '2024-05-01 10:00:00')");
    }
    JobConfig config = ConfigLoader.fromYaml(yaml(db(name), "id", "0s", 100));
    SyncResult first = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertEquals(1, first.rowsRead());
    // Re-running with no new data reads nothing: the cursor moved only after the write succeeded.
    assertEquals(0, new WeirRunner(config).run(RunMode.INCREMENTAL).rowsRead());
    assertEquals(1, countLines(tmp.resolve("out.jsonl")), "no duplicate lines after a replay");
  }

  @Test
  void incrementalStopsAtMaxRowsPerRun() throws Exception {
    String name = "cap";
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      for (int i = 1; i <= 50; i++) {
        st.execute("INSERT INTO t VALUES (" + i + ",'n" + i + "',TIMESTAMP '2024-05-01 10:00:00')");
      }
    }
    String yml = yaml(db(name), "id", "0s", 1000, 20);
    JobConfig config = ConfigLoader.fromYaml(yml);
    assertEquals(20L, config.source.read.incremental.maxRowsPerRun);
    SyncResult r = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(r.success(), r.message());
    assertEquals(20, r.rowsRead(), "a bounded pass protects the source from a huge backlog");
  }

  private long countLines(Path p) throws Exception {
    if (!Files.exists(p)) {
      return 0;
    }
    return Files.readAllLines(p).stream().filter(s -> !s.isBlank()).count();
  }

  private long distinctIds(Path p, long id) throws Exception {
    if (!Files.exists(p)) {
      return 0;
    }
    return Files.readAllLines(p).stream()
        .filter(s -> s.contains("\"id\":" + id))
        .count();
  }
}
