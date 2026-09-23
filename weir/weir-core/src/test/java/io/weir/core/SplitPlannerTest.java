package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.model.RunMode;
import io.weir.model.SplitMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Shard planning must survive keys that are not auto-increment integers — the old planner called
 * {@code rs.getLong()} on MIN/MAX and quietly collapsed everything into one shard.
 */
class SplitPlannerTest {

  @TempDir Path tmp;

  private String db(String name) {
    return "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL";
  }

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(db(name), "sa", "");
  }

  private String yaml(String url, String splitColumn, int partitions, String strategy, Path out) {
    return """
        name: split-job
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, code]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            splits:
              column: %s
              numPartitions: %d
              mode: parallel
              strategy: %s
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        runtime:
          extractThreads: 4
          maxConcurrentQueries: 4
        """
        .formatted(url, splitColumn, partitions, strategy, out.toAbsolutePath(),
            tmp.resolve("st").toAbsolutePath());
  }

  @Test
  void numericRangeShardsCoverEveryRowOnce() throws Exception {
    try (Connection c = open("num"); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(10))");
      for (int i = 1; i <= 40; i++) {
        st.execute("INSERT INTO t VALUES (" + i + ", 'c" + i + "')");
      }
    }
    Path out = tmp.resolve("out/num.jsonl");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("num"), "id", 4, "range", out));
    SyncResult r = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(r.success(), r.message());
    assertEquals(40, r.rowsRead());
    assertEquals(40, countLines(out), "each row appears exactly once across shards");
  }

  @Test
  void modulusShardsCoverEveryRowOnce() throws Exception {
    try (Connection c = open("mod"); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(10))");
      // Sparse ids: uniform ranges would leave most shards empty.
      for (int i = 1; i <= 20; i++) {
        st.execute("INSERT INTO t VALUES (" + (i * 1000) + ", 'c" + i + "')");
      }
    }
    Path out = tmp.resolve("out/mod.jsonl");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("mod"), "id", 4, "mod", out));
    SyncResult r = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(r.success(), r.message());
    assertEquals(20, r.rowsRead());
    assertEquals(20, countLines(out));
  }

  @Test
  void nonNumericKeyDegradesToSingleShardInsteadOfFailing() throws Exception {
    try (Connection c = open("str"); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(10))");
      for (int i = 1; i <= 10; i++) {
        st.execute("INSERT INTO t VALUES (" + i + ", 'c" + i + "')");
      }
    }
    Path out = tmp.resolve("out/str.jsonl");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("str"), "code", 4, "auto", out));
    // H2 has no hash-partition predicate, so AUTO must fall back rather than emit broken SQL.
    SyncResult r = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(r.success(), "non-numeric split column must not break the run: " + r.message());
    assertEquals(10, r.rowsRead());
    assertEquals(10, countLines(out), "the single-shard fallback still reads every row");
  }

  @Test
  void explicitNoneShardReadsWholeTable() throws Exception {
    try (Connection c = open("none"); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(10))");
      st.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')");
    }
    Path out = tmp.resolve("out/none.jsonl");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("none"), "id", 4, "none", out));
    assertEquals(SplitMode.NONE, config.source.read.splits.strategy);
    SyncResult r = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(r.success(), r.message());
    assertEquals(3, countLines(out));
  }

  @Test
  void emptyTableDoesNotHangOrLoseShards() throws Exception {
    try (Connection c = open("empty"); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(10))");
    }
    Path out = tmp.resolve("out/empty.jsonl");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("empty"), "id", 4, "range", out));
    SyncResult r = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(r.success(), r.message());
    assertEquals(0, r.rowsRead());
    assertEquals(0, countLines(out), "an empty source produces no rows");
  }

  private long countLines(Path p) throws Exception {
    if (!Files.exists(p)) {
      return 0;
    }
    return Files.readAllLines(p).stream().filter(s -> !s.isBlank()).count();
  }
}
