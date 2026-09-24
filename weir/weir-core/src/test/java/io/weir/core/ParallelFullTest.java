package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Parallel FULL sync must give every worker its own writer/connection — with the old single shared
 * sink all JDBC writes serialised behind one global lock regardless of extraction parallelism.
 *
 * <p>The test asserts the functional contract (all rows land exactly once, watermark correct,
 * checkpoint complete); the throughput win is architectural, not something a unit test can time.
 */
class ParallelFullTest {

  @TempDir Path tmp;

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(
        "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL", "sa", "");
  }

  @Test
  void parallelFullWritesEveryRowExactlyOnce() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("dst").toAbsolutePath() + ";MODE=MySQL";

    try (Connection src = open("src"); Connection dst = open("dst")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
        StringBuilder ins = new StringBuilder("INSERT INTO t VALUES ");
        for (int i = 1; i <= 40; i++) {
          if (i > 1) {
            ins.append(", ");
          }
          ins.append("(").append(i).append(", 'v").append(i).append("', ").append(i).append(".00)");
        }
        st.execute(ins.toString());
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
      }
    }

    String yml =
        """
        name: parallel-full
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name, amount]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            splits:
              column: id
              numPartitions: 4
              mode: parallel
          poolMax: 4
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        runtime:
          extractThreads: 3
          writeBatchSize: 5
        state:
          type: file
          path: %s
        """
            .formatted(srcUrl, dstUrl, tmp.resolve("st").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yml);
    assertEquals(4, config.source.read.splits.numPartitions, "parallel split config parsed");
    assertEquals(3, config.runtime.extractThreads);

    SyncResult result = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(result.success(), result.message());
    assertTrue(result.message().contains("shards done"), "checkpointed path ran: " + result.message());
    assertEquals(40, result.report().rowsRead(), "every source row read exactly once");
    assertEquals(40, result.report().rowsWritten(), "every row written exactly once");
    assertEquals(40, countRows(dstUrl), "target holds all 40 rows");
    assertEquals(40, countIds(dstUrl), "no duplicate keys in the target");
  }

  private int countRows(String url) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "sa", "");
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /** COUNT(DISTINCT id) — merge makes duplicates impossible, the count just proves it holds. */
  private int countIds(String url) throws Exception {
    try (Connection c = DriverManager.getConnection(url, "sa", "");
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(DISTINCT id) FROM t")) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
