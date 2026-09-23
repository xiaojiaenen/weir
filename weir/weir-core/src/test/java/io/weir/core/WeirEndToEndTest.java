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

class WeirEndToEndTest {

  @TempDir Path tmp;

  private Connection openSource() throws Exception {
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    return DriverManager.getConnection(url, "sa", "");
  }

  @Test
  void fullAndIncrementalIdSyncToFile() throws Exception {
    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute(
          "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, amount DECIMAL(10,2), update_time TIMESTAMP)");
      st.execute(
          "INSERT INTO orders VALUES (1, 10, 1.5, TIMESTAMP '2024-01-01 10:00:00'),"
              + "(2, 11, 2.5, TIMESTAMP '2024-01-01 11:00:00'),"
              + "(3, 12, 3.5, TIMESTAMP '2024-01-01 12:00:00')");
    }

    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    Path out = tmp.resolve("out/orders.jsonl");
    Path stateDir = tmp.resolve("state");

    String yaml =
        """
        name: orders-to-file
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: orders
            columns: [id, user_id, amount, update_time]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
              fetchSize: 100
            splits:
              column: id
              numPartitions: 2
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        """.formatted(url, out.toAbsolutePath(), stateDir.toAbsolutePath());

    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult full = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(full.success(), full.message());
    assertEquals(3, full.rowsRead());

    // insert new row and run incremental id
    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute(
          "INSERT INTO orders VALUES (4, 13, 4.5, TIMESTAMP '2024-01-02 09:00:00')");
    }
    config.mode = RunMode.INCREMENTAL;
    SyncResult incr = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(incr.success(), incr.message());
    assertEquals(1, incr.rowsRead());
    assertEquals(3L, full.endWatermark().id());
    assertEquals(4L, incr.endWatermark().id());

    String content = Files.readString(out);
    assertTrue(content.contains("\"id\":1"));
    assertTrue(content.contains("\"id\":4"));
  }

  @Test
  void updateTimeIdIncrementalWithMergeJdbcTarget() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("src2").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("dst2").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = DriverManager.getConnection(srcUrl, "sa", "");
        Connection dst = DriverManager.getConnection(dstUrl, "sa", "")) {
      try (Statement st = src.createStatement()) {
        st.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(50), update_time TIMESTAMP)");
        Instant t0 = Instant.parse("2024-06-01T00:00:00Z");
        st.execute(
            "INSERT INTO t VALUES (1, 'a', TIMESTAMP '"
                + Timestamp.from(t0).toString()
                + "'), (2, 'b', TIMESTAMP '"
                + Timestamp.from(t0.plusSeconds(60)).toString()
                + "')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(50), update_time TIMESTAMP)");
      }
    }

    String yaml =
        """
        name: t-merge
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
              strategy: update_time_id
              column: update_time
              idColumn: id
              overlap: 0s
              fetchSize: 50
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
            .formatted(
                srcUrl, dstUrl, tmp.resolve("state2").toAbsolutePath());

    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult first = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(first.success(), first.message());
    assertEquals(2, first.rowsWritten());

    // update id=1 and insert id=3
    try (Connection src = DriverManager.getConnection(srcUrl, "sa", "");
        Statement st = src.createStatement()) {
      Instant t1 = Instant.parse("2024-06-02T00:00:00Z");
      st.execute(
          "UPDATE t SET name = 'a2', update_time = TIMESTAMP '"
              + Timestamp.from(t1).toString()
              + "' WHERE id = 1");
      st.execute(
          "INSERT INTO t VALUES (3, 'c', TIMESTAMP '"
              + Timestamp.from(t1.plusSeconds(10)).toString()
              + "')");
    }

    SyncResult second = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(second.success(), second.message());
    assertEquals(2, second.rowsRead());

    try (Connection dst = DriverManager.getConnection(dstUrl, "sa", "");
        Statement st = dst.createStatement();
        var rs = st.executeQuery("SELECT name FROM t WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals("a2", rs.getString(1));
    }
    try (Connection dst = DriverManager.getConnection(dstUrl, "sa", "");
        Statement st = dst.createStatement();
        var rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
    }
  }

  @Test
  void parallelFullExtractReadsAllRowsOnce() throws Exception {
    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE big (id BIGINT PRIMARY KEY, v INT)");
      for (int i = 1; i <= 40; i++) {
        st.execute("INSERT INTO big VALUES (" + i + ", " + (i * 10) + ")");
      }
    }
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    Path out = tmp.resolve("out/big.jsonl");
    String yaml =
        """
        name: big-parallel
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: big
            columns: [id, v]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            splits:
              column: id
              numPartitions: 4
              mode: parallel
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
          writeBatchSize: 10
        """
            .formatted(url, out.toAbsolutePath(), tmp.resolve("st-p").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(result.success(), result.message());
    assertEquals(40, result.rowsRead());
    assertEquals(40, result.rowsWritten());
    String content = Files.readString(out);
    long lines = content.lines().filter(s -> !s.isBlank()).count();
    assertEquals(40, lines);
  }

  @Test
  void softDeleteDiffEmitsDeleteMarkers() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("sd-src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("sd-dst").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = DriverManager.getConnection(srcUrl, "sa", "");
        Connection dst = DriverManager.getConnection(dstUrl, "sa", "")) {
      try (Statement st = src.createStatement()) {
        st.execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), is_deleted INT, update_time TIMESTAMP)");
        st.execute(
            "INSERT INTO t VALUES (1, 'a', 0, TIMESTAMP '2024-01-01 10:00:00'),"
                + "(2, 'b', 1, TIMESTAMP '2024-01-01 11:00:00')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
        st.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
      }
    }
    String yaml =
        """
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
            columns: [id, name, is_deleted, update_time]
            primaryKey: [id]
            incremental:
              strategy: update_time
              column: update_time
              idColumn: id
            deleteDetect:
              mode: soft_column
              softColumn: is_deleted
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
            .formatted(srcUrl, dstUrl, tmp.resolve("st-sd").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    try (Connection dst = DriverManager.getConnection(dstUrl, "sa", "");
        Statement st = dst.createStatement();
        var rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  @Test
  void consoleWriterReceivesRows() throws Exception {
    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE c (id BIGINT PRIMARY KEY, n VARCHAR(10))");
      st.execute("INSERT INTO c VALUES (1, 'x'), (2, 'y')");
    }
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    String yaml =
        """
        name: to-console
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: c
            columns: [id, n]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
        target:
          type: console
        state:
          type: file
          path: %s
        """
            .formatted(url, tmp.resolve("st-c").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(result.success(), result.message());
    assertEquals(2, result.rowsWritten());
  }

  @Test
  void queryModeIncrementalProjectsContractColumns() throws Exception {
    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE o (id BIGINT PRIMARY KEY, uid BIGINT, update_time TIMESTAMP)");
      st.execute("CREATE TABLE u (id BIGINT PRIMARY KEY, channel VARCHAR(20))");
      st.execute("INSERT INTO u VALUES (10, 'app'), (11, 'web')");
      st.execute(
          "INSERT INTO o VALUES (1, 10, TIMESTAMP '2024-03-01 08:00:00'),"
              + "(2, 11, TIMESTAMP '2024-03-01 09:00:00')");
    }
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    Path out = tmp.resolve("out/wide.jsonl");
    String yaml =
        """
        name: query-incr
        mode: incremental
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: query
            query: |
              SELECT o.id, o.uid, o.update_time, u.channel
              FROM o JOIN u ON u.id = o.uid
            columns: [id, uid, update_time, channel]
            primaryKey: [id]
            incremental:
              strategy: update_time_id
              column: update_time
              idColumn: id
              overlap: 0s
              fetchSize: 50
        target:
          type: file
          path: %s
          format: jsonl
          primaryKey: [id]
        state:
          type: file
          path: %s
        """
            .formatted(url, out.toAbsolutePath(), tmp.resolve("st-q").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult first = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(first.success(), first.message());
    assertEquals(2, first.rowsRead());

    try (Connection conn = openSource(); Statement st = conn.createStatement()) {
      st.execute(
          "INSERT INTO o VALUES (3, 10, TIMESTAMP '2024-03-02 08:00:00')");
    }
    SyncResult second = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(second.success(), second.message());
    assertEquals(1, second.rowsRead());
    String content = Files.readString(out);
    assertTrue(content.contains("channel"));
    assertTrue(content.contains("\"id\":3"));
  }

  @Test
  void pkDiffFixesTargetInsertsAndDeletes() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("d-src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("d-dst").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = DriverManager.getConnection(srcUrl, "sa", "");
        Connection dst = DriverManager.getConnection(dstUrl, "sa", "")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
        st.execute("INSERT INTO t VALUES (1, 'keep'), (2, 'upd'), (3, 'new')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
        st.execute("INSERT INTO t VALUES (1, 'keep'), (2, 'old'), (9, 'gone')");
      }
    }
    String yaml =
        """
        name: pk-diff
        mode: diff
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name]
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
            .formatted(srcUrl, dstUrl, tmp.resolve("st-d").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    try (Connection dst = DriverManager.getConnection(dstUrl, "sa", "");
        Statement st = dst.createStatement()) {
      try (var rs = st.executeQuery("SELECT name FROM t WHERE id = 2")) {
        assertTrue(rs.next());
        assertEquals("upd", rs.getString(1));
      }
      try (var rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
        assertTrue(rs.next());
        // keep(1)+upd(2)+new(3); 9 deleted
        assertEquals(3, rs.getInt(1));
      }
    }
  }

  @Test
  void schemaEvolutionAddsColumnOnJdbcTarget() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("s-src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("s-dst").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = DriverManager.getConnection(srcUrl, "sa", "");
        Connection dst = DriverManager.getConnection(dstUrl, "sa", "")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), extra VARCHAR(20))");
        st.execute("INSERT INTO t VALUES (1, 'a', 'x')");
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
      }
    }
    String yaml =
        """
        name: schema-add
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name, extra]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        quality:
          autoAddColumns: true
        state:
          type: file
          path: %s
        """
            .formatted(srcUrl, dstUrl, tmp.resolve("st-s").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(result.success(), result.message());
    try (Connection dst = DriverManager.getConnection(dstUrl, "sa", "");
        Statement st = dst.createStatement();
        var rs = st.executeQuery("SELECT extra FROM t WHERE id = 1")) {
      assertTrue(rs.next());
      assertEquals("x", rs.getString(1));
    }
  }

  @Test
  void checkModeValidatesConfig() throws Exception {
    String url = "jdbc:h2:" + tmp.resolve("src3").toAbsolutePath() + ";MODE=MySQL";
    try (Connection conn = DriverManager.getConnection(url, "sa", "");
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE x (id BIGINT PRIMARY KEY)");
      st.execute("INSERT INTO x VALUES (1)");
    }
    String yaml =
        """
        name: check-job
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: x
            columns: [id]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        """
            .formatted(url, tmp.resolve("x.jsonl").toAbsolutePath(), tmp.resolve("st").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yaml);
    SyncResult result = new WeirRunner(config).run(RunMode.CHECK);
    assertTrue(result.success(), result.message());
  }
}
