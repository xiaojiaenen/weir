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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The streaming (windowed) pk_diff path must produce exactly the same reconciliation as the old
 * whole-table scan, while never holding more than one window of rows in memory.
 *
 * <p>{@code runtime.diffWindowRows} is set far below the row count so the compare is forced across
 * several keyset-pagination windows — including deletes and inserts at window boundaries.
 */
class PkDiffStreamingTest {

  @TempDir Path tmp;

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(
        "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL", "sa", "");
  }

  private String yaml(String srcUrl, String dstUrl, Path state) {
    return """
        name: diff-stream
        mode: diff
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
        runtime:
          diffWindowRows: 10
        state:
          type: file
          path: %s
        """
        .formatted(srcUrl, dstUrl, state.toAbsolutePath());
  }

  @Test
  void windowedDiffReconcilesAcrossBoundaries() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("dst").toAbsolutePath() + ";MODE=MySQL";

    try (Connection src = open("src"); Connection dst = open("dst")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
          rows.add("(" + i + ", 'v" + i + "', " + i + ".50)");
        }
        st.execute("INSERT INTO t VALUES " + String.join(", ", rows));
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
          // 3 and 17 missing (inserts); 12 stale (rewrite); 26..30 target-only (deletes).
          if (i == 3 || i == 17) {
            continue;
          }
          String nm = i == 12 ? "OLD" : ("v" + i);
          String amt = i == 12 ? "99.99" : (i + ".50");
          rows.add("(" + i + ", '" + nm + "', " + amt + ")");
        }
        st.execute("INSERT INTO t VALUES " + String.join(", ", rows));
      }
    }

    JobConfig config = ConfigLoader.fromYaml(yaml(srcUrl, dstUrl, tmp.resolve("st")));

    SyncResult first = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(first.success(), first.message());
    String msg = first.message();
    assertTrue(msg.contains("streaming"), "streaming path engaged: " + msg);
    assertTrue(msg.contains("insert=2"), "rows 3 and 17 inserted: " + msg);
    assertTrue(msg.contains("delete=5"), "rows 26-30 deleted: " + msg);
    assertTrue(msg.contains("rewrite=1"), "row 12 rewritten: " + msg);
    assertEquals(3, first.report().rowsWritten() - first.report().rowsDeleted(),
        "8 written rows = 2 inserts + 1 rewrite + 5 delete markers");
    assertEquals(1, first.report().rowsRewritten(), "only the stale row is rewritten");
    assertEquals(5, first.report().rowsDeleted());

    assertEquals(25, countRows(dstUrl), "target converges to the source's 25 rows");
    assertEquals("v12", name(dstUrl, 12), "stale content corrected");
    assertEquals("v3", name(dstUrl, 3), "missing row restored");
    assertFalse(exists(dstUrl, 26), "target-only row removed");
    assertFalse(exists(dstUrl, 30), "target-only row removed");

    // Convergence: a second pass over the reconciled table must write nothing at all.
    SyncResult second = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(second.success(), second.message());
    assertTrue(second.message().contains("streaming"), "still streaming: " + second.message());
    assertEquals(0, second.report().rowsWritten(), "no writes on a converged table");
    assertEquals(0, second.report().rowsDeleted(), "no deletes on a converged table");
    assertEquals(25, countRows(dstUrl));
  }

  @Test
  void emptyTargetStreamsTheWholeSourceAsInserts() throws Exception {
    String srcUrl = "jdbc:h2:" + tmp.resolve("src2").toAbsolutePath() + ";MODE=MySQL";
    String dstUrl = "jdbc:h2:" + tmp.resolve("dst2").toAbsolutePath() + ";MODE=MySQL";
    try (Connection src = open("src2"); Connection dst = open("dst2")) {
      try (Statement st = src.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
        List<String> rows = new ArrayList<>();
        for (int i = 1; i <= 23; i++) {
          rows.add("(" + i + ", 'v" + i + "', " + i + ".00)");
        }
        st.execute("INSERT INTO t VALUES " + String.join(", ", rows));
      }
      try (Statement st = dst.createStatement()) {
        st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), amount DECIMAL(10,2))");
      }
    }
    JobConfig config = ConfigLoader.fromYaml(yaml(srcUrl, dstUrl, tmp.resolve("st2")));
    SyncResult result = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(result.success(), result.message());
    assertTrue(result.message().contains("insert=23"), "all 23 rows inserted: " + result.message());
    assertEquals(23, countRows(dstUrl));
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
