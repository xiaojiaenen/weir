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
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@code weir check} must catch configuration mistakes before a run touches the source. */
class PreflightTest {

  @TempDir Path tmp;

  private String db(String name) {
    return "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL";
  }

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(db(name), "sa", "");
  }

  private String yaml(String url, String columns, String incrementalColumn) {
    return """
        name: preflight-job
        mode: incremental
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [%s]
            primaryKey: [id]
            incremental:
              strategy: update_time_id
              column: %s
              idColumn: id
              overlap: 0s
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
            columns,
            incrementalColumn,
            tmp.resolve("out.jsonl").toAbsolutePath(),
            tmp.resolve("st").toAbsolutePath());
  }

  private void createTable(String name) throws Exception {
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      st.execute("INSERT INTO t VALUES (1,'a',TIMESTAMP '2024-01-01 10:00:00')");
    }
  }

  @Test
  void validConfigPassesCheck() throws Exception {
    createTable("ok");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("ok"), "id, name, update_time", "update_time"));
    SyncResult r = new WeirRunner(config).run(RunMode.CHECK);
    assertTrue(r.success(), r.message());
  }

  @Test
  void missingIncrementalColumnFailsCheck() throws Exception {
    createTable("bad");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("bad"), "id, name, update_time", "nope"));
    SyncResult r = new WeirRunner(config).run(RunMode.CHECK);
    assertFalse(r.success(), "a cursor column that does not exist must fail fast");
    assertTrue(r.message().contains("nope"), "the finding names the offending column: " + r.message());
  }

  @Test
  void zeroOverlapIsReportedAsAWarning() throws Exception {
    createTable("warn");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("warn"), "id, name, update_time", "update_time"));
    SyncResult r = new WeirRunner(config).run(RunMode.CHECK);
    assertTrue(r.success(), "a warning alone does not fail the check");
    assertTrue(
        r.report().qualityFindings().stream().anyMatch(f -> f.contains("NO_OVERLAP")),
        "overlap=0 on a timestamp cursor is called out: " + r.report().qualityFindings());
  }

  @Test
  void watermarkAheadOfSourceIsDetected() throws Exception {
    String name = "regress";
    createTable(name);
    JobConfig config = ConfigLoader.fromYaml(yaml(db(name), "id, name, update_time", "update_time"));

    // Run once so a watermark is stored, then rewind the source's clock.
    new WeirRunner(config).run(RunMode.INCREMENTAL);
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("UPDATE t SET update_time = TIMESTAMP '2000-01-01 00:00:00'");
    }
    SyncResult r = new WeirRunner(config).run(RunMode.CHECK);
    assertTrue(
        r.report().qualityFindings().stream().anyMatch(f -> f.contains("WATERMARK_REGRESSION")),
        "a source restored to an earlier point is flagged: " + r.report().qualityFindings());
  }

  @Test
  void unknownSourceTableFailsCheck() throws Exception {
    createTable("tbl");
    String yml = yaml(db("tbl"), "id, name, update_time", "update_time").replace("table: t", "table: missing_t");
    JobConfig config = ConfigLoader.fromYaml(yml);
    SyncResult r = new WeirRunner(config).run(RunMode.CHECK);
    assertFalse(r.success(), "a non-existent source table must fail the check");
  }

  @Test
  void freshJobBootstrapReadsEverything() throws Exception {
    String name = "boot";
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
      st.execute(
          "INSERT INTO t VALUES (1,'a',TIMESTAMP '"
              + Timestamp.from(t0)
              + "'),(2,'b',TIMESTAMP '"
              + Timestamp.from(t0.plusSeconds(30))
              + "')");
    }
    JobConfig config = ConfigLoader.fromYaml(yaml(db(name), "id, name, update_time", "update_time"));
    SyncResult r = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(r.success(), r.message());
    assertEquals(2, r.rowsRead(), "with no stored watermark the first incremental pass bootstraps");
  }
}
