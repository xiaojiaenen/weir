package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.RunReport;
import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.core.state.StateStores;
import io.weir.model.RunMode;
import io.weir.model.Watermark;
import io.weir.spi.StateStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Operators need to answer "did it run, how far did it get, how stale is it?" without reading logs.
 * That is what the run report and history are for.
 */
class RunObservabilityTest {

  @TempDir Path tmp;

  private String db(String name) {
    return "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL";
  }

  private Connection open(String name) throws Exception {
    return DriverManager.getConnection(db(name), "sa", "");
  }

  private String yaml(String url, Path reportDir) {
    return """
        name: obs-job
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
              strategy: id
              idColumn: id
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        runtime:
          writeBatchSize: 2
          reportPath: %s
        """
        .formatted(
            url,
            tmp.resolve("out.jsonl").toAbsolutePath(),
            tmp.resolve("st").toAbsolutePath(),
            reportDir.toAbsolutePath());
  }

  private void create(String name, int rows) throws Exception {
    try (Connection c = open(name); Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20), update_time TIMESTAMP)");
      for (int i = 1; i <= rows; i++) {
        st.execute("INSERT INTO t VALUES (" + i + ",'n" + i + "',TIMESTAMP '2024-01-01 10:00:00')");
      }
    }
  }

  @Test
  void reportCarriesCountersAndTiming() throws Exception {
    create("c1", 5);
    JobConfig config = ConfigLoader.fromYaml(yaml(db("c1"), tmp.resolve("reports")));
    SyncResult r = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(r.success(), r.message());

    RunReport report = r.report();
    assertNotNull(report, "every run produces a report");
    assertEquals("obs-job", report.jobId());
    assertEquals(RunMode.INCREMENTAL, report.mode());
    assertEquals("SUCCESS", report.status());
    assertEquals(5, report.rowsRead());
    assertEquals(5, report.rowsWritten());
    assertEquals(3, report.batches(), "5 rows at writeBatchSize=2 is 3 batches");
    assertTrue(report.durationMs() >= 0);
    assertTrue(report.finishedAt() != null);
    assertTrue(report.startedAt() != null);
    // 5 rows in id mode leaves the cursor at id=5.
    assertEquals(5L, report.endWatermark().id());
  }

  @Test
  void runHistoryIsQueryable() throws Exception {
    create("c2", 3);
    JobConfig config = ConfigLoader.fromYaml(yaml(db("c2"), tmp.resolve("reports2")));
    new WeirRunner(config).run(RunMode.INCREMENTAL);
    new WeirRunner(config).run(RunMode.INCREMENTAL);

    try (StateStore state = StateStores.create(config)) {
      List<RunReport> runs = state.loadRuns("obs-job", 10);
      assertEquals(2, runs.size(), "both runs are recorded");
      // Newest first.
      assertTrue(
          !runs.get(0).startedAt().isBefore(runs.get(1).startedAt()),
          "history is returned newest-first");
      assertEquals(
          0, runs.get(0).rowsRead(), "the newest pass has nothing new to read (cursor advanced)");
      assertEquals(3, runs.get(1).rowsRead(), "the older pass is the one that loaded the table");
    }
  }

  @Test
  void reportFileIsWrittenWhenConfigured() throws Exception {
    create("c3", 2);
    Path reportDir = tmp.resolve("reports3");
    JobConfig config = ConfigLoader.fromYaml(yaml(db("c3"), reportDir));
    SyncResult r = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertTrue(r.success(), r.message());
    try (Stream<Path> files = Files.list(reportDir)) {
      List<Path> json = files.filter(p -> p.toString().endsWith(".json")).toList();
      assertFalse(json.isEmpty(), "a JSON report lands in runtime.reportPath");
      String body = Files.readString(json.get(0));
      assertTrue(body.contains("\"jobId\" : \"obs-job\""), "report is machine readable: " + body);
      assertTrue(body.contains("rowsWritten"), "report includes counters");
    }
  }

  @Test
  void resetWatermarkForcesABootstrap() throws Exception {
    create("c4", 4);
    JobConfig config = ConfigLoader.fromYaml(yaml(db("c4"), tmp.resolve("reports4")));
    SyncResult first = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertEquals(4, first.rowsRead());

    try (StateStore state = StateStores.create(config)) {
      assertFalse(state.loadWatermark("obs-job").orElse(Watermark.empty()).isEmpty());
      state.resetWatermark("obs-job");
      assertTrue(state.loadWatermark("obs-job").orElse(Watermark.empty()).isEmpty());
    }
    SyncResult second = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertEquals(4, second.rowsRead(), "after a reset everything is re-read");
  }

  @Test
  void failedRunIsRecordedAsFailed() throws Exception {
    create("c5", 2);
    String yml = yaml(db("c5"), tmp.resolve("reports5")).replace("table: t", "table: gone");
    JobConfig config = ConfigLoader.fromYaml(yml);
    SyncResult r = new WeirRunner(config).run(RunMode.INCREMENTAL);
    assertFalse(r.success());
    assertEquals("FAILED", r.report().status());
    try (StateStore state = StateStores.create(config)) {
      List<RunReport> runs = state.loadRuns("obs-job", 10);
      assertTrue(runs.stream().anyMatch(x -> "FAILED".equals(x.status())), "failures are visible too");
    }
  }
}
