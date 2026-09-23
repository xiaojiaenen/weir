package io.weir.core.run;

import io.weir.api.RunMetrics;
import io.weir.api.RunReport;
import io.weir.api.RunReportCodec;
import io.weir.api.SyncResult;
import io.weir.api.WeirException;
import io.weir.config.JobConfig;
import io.weir.core.diff.PkDiff;
import io.weir.core.diff.TargetReader;
import io.weir.core.extract.Fingerprint;
import io.weir.core.extract.JdbcExtractor;
import io.weir.core.quality.Preflight;
import io.weir.core.quality.Quality;
import io.weir.core.state.StateStores;
import io.weir.model.DataRow;
import io.weir.model.DeleteDetectMode;
import io.weir.model.RunMode;
import io.weir.model.Watermark;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import io.weir.spi.StateStore;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates one sync run: preflight → extract → idempotent write → advance watermark only after
 * success → reconcile → report.
 *
 * <p>The ordering is the whole consistency story: the cursor never moves until the batch has landed,
 * so a retry re-reads the same window and the target's PK merge absorbs the duplicate. That is
 * "effectively-once" — at-least-once delivery plus an idempotent sink.
 */
public final class WeirRunner {
  private static final Logger log = LoggerFactory.getLogger(WeirRunner.class);

  private final JobConfig config;
  private final RowWriterFactory writerFactory;
  private final RunMetrics metrics = new RunMetrics();
  private volatile RunReport lastReport;

  public WeirRunner(JobConfig config) {
    this(config, resolveFactory(config));
  }

  public WeirRunner(JobConfig config, RowWriterFactory writerFactory) {
    this.config = config;
    this.writerFactory = writerFactory;
  }

  private static RowWriterFactory resolveFactory(JobConfig config) {
    String type = config.target.type;
    for (RowWriterFactory factory : ServiceLoader.load(RowWriterFactory.class)) {
      if (factory.type().equalsIgnoreCase(type)) {
        return factory;
      }
    }
    throw new WeirException(
        "No RowWriterFactory for target.type="
            + type
            + ". Add the matching weir-writer-* module to the classpath.");
  }

  /** Report of the most recent {@link #run} on this instance. */
  public RunReport lastReport() {
    return lastReport;
  }

  public RunMetrics metrics() {
    return metrics;
  }

  // ------------------------------------------------------------------ run

  public SyncResult run(RunMode mode) {
    long start = System.currentTimeMillis();
    String runId = UUID.randomUUID().toString();
    String jobId = config.name;
    metrics.mode(mode);
    if (mode == RunMode.CHECK) {
      SyncResult checked = check(jobId, runId, start);
      lastReport = checked.report();
      return checked;
    }

    RunReport.Builder report =
        RunReport.builder(jobId, runId, mode).startedAt(Instant.now()).status("RUNNING");
    List<String> findings = new ArrayList<>();

    try (StateStore state = StateStores.create(config);
        JdbcExtractor extractor = new JdbcExtractor(config, metrics, null);
        RowWriter writer = writerFactory.create(config)) {

      Watermark startWm =
          mode == RunMode.FULL
              ? Watermark.empty()
              : state.loadWatermark(jobId).orElse(Watermark.empty());
      metrics.startWatermark(startWm);
      report.startWatermark(startWm);

      // Preflight before anything touches the source or target.
      if (config.quality.preflightEnabled) {
        Preflight.Report pf =
            extractor.withConnection(
                conn -> Preflight.check(conn, extractor.dialect(), config, startWm));
        pf.findings().forEach(f -> findings.add(f.toString()));
        pf.findings().stream()
            .filter(f -> f.level() == Preflight.Level.WARN)
            .forEach(f -> log.warn("weir preflight {}", f));
        Preflight.failOnErrors(pf);
      }

      writer.open();
      List<String> pk = config.effectivePrimaryKeys();
      int batch = Math.max(1, config.runtime.writeBatchSize);
      BatchSink sink = new BatchSink(writer, batch, metrics);

      JdbcExtractor.ExtractStats stats;
      long extractStart = System.currentTimeMillis();
      if (mode == RunMode.FULL) {
        stats = extractor.extractFull(sink);
      } else if (mode == RunMode.DIFF) {
        stats = runDiff(extractor, sink, pk, state, findings);
      } else {
        stats = extractor.extractIncremental(startWm, sink);
      }
      sink.flush();
      long extractMs = System.currentTimeMillis() - extractStart;
      metrics.addExtractMs(extractMs);

      Watermark endWm = stats.maxWatermark.isEmpty() ? startWm : stats.maxWatermark;
      // Only advance after every batch has been written successfully.
      state.saveWatermark(jobId, endWm);
      metrics.endWatermark(endWm);

      long dur = System.currentTimeMillis() - start;
      metrics.finish(dur);

      // Reconciliation.
      List<Quality.Report> checks = new ArrayList<>();
      checks.add(Quality.checkRowCount(config, sink.written()));
      checks.add(Quality.checkTargetCount(config, sink.written(), metrics.rowsDeleted()));
      checks.add(Quality.checkWatermarkLag(config, endWm.ts()));
      if (config.quality.sampleHashCheck > 0 && !sink.recent().isEmpty()) {
        checks.add(Quality.checkSampleHash(config, sink.recent(), pk, comparableColumns(sink.recent())));
      }
      boolean qualityOk = true;
      for (Quality.Report c : checks) {
        if (!c.message().endsWith("disabled") && !c.message().contains("skipped")) {
          findings.add((c.ok() ? "OK " : "FAIL ") + c.message());
        }
        if (!c.ok()) {
          qualityOk = false;
        }
      }

      String status =
          !qualityOk
              ? (config.quality.failOnQualityMismatch ? "QUALITY_FAILED" : "QUALITY_WARN")
              : "SUCCESS";
      boolean success = !status.equals("QUALITY_FAILED");
      String message = String.join("; ", findings);
      if (message.isBlank()) {
        message = metrics.summary();
      }

      report
          .status(status)
          .success(success)
          .finishedAt(Instant.now())
          .durationMs(dur)
          .from(metrics)
          .startWatermark(startWm)
          .endWatermark(endWm)
          .lagMillis(metrics.lagMillis())
          .findings(findings)
          .message(message);
      RunReport built = report.build();
      state.saveReport(built);
      writeReportFile(built);
      if (config.runtime.metricsEnabled) {
        log.info(
            "weir run finished job={} run={} mode={} status={} {}",
            jobId,
            runId,
            mode,
            status,
            metrics.summary());
      }
      lastReport = built;
      return new SyncResult(
          jobId,
          runId,
          mode,
          success,
          metrics.rowsRead(),
          sink.written(),
          startWm,
          endWm,
          dur,
          message,
          built);
    } catch (Exception e) {
      long dur = System.currentTimeMillis() - start;
      metrics.finish(dur);
      log.error("weir run failed job={} mode={}", jobId, mode, e);
      RunReport built =
          RunReport.builder(jobId, runId, mode)
              .status("FAILED")
              .success(false)
              .finishedAt(Instant.now())
              .durationMs(dur)
              .from(metrics)
              .findings(findings)
              .message(String.valueOf(e.getMessage()))
              .build();
      lastReport = built;
      try (StateStore state = StateStores.create(config)) {
        state.saveReport(built);
      } catch (Exception ignored) {
        // Never mask the original failure with a state-store problem.
      }
      return new SyncResult(
          jobId,
          runId,
          mode,
          false,
          metrics.rowsRead(),
          metrics.rowsWritten(),
          Watermark.empty(),
          Watermark.empty(),
          dur,
          e.getMessage() == null ? e.toString() : e.getMessage(),
          built);
    }
  }

  // ------------------------------------------------------------------ diff

  private JdbcExtractor.ExtractStats runDiff(
      JdbcExtractor extractor, BatchSink sink, List<String> pk, StateStore state, List<String> findings)
      throws SQLException {
    DeleteDetectMode mode = config.source.read.deleteDetect.mode;
    if (mode == DeleteDetectMode.NONE) {
      findings.add("deleteDetect.mode=none — nothing to do");
      return new JdbcExtractor.ExtractStats();
    }

    if (mode == DeleteDetectMode.SOFT_COLUMN) {
      String soft = config.source.read.deleteDetect.softColumn;
      List<String> trueValues = config.source.read.deleteDetect.softTrueValues;
      JdbcExtractor.ExtractStats stats = new JdbcExtractor.ExtractStats();
      extractor.extractFull(
          row -> {
            stats.observe(Watermark.empty());
            if (PkDiff.isSoftDeleted(row.get(soft), trueValues)) {
              DataRow marker = withOp(row, "d");
              sink.accept(marker);
              metrics.addDeleted(1);
            }
          });
      findings.add("soft_column scan: " + stats.rows + " rows, " + metrics.rowsDeleted() + " deleted");
      return stats;
    }

    List<String> pkCols =
        pk.isEmpty() ? List.of(config.source.read.incremental.idColumn) : pk;

    // Set-based diffing needs to read the target back. Against a write-only sink the target looks
    // empty, and "re-insert everything" is the worst possible interpretation.
    if (!TargetReader.canRead(config)) {
      throw new WeirException(
          "deleteDetect mode "
              + mode
              + " requires a target Weir can scan (target.type=jdbc); '"
              + config.target.type
              + "' is write-only — use deleteDetect.mode=soft_column instead");
    }

    if (mode == DeleteDetectMode.FINGERPRINT) {
      boolean drillDown = shouldDrillDown(extractor, state, pkCols, findings);
      if (!drillDown) {
        findings.add("fingerprint unchanged — skipped row-level diff");
        return new JdbcExtractor.ExtractStats();
      }
      findings.add("fingerprint changed — drilling into row-level diff");
    }

    return reconcile(extractor, sink, pkCols, findings);
  }

  /** Compare this run's fingerprint with the one stored last time. */
  private boolean shouldDrillDown(
      JdbcExtractor extractor, StateStore state, List<String> pkCols, List<String> findings)
      throws SQLException {
    String partitionColumn = config.source.read.deleteDetect.fingerprintColumn;
    Instant to = Instant.now();
    Instant from = to.minus(java.time.Duration.ofDays(1));
    Fingerprint current = extractor.fingerprint(partitionColumn, from, to);
    String key = "fingerprint";
    String previous = state.loadSnapshotMeta(config.name, key);
    state.saveSnapshotMeta(config.name, key, current.count() + ":" + current.hashSum());
    if (previous == null) {
      findings.add("no stored fingerprint yet — full diff");
      return true;
    }
    String[] parts = previous.split(":", 2);
    try {
      long prevCount = Long.parseLong(parts[0]);
      long prevHash = parts.length > 1 ? Long.parseLong(parts[1]) : 0L;
      return !current.matches(new Fingerprint(prevCount, prevHash));
    } catch (NumberFormatException e) {
      return true;
    }
  }

  /**
   * Git-style reconciliation: one source scan, one target read, then insert / delete / rewrite.
   *
   * <p>The target projection depends on whether content comparison is on — reading only PK columns
   * while comparing full rows makes every row look changed, which is exactly the bug this replaces.
   */
  private JdbcExtractor.ExtractStats reconcile(
      JdbcExtractor extractor, BatchSink sink, List<String> pkCols, List<String> findings)
      throws SQLException {
    JdbcExtractor.ExtractStats stats = new JdbcExtractor.ExtractStats();

    Map<String, DataRow> sourceRows = new LinkedHashMap<>();
    extractor.extractFull(row -> sourceRows.put(PkDiff.keyOf(row, pkCols), row));
    stats.rows = sourceRows.size();

    boolean compareContent = config.quality.compareRowContent;
    List<String> targetProjection =
        compareContent ? comparableColumns(new ArrayList<>(sourceRows.values())) : pkCols;
    Map<String, DataRow> targetRows = TargetReader.read(config, pkCols, targetProjection);

    long inserts = 0;
    long deletes = 0;
    long rewrites = 0;
    for (Map.Entry<String, DataRow> e : sourceRows.entrySet()) {
      if (!targetRows.containsKey(e.getKey())) {
        sink.accept(e.getValue());
        inserts++;
      }
    }
    for (Map.Entry<String, DataRow> e : targetRows.entrySet()) {
      if (!sourceRows.containsKey(e.getKey())) {
        sink.accept(PkDiff.deleteMarker(PkDiff.pkValues(e.getValue(), pkCols), pkCols));
        deletes++;
      }
    }
    if (compareContent) {
      List<String> compareCols = PkDiff.compareColumns(
          sourceRows.isEmpty() ? null : sourceRows.values().iterator().next(),
          targetRows.isEmpty() ? null : targetRows.values().iterator().next());
      for (Map.Entry<String, DataRow> e : sourceRows.entrySet()) {
        DataRow t = targetRows.get(e.getKey());
        if (t != null && PkDiff.rowDiffers(e.getValue(), t, pkCols, compareCols)) {
          sink.accept(e.getValue());
          rewrites++;
        }
      }
    }
    sink.flush();
    metrics.addDeleted(deletes);
    metrics.addRewritten(rewrites);
    findings.add(
        "pk_diff source="
            + sourceRows.size()
            + " target="
            + targetRows.size()
            + " insert="
            + inserts
            + " delete="
            + deletes
            + " rewrite="
            + rewrites);
    log.info("weir pk_diff {}", findings.get(findings.size() - 1));
    return stats;
  }

  // ------------------------------------------------------------------ helpers

  private static List<String> comparableColumns(List<DataRow> sample) {
    if (sample == null || sample.isEmpty()) {
      return List.of();
    }
    List<String> cols = new ArrayList<>(sample.get(0).values().keySet());
    cols.remove("_op");
    return List.copyOf(cols);
  }

  private void writeReportFile(RunReport report) {
    String dir = config.runtime.reportPath;
    if (dir == null || dir.isBlank()) {
      return;
    }
    try {
      java.nio.file.Path path = java.nio.file.Path.of(dir);
      java.nio.file.Files.createDirectories(path);
      java.nio.file.Files.writeString(
          path.resolve(report.jobId() + "-" + report.runId() + ".json"),
          RunReportCodec.toPrettyJson(report),
          java.nio.charset.StandardCharsets.UTF_8,
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.TRUNCATE_EXISTING);
    } catch (Exception e) {
      log.warn("cannot write run report file: {}", e.getMessage());
    }
  }

  private static DataRow withOp(DataRow row, String op) {
    Map<String, Object> map = new LinkedHashMap<>(row.values());
    map.put("_op", op);
    return new DataRow(map);
  }

  private SyncResult check(String jobId, String runId, long start) {
    List<String> findings = new ArrayList<>();
    try (StateStore state = StateStores.create(config);
        JdbcExtractor extractor = new JdbcExtractor(config, metrics, null)) {
      config.validate();
      List<String> pk = config.effectivePrimaryKeys();
      if ("merge".equalsIgnoreCase(config.target.writeMode) && pk.isEmpty()) {
        throw new WeirException("merge writeMode requires primary keys");
      }
      Watermark wm = state.loadWatermark(jobId).orElse(Watermark.empty());
      Preflight.Report pf =
          extractor.withConnection(
              conn -> Preflight.check(conn, extractor.dialect(), config, wm));
      pf.findings().forEach(f -> findings.add(f.toString()));
      long dur = System.currentTimeMillis() - start;
      metrics.finish(dur);
      RunReport report =
          RunReport.builder(jobId, runId, RunMode.CHECK)
              .status(pf.hasErrors() ? "PREFLIGHT_FAILED" : "CHECK_OK")
              .success(!pf.hasErrors())
              .finishedAt(Instant.now())
              .durationMs(dur)
              .findings(findings)
              .message(
                  pf.hasErrors()
                      ? "preflight errors: "
                          + pf.findings().stream()
                              .filter(f -> f.level() == Preflight.Level.ERROR)
                              .map(Preflight.Finding::message)
                              .reduce((a, b) -> a + "; " + b)
                              .orElse("")
                      : "CHECK OK"
                          + (pf.hasWarnings()
                              ? " with warnings: "
                                  + pf.findings().stream()
                                      .filter(f -> f.level() == Preflight.Level.WARN)
                                      .map(Preflight.Finding::message)
                                      .reduce((a, b) -> a + "; " + b)
                                      .orElse("")
                              : ""))
              .build();
      state.saveReport(report);
      return new SyncResult(
          jobId,
          runId,
          RunMode.CHECK,
          !pf.hasErrors(),
          0,
          0,
          wm,
          wm,
          dur,
          report.message(),
          report);
    } catch (Exception e) {
      long dur = System.currentTimeMillis() - start;
      return new SyncResult(
          jobId,
          runId,
          RunMode.CHECK,
          false,
          0,
          0,
          Watermark.empty(),
          Watermark.empty(),
          dur,
          String.valueOf(e.getMessage()),
          RunReport.builder(jobId, runId, RunMode.CHECK)
              .status("FAILED")
              .success(false)
              .durationMs(dur)
              .message(String.valueOf(e.getMessage()))
              .build());
    }
  }

  /**
   * Buffers rows into write batches and tracks what was written, keeping a bounded sample for
   * post-write hash verification.
   */
  static final class BatchSink implements java.util.function.Consumer<DataRow> {
    private static final int SAMPLE_CAP = 5_000;

    private final RowWriter writer;
    private final int batchSize;
    private final RunMetrics metrics;
    private final List<DataRow> pending;
    private final List<DataRow> recent = new ArrayList<>();
    private final AtomicLong written = new AtomicLong();
    private final Object lock = new Object();

    BatchSink(RowWriter writer, int batchSize, RunMetrics metrics) {
      this.writer = writer;
      this.batchSize = batchSize;
      this.metrics = metrics;
      this.pending = new ArrayList<>(batchSize);
    }

    @Override
    public void accept(DataRow row) {
      synchronized (lock) {
        pending.add(row);
        if (pending.size() >= batchSize) {
          flushLocked();
        }
      }
    }

    public void flush() {
      synchronized (lock) {
        flushLocked();
      }
    }

    private void flushLocked() {
      if (pending.isEmpty()) {
        return;
      }
      List<DataRow> batch = List.copyOf(pending);
      pending.clear();
      long t0 = System.currentTimeMillis();
      try {
        writer.writeBatch(batch);
      } catch (SQLException e) {
        throw new WeirException("writeBatch failed", e);
      }
      metrics.addWriteMs(System.currentTimeMillis() - t0);
      metrics.addWritten(batch.size());
      metrics.addBatch();
      written.addAndGet(batch.size());
      if (recent.size() < SAMPLE_CAP) {
        int room = SAMPLE_CAP - recent.size();
        recent.addAll(batch.subList(0, Math.min(room, batch.size())));
      }
    }

    long written() {
      return written.get();
    }

    List<DataRow> recent() {
      synchronized (lock) {
        return List.copyOf(recent);
      }
    }
  }
}
