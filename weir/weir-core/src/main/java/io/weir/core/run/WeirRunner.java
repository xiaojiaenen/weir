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
import io.weir.spi.ShardProgress;
import io.weir.spi.StateStore;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

  static RowWriterFactory resolveFactory(JobConfig config) {
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
      // Shared totals so per-thread sinks in the parallel FULL path roll up into one number.
      AtomicLong writtenTotal = new AtomicLong();
      List<DataRow> sharedSample = new ArrayList<>();
      BatchSink sink = new BatchSink(writer, batch, metrics, writtenTotal, sharedSample);

      JdbcExtractor.ExtractStats stats;
      long extractStart = System.currentTimeMillis();
      if (mode == RunMode.FULL) {
        if (config.runtime.fullCheckpoint) {
          stats = runFullCheckpointed(extractor, sink, state, findings, writtenTotal, sharedSample);
        } else {
          stats = extractor.extractFull(sink);
        }
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

  /**
   * FULL sync with per-shard checkpointing: every shard whose rows have been fully written is
   * recorded, so a crashed run resumes from the first unfinished shard instead of re-scanning the
   * table. Progress is keyed by a plan fingerprint — if the shard plan changes, the old progress
   * is discarded rather than trusted. Once every shard has landed the progress is cleared and the
   * accumulated watermark is committed exactly like the plain path.
   */
  private JdbcExtractor.ExtractStats runFullCheckpointed(
      JdbcExtractor extractor,
      BatchSink sink,
      StateStore state,
      List<String> findings,
      AtomicLong writtenTotal,
      List<DataRow> sharedSample)
      throws SQLException {
    io.weir.core.extract.SplitPlanner.Plan plan = extractor.planShards();
    plan.warnings().forEach(w -> log.warn("weir split {}", w));
    List<io.weir.core.extract.Shard> shards = plan.shards();
    String planId = FullShards.planId(plan, config, extractor.dialect());
    ShardProgress progress =
        state
            .loadFullProgress(config.name)
            .filter(p -> p.planId().equals(planId))
            .orElseGet(() -> ShardProgress.empty(planId));
    if (!progress.done().isEmpty()) {
      findings.add(
          "full checkpoint: resuming with "
              + progress.done().size()
              + "/"
              + shards.size()
              + " shards already done");
    }

    JobConfig.SourceConfig.ReadConfig.SplitConfig splits = config.source.read.splits;
    boolean parallel =
        shards.size() > 1
            && (splits != null && "parallel".equalsIgnoreCase(splits.mode)
                || config.runtime.extractThreads > 1);
    int threads =
        Math.max(
            1,
            Math.min(
                parallel ? config.runtime.extractThreads : 1,
                Math.max(1, config.runtime.maxConcurrentQueries)));
    int batch = Math.max(1, config.runtime.writeBatchSize);

    JdbcExtractor.ExtractStats stats = new JdbcExtractor.ExtractStats();
    ShardProgress[] current = {progress};
    Object progressLock = new Object();
    java.util.List<SQLException> failures = new java.util.ArrayList<>();
    java.util.List<Integer> todo = new ArrayList<>();
    for (int i = 0; i < shards.size(); i++) {
      if (!progress.done().contains(i)) {
        todo.add(i);
      }
    }

    if (threads <= 1 || todo.size() <= 1) {
      for (int index : todo) {
        runAndCheckpointOne(extractor, sink, state, plan, planId, index, stats, current, progressLock);
      }
    } else {
      // Each worker owns its writer/connection so JDBC writes no longer serialise behind one
      // shared BatchSink — with N threads the write throughput scales with the pool.
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      List<Future<?>> futures = new ArrayList<>();
      try {
        for (int index : todo) {
          futures.add(
              executor.submit(
                  () -> {
                    try (RowWriter localWriter = writerFactory.create(config)) {
                      localWriter.open();
                      BatchSink localSink =
                          new BatchSink(localWriter, batch, metrics, writtenTotal, sharedSample);
                      runAndCheckpointOne(
                          extractor,
                          localSink,
                          state,
                          plan,
                          planId,
                          index,
                          stats,
                          current,
                          progressLock);
                    } catch (SQLException | RuntimeException e) {
                      synchronized (failures) {
                        failures.add(
                            e instanceof SQLException sql
                                ? sql
                                : new SQLException("shard extract failed: " + e.getMessage(), e));
                      }
                    }
                  }));
        }
        for (Future<?> f : futures) {
          try {
            f.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("parallel full extract interrupted", e);
          } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof SQLException sql) {
              throw sql;
            }
            throw new SQLException("shard extract failed: " + cause.getMessage(), cause);
          }
        }
      } finally {
        executor.shutdown();
        try {
          executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (!failures.isEmpty()) {
        // Progress for the shards that did land is already persisted — the next run resumes.
        throw failures.get(0);
      }
    }

    stats.maxWatermark = current[0].maxWatermark();
    state.clearFullProgress(config.name);
    findings.add("full checkpoint: " + shards.size() + "/" + shards.size() + " shards done");
    return stats;
  }

  /** Extract one shard, flush it, then record it done — or leave progress untouched on failure. */
  private void runAndCheckpointOne(
      JdbcExtractor extractor,
      BatchSink sink,
      StateStore state,
      io.weir.core.extract.SplitPlanner.Plan plan,
      String planId,
      int index,
      JdbcExtractor.ExtractStats stats,
      ShardProgress[] current,
      Object progressLock)
      throws SQLException {
    JdbcExtractor.ExtractStats one = FullShards.runShard(config, extractor, plan, index, sink);
    // Flush outside the lock: each sink owns its writer, so shards write concurrently and only the
    // progress bookkeeping is serialised.
    sink.flush();
    synchronized (progressLock) {
      current[0] = current[0].withShardDone(index, one.maxWatermark);
      state.saveFullProgress(config.name, current[0]);
      stats.merge(one);
      log.info(
          "weir full shard {}/{} done rows={} wm={}",
          index + 1,
          plan.shards().size(),
          one.rows,
          one.maxWatermark);
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
   * Git-style reconciliation: insert / delete / rewrite.
   *
   * <p>Single-column keys use a streaming windowed compare: the target is read in key-ordered
   * keyset pages and the source is probed once per page with an indexed
   * {@code pk > ? AND pk <= ?} range, so memory stays O(windowRows) and huge tables reconcile
   * without ever being loaded whole. Composite keys keep the legacy whole-table scan — the keyset
   * predicate for multi-column keys is dialect-specific and not worth the surface here.
   */
  private JdbcExtractor.ExtractStats reconcile(
      JdbcExtractor extractor, BatchSink sink, List<String> pkCols, List<String> findings)
      throws SQLException {
    if (pkCols.size() == 1) {
      return reconcileStreaming(extractor, sink, pkCols.get(0), findings);
    }
    return reconcileWholeTable(extractor, sink, pkCols, findings);
  }

  /** Memory-bounded path: one target page at a time, source probed per window. */
  private JdbcExtractor.ExtractStats reconcileStreaming(
      JdbcExtractor extractor, BatchSink sink, String pk, List<String> findings)
      throws SQLException {
    JdbcExtractor.ExtractStats stats = new JdbcExtractor.ExtractStats();
    boolean compareContent = config.quality.compareRowContent;
    int window = Math.max(50, config.runtime.diffWindowRows);

    long inserts = 0;
    long deletes = 0;
    long rewrites = 0;
    long[] seen = {0};
    boolean compareColsKnown = false;
    List<String> compareCols = List.of();

    // streamByPk is synchronous: it fills this list page by page, in key order, before returning.
    List<List<DataRow>> pages = new ArrayList<>();
    TargetReader.streamByPk(config, pk, List.of(), window, pages::add);

    Object prevKey = null;
    for (List<DataRow> page : pages) {
      DataRow pageLast = page.get(page.size() - 1);
      Object windowEnd = pageLast.get(pk);
      Object windowStart = prevKey;

      Map<String, DataRow> targetRows = new LinkedHashMap<>();
      for (DataRow t : page) {
        targetRows.put(PkDiff.keyOf(t, List.of(pk)), t);
      }
      Map<String, DataRow> sourceRows = new LinkedHashMap<>();
      extractor.extractKeyWindow(
          pk,
          windowStart,
          windowEnd,
          row -> {
            seen[0]++;
            sourceRows.put(PkDiff.keyOf(row, List.of(pk)), row);
          });

      if (!compareColsKnown && compareContent) {
        DataRow srcSample = sourceRows.isEmpty() ? null : sourceRows.values().iterator().next();
        DataRow tgtSample = targetRows.isEmpty() ? null : targetRows.values().iterator().next();
        compareCols = PkDiff.compareColumns(srcSample, tgtSample);
        compareColsKnown = true;
      }

      for (Map.Entry<String, DataRow> e : sourceRows.entrySet()) {
        DataRow t = targetRows.get(e.getKey());
        if (t == null) {
          sink.accept(e.getValue());
          inserts++;
        } else if (compareContent && PkDiff.rowDiffers(e.getValue(), t, List.of(pk), compareCols)) {
          sink.accept(e.getValue());
          rewrites++;
        }
      }
      for (Map.Entry<String, DataRow> e : targetRows.entrySet()) {
        if (!sourceRows.containsKey(e.getKey())) {
          sink.accept(PkDiff.deleteMarker(PkDiff.pkValues(e.getValue(), List.of(pk)), List.of(pk)));
          deletes++;
        }
      }
      prevKey = windowEnd;
    }

    // Tail: every source row past the last target key can only be an insert — stream it straight
    // through without materialising it. When the target was empty this is the entire source.
    long[] tailInserts = {0};
    extractor.extractKeyWindow(
        pk,
        prevKey,
        null,
        row -> {
          seen[0]++;
          sink.accept(row);
          tailInserts[0]++;
        });
    inserts += tailInserts[0];

    stats.rows = seen[0];
    sink.flush();
    metrics.addDeleted(deletes);
    metrics.addRewritten(rewrites);
    findings.add(
        "pk_diff streaming window="
            + window
            + " source="
            + seen[0]
            + " insert="
            + inserts
            + " delete="
            + deletes
            + " rewrite="
            + rewrites);
    log.info("weir pk_diff {}", findings.get(findings.size() - 1));
    return stats;
  }

  /** Legacy path for composite keys: both sides loaded whole. Memory O(table). */
  private JdbcExtractor.ExtractStats reconcileWholeTable(
      JdbcExtractor extractor, BatchSink sink, List<String> pkCols, List<String> findings)
      throws SQLException {
    JdbcExtractor.ExtractStats stats = new JdbcExtractor.ExtractStats();
    findings.add("pk_diff composite key — using whole-table compare (memory O(table))");

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
    /** Cross-sink aggregation: parallel shards each own a writer but share one total. */
    private final AtomicLong sharedWritten;
    private final List<DataRow> sharedSample;

    BatchSink(RowWriter writer, int batchSize, RunMetrics metrics) {
      this(writer, batchSize, metrics, null, null);
    }

    BatchSink(
        RowWriter writer,
        int batchSize,
        RunMetrics metrics,
        AtomicLong sharedWritten,
        List<DataRow> sharedSample) {
      this.writer = writer;
      this.batchSize = batchSize;
      this.metrics = metrics;
      this.pending = new ArrayList<>(batchSize);
      this.sharedWritten = sharedWritten;
      this.sharedSample = sharedSample;
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
      if (sharedWritten != null) {
        sharedWritten.addAndGet(batch.size());
      }
      if (sharedSample != null) {
        synchronized (sharedSample) {
          if (sharedSample.size() < SAMPLE_CAP) {
            int room = SAMPLE_CAP - sharedSample.size();
            sharedSample.addAll(batch.subList(0, Math.min(room, batch.size())));
          }
        }
      } else if (recent.size() < SAMPLE_CAP) {
        int room = SAMPLE_CAP - recent.size();
        recent.addAll(batch.subList(0, Math.min(room, batch.size())));
      }
    }

    long written() {
      return sharedWritten != null ? sharedWritten.get() : written.get();
    }

    List<DataRow> recent() {
      if (sharedSample != null) {
        synchronized (sharedSample) {
          return List.copyOf(sharedSample);
        }
      }
      synchronized (lock) {
        return List.copyOf(recent);
      }
    }
  }
}
