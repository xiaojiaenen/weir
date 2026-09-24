package io.weir.spi;

import io.weir.api.RunReport;
import io.weir.model.Watermark;
import java.util.List;
import java.util.Optional;

/** Durable cursor + run history. */
public interface StateStore extends AutoCloseable {

  Optional<Watermark> loadWatermark(String jobId);

  void saveWatermark(String jobId, Watermark watermark);

  /** Drop the cursor so the next incremental run bootstraps from the start. */
  default void resetWatermark(String jobId) {
    saveWatermark(jobId, Watermark.empty());
  }

  /** Persist one run outcome (legacy flat form). */
  void saveRun(
      String jobId,
      String runId,
      String status,
      long rowsRead,
      long rowsWritten,
      String message);

  /** Persist a full run report; preferred over {@link #saveRun}. */
  default void saveReport(RunReport report) {
    saveRun(
        report.jobId(),
        report.runId(),
        report.status(),
        report.rowsRead(),
        report.rowsWritten(),
        report.message());
  }

  /** Most recent run reports, newest first. */
  default List<RunReport> loadRuns(String jobId, int limit) {
    return List.of();
  }

  /** All known job watermarks (for CLI {@code state} and ops). */
  default java.util.Map<String, Watermark> loadAllWatermarks() {
    return java.util.Map.of();
  }

  /**
   * Arbitrary per-job metadata — used to remember partition fingerprints between diff passes so an
   * unchanged window can be skipped entirely.
   */
  default void saveSnapshotMeta(String jobId, String key, String value) {}

  /** Read back {@link #saveSnapshotMeta}. Null when never written. */
  default String loadSnapshotMeta(String jobId, String key) {
    return null;
  }

  /**
   * Persist per-shard progress of a full snapshot so a crashed run resumes from the first
   * unfinished shard. Implemented on top of snapshot metadata, so every store supports it.
   */
  default void saveFullProgress(String jobId, ShardProgress progress) {
    saveSnapshotMeta(jobId, "full.planId", progress.planId());
    saveSnapshotMeta(jobId, "full.done", progress.doneAsText());
    saveSnapshotMeta(jobId, "full.wm", ShardProgress.encodeWatermark(progress.maxWatermark()));
  }

  /** Read back {@link #saveFullProgress}; empty when no progress is stored. */
  default Optional<ShardProgress> loadFullProgress(String jobId) {
    String planId = loadSnapshotMeta(jobId, "full.planId");
    if (planId == null || planId.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(
        new ShardProgress(
            planId,
            ShardProgress.parseDone(loadSnapshotMeta(jobId, "full.done")),
            ShardProgress.decodeWatermark(loadSnapshotMeta(jobId, "full.wm"))));
  }

  /** Drop full-sync progress — called once every shard has landed, and by {@code weir reset}. */
  default void clearFullProgress(String jobId) {
    for (String key : ShardProgress.metaKeys()) {
      saveSnapshotMeta(jobId, key, null);
    }
  }

  @Override
  default void close() {}
}
