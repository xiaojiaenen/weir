package io.weir.api;

import io.weir.model.RunMode;
import io.weir.model.Watermark;

/**
 * Per-run observability counters. Reported at the end of every run and persisted with the run
 * record, so operators can answer "how far behind is this job?" without re-reading logs.
 */
public final class RunMetrics {
  private long rowsRead;
  private long rowsWritten;
  private long rowsDeleted;
  private long rowsRewritten;
  private long batches;
  private long retries;
  private long throttledMs;
  private long shards;
  private long failedShards;
  private long extractMs;
  private long writeMs;
  private long durationMs;
  private RunMode mode;
  private Watermark startWatermark;
  private Watermark endWatermark;

  public RunMetrics() {}

  public long rowsRead() {
    return rowsRead;
  }

  public long rowsWritten() {
    return rowsWritten;
  }

  public long rowsDeleted() {
    return rowsDeleted;
  }

  public long rowsRewritten() {
    return rowsRewritten;
  }

  public long batches() {
    return batches;
  }

  public long retries() {
    return retries;
  }

  public long throttledMs() {
    return throttledMs;
  }

  public long shards() {
    return shards;
  }

  public long failedShards() {
    return failedShards;
  }

  public long extractMs() {
    return extractMs;
  }

  public long writeMs() {
    return writeMs;
  }

  public long durationMs() {
    return durationMs;
  }

  public RunMode mode() {
    return mode;
  }

  public Watermark startWatermark() {
    return startWatermark;
  }

  public Watermark endWatermark() {
    return endWatermark;
  }

  public void addRead(long n) {
    rowsRead += n;
  }

  public void addWritten(long n) {
    rowsWritten += n;
  }

  public void addDeleted(long n) {
    rowsDeleted += n;
  }

  public void addRewritten(long n) {
    rowsRewritten += n;
  }

  public void addBatch() {
    batches++;
  }

  public void addRetry() {
    retries++;
  }

  public void addThrottle(long ms) {
    throttledMs += ms;
  }

  public void addShard() {
    shards++;
  }

  public void shards(long v) {
    this.shards = v;
  }

  public void addFailedShard() {
    failedShards++;
  }

  public void addExtractMs(long ms) {
    extractMs += ms;
  }

  public void addWriteMs(long ms) {
    writeMs += ms;
  }

  public void finish(long durationMs) {
    this.durationMs = durationMs;
  }

  public void mode(RunMode mode) {
    this.mode = mode;
  }

  public void startWatermark(Watermark w) {
    this.startWatermark = w;
  }

  public void endWatermark(Watermark w) {
    this.endWatermark = w;
  }

  /** Rows per second across the whole run; 0 when the run was instantaneous. */
  public double rowsPerSecond() {
    if (durationMs <= 0) {
      return 0d;
    }
    return rowsRead * 1000d / durationMs;
  }

  /** Freshness lag in ms: now minus the watermark we advanced to. 0 when unknown. */
  public long lagMillis() {
    if (endWatermark == null || endWatermark.ts() == null) {
      return 0L;
    }
    return Math.max(0L, System.currentTimeMillis() - endWatermark.ts().toEpochMilli());
  }

  /** Compact single-line rendering for logs and CLI. */
  public String summary() {
    return "rowsRead="
        + rowsRead
        + " rowsWritten="
        + rowsWritten
        + " rowsDeleted="
        + rowsDeleted
        + " rowsRewritten="
        + rowsRewritten
        + " batches="
        + batches
        + " retries="
        + retries
        + " shards="
        + shards
        + " extractMs="
        + extractMs
        + " writeMs="
        + writeMs
        + " durationMs="
        + durationMs
        + " rows/s="
        + String.format("%.1f", rowsPerSecond())
        + " lagMs="
        + lagMillis();
  }
}
