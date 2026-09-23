package io.weir.api;

import io.weir.model.RunMode;
import io.weir.model.Watermark;

/** Result of one Weir run. */
public record SyncResult(
    String jobId,
    String runId,
    RunMode mode,
    boolean success,
    long rowsRead,
    long rowsWritten,
    Watermark startWatermark,
    Watermark endWatermark,
    long durationMs,
    String message,
    RunReport report) {

  /** Back-compat constructor for callers that do not carry a full report. */
  public SyncResult(
      String jobId,
      String runId,
      RunMode mode,
      boolean success,
      long rowsRead,
      long rowsWritten,
      Watermark startWatermark,
      Watermark endWatermark,
      long durationMs,
      String message) {
    this(
        jobId,
        runId,
        mode,
        success,
        rowsRead,
        rowsWritten,
        startWatermark,
        endWatermark,
        durationMs,
        message,
        null);
  }

  /** Human-readable one-liner used by the CLI and the DolphinScheduler adapter. */
  public String toLine() {
    return "job="
        + jobId
        + " run="
        + runId
        + " mode="
        + mode
        + " success="
        + success
        + " rowsRead="
        + rowsRead
        + " rowsWritten="
        + rowsWritten
        + " ms="
        + durationMs
        + " msg="
        + message;
  }
}
