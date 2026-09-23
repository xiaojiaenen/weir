package io.weir.api;

import io.weir.model.RunMode;
import io.weir.model.Watermark;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * One persisted run record: outcome, counters, window and quality verdict.
 *
 * <p>This is what {@code weir runs} prints and what DolphinScheduler surfaces after a task, so it
 * must be self-contained (no need to correlate with logs).
 */
public record RunReport(
    String jobId,
    String runId,
    RunMode mode,
    String status,
    boolean success,
    Instant startedAt,
    Instant finishedAt,
    long durationMs,
    long rowsRead,
    long rowsWritten,
    long rowsDeleted,
    long rowsRewritten,
    long batches,
    long retries,
    long shards,
    Watermark startWatermark,
    Watermark endWatermark,
    long lagMillis,
    double rowsPerSecond,
    List<String> qualityFindings,
    String message) {

  public static Builder builder(String jobId, String runId, RunMode mode) {
    return new Builder(jobId, runId, mode);
  }

  /** Mutable builder — reports are assembled incrementally across a run. */
  public static final class Builder {
    private final String jobId;
    private final String runId;
    private final RunMode mode;
    private String status = "RUNNING";
    private boolean success;
    private Instant startedAt = Instant.now();
    private Instant finishedAt;
    private long durationMs;
    private long rowsRead;
    private long rowsWritten;
    private long rowsDeleted;
    private long rowsRewritten;
    private long batches;
    private long retries;
    private long shards;
    private Watermark startWatermark = Watermark.empty();
    private Watermark endWatermark = Watermark.empty();
    private long lagMillis;
    private double rowsPerSecond;
    private final List<String> qualityFindings = new ArrayList<>();
    private String message = "";

    private Builder(String jobId, String runId, RunMode mode) {
      this.jobId = jobId;
      this.runId = runId;
      this.mode = mode;
    }

    public Builder status(String v) {
      this.status = v;
      return this;
    }

    public Builder success(boolean v) {
      this.success = v;
      return this;
    }

    public Builder startedAt(Instant v) {
      this.startedAt = v;
      return this;
    }

    public Builder finishedAt(Instant v) {
      this.finishedAt = v;
      return this;
    }

    public Builder durationMs(long v) {
      this.durationMs = v;
      return this;
    }

    public Builder rowsRead(long v) {
      this.rowsRead = v;
      return this;
    }

    public Builder rowsWritten(long v) {
      this.rowsWritten = v;
      return this;
    }

    public Builder rowsDeleted(long v) {
      this.rowsDeleted = v;
      return this;
    }

    public Builder rowsRewritten(long v) {
      this.rowsRewritten = v;
      return this;
    }

    public Builder batches(long v) {
      this.batches = v;
      return this;
    }

    public Builder retries(long v) {
      this.retries = v;
      return this;
    }

    public Builder shards(long v) {
      this.shards = v;
      return this;
    }

    public Builder startWatermark(Watermark v) {
      this.startWatermark = v;
      return this;
    }

    public Builder endWatermark(Watermark v) {
      this.endWatermark = v;
      return this;
    }

    public Builder lagMillis(long v) {
      this.lagMillis = v;
      return this;
    }

    public Builder rowsPerSecond(double v) {
      this.rowsPerSecond = v;
      return this;
    }

    public Builder addFinding(String finding) {
      if (finding != null && !finding.isBlank()) {
        qualityFindings.add(finding);
      }
      return this;
    }

    public Builder findings(List<String> v) {
      qualityFindings.clear();
      if (v != null) {
        qualityFindings.addAll(v);
      }
      return this;
    }

    public Builder message(String v) {
      this.message = v == null ? "" : v;
      return this;
    }

    public Builder from(RunMetrics metrics) {
      this.rowsRead = metrics.rowsRead();
      this.rowsWritten = metrics.rowsWritten();
      this.rowsDeleted = metrics.rowsDeleted();
      this.rowsRewritten = metrics.rowsRewritten();
      this.batches = metrics.batches();
      this.retries = metrics.retries();
      this.shards = metrics.shards();
      this.lagMillis = metrics.lagMillis();
      this.rowsPerSecond = metrics.rowsPerSecond();
      return this;
    }

    public RunReport build() {
      return new RunReport(
          jobId,
          runId,
          mode,
          status,
          success,
          startedAt,
          finishedAt == null ? Instant.now() : finishedAt,
          durationMs,
          rowsRead,
          rowsWritten,
          rowsDeleted,
          rowsRewritten,
          batches,
          retries,
          shards,
          startWatermark,
          endWatermark,
          lagMillis,
          rowsPerSecond,
          List.copyOf(qualityFindings),
          message);
    }
  }

  /** Human-readable single line for CLI output. */
  public String toLine() {
    return (startedAt == null ? "-" : startedAt)
        + "  "
        + status
        + "  mode="
        + mode
        + "  run="
        + (runId == null ? "-" : runId.length() > 8 ? runId.substring(0, 8) : runId)
        + "  rows="
        + rowsWritten
        + "  ms="
        + durationMs
        + "  lagMs="
        + lagMillis
        + (message == null || message.isBlank() ? "" : "  msg=" + message);
  }
}
