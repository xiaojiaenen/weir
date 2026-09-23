package io.weir.core.quality;

import io.weir.config.JobConfig;
import io.weir.core.diff.TargetReader;
import io.weir.model.DataRow;
import io.weir.model.RowValues;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Post-run reconciliation: row counts, watermark lag, and optional sampled content hashing against
 * the target.
 *
 * <p>These are advisory by default. Set {@code quality.failOnQualityMismatch} to turn a mismatch
 * into a hard failure — useful right after a backfill, risky for a job with a lagging target.
 */
public final class Quality {
  private Quality() {}

  public record Report(boolean ok, String message) {
    public static Report ok(String message) {
      return new Report(true, message);
    }

    public static Report fail(String message) {
      return new Report(false, message);
    }

    public static Report skip(String message) {
      return new Report(true, message);
    }
  }

  public static Report checkRowCount(JobConfig config, long rowsWritten) {
    if (!config.quality.rowCountCheck) {
      return Report.skip("row_count_check disabled");
    }
    if (rowsWritten < 0) {
      return Report.fail("rowsWritten is negative");
    }
    return Report.ok("rowsWritten=" + rowsWritten);
  }

  /**
   * Sanity-check the target's own COUNT(*) against what we just wrote.
   *
   * <p>Only meaningful when this run deleted nothing: deletes legitimately shrink the target below
   * the number of rows written, and flagging that as "lost rows" would make every diff pass fail.
   */
  public static Report checkTargetCount(JobConfig config, long rowsWritten, long rowsDeleted) {
    if (!config.quality.rowCountCheck
        || !"jdbc".equalsIgnoreCase(config.target.type)
        || config.target.url == null) {
      return Report.skip("target count check not applicable");
    }
    long target;
    try {
      target = TargetReader.count(config);
    } catch (SQLException e) {
      return Report.fail("cannot count target: " + e.getMessage());
    }
    if (rowsDeleted == 0 && rowsWritten > target) {
      return Report.fail(
          "rowsWritten=" + rowsWritten + " exceeds target count=" + target + " (lost rows?)");
    }
    return Report.ok("targetCount=" + target + " thisRun=" + rowsWritten);
  }

  public static Report checkWatermarkLag(JobConfig config, java.time.Instant endTs) {
    if (endTs == null || config.quality.watermarkMaxLagMs <= 0) {
      return Report.skip("watermark lag check skipped");
    }
    long lag = System.currentTimeMillis() - endTs.toEpochMilli();
    if (lag > config.quality.watermarkMaxLagMs) {
      return Report.fail("watermark lag " + lag + "ms > " + config.quality.watermarkMaxLagMs + "ms");
    }
    return Report.ok("watermark lag " + lag + "ms");
  }

  /**
   * Sample rows we just wrote and re-read them from a JDBC target, comparing content hashes.
   *
   * <p>This is the check that catches a silent type-coercion bug: the row count is right but the
   * values are subtly wrong.
   */
  public static Report checkSampleHash(
      JobConfig config, List<DataRow> written, List<String> pk, List<String> columns) {
    double fraction = config.quality.sampleHashCheck;
    if (fraction <= 0.0 || written.isEmpty()) {
      return Report.skip("sample hash check disabled");
    }
    if (!"jdbc".equalsIgnoreCase(config.target.type) || config.target.url == null || pk.isEmpty()) {
      return Report.skip("sample hash check needs a jdbc target with primary keys");
    }

    int sampleSize =
        Math.min(written.size(), Math.max(1, (int) Math.ceil(written.size() * fraction)));
    List<DataRow> sample = new ArrayList<>(sampleSize);
    // Deterministic-ish spread across the batch without shuffling the whole list.
    double stride = (double) written.size() / sampleSize;
    for (int i = 0; i < sampleSize; i++) {
      int idx = Math.min(written.size() - 1, (int) Math.round(i * stride));
      DataRow row = written.get(idx);
      if (row != null && !io.weir.core.diff.PkDiff.isDeleted(row)) {
        sample.add(row);
      }
    }
    if (sample.isEmpty()) {
      return Report.skip("no rows to sample");
    }

    Map<String, DataRow> targetRows;
    try {
      targetRows = TargetReader.read(config, pk, columns);
    } catch (SQLException e) {
      return Report.fail("cannot re-read target for sample hash: " + e.getMessage());
    }

    int mismatched = 0;
    int missing = 0;
    for (DataRow src : sample) {
      DataRow tgt = targetRows.get(io.weir.core.diff.PkDiff.keyOf(src, pk));
      if (tgt == null) {
        missing++;
        continue;
      }
      if (io.weir.core.diff.PkDiff.rowDiffers(src, tgt, pk, columns)) {
        mismatched++;
      }
    }
    if (missing > 0 || mismatched > 0) {
      return Report.fail(
          "sample hash mismatch: sampled="
              + sample.size()
              + " missingOnTarget="
              + missing
              + " contentMismatch="
              + mismatched);
    }
    return Report.ok("sample hash verified over " + sample.size() + " rows");
  }

  /** Pick a deterministic-ish sample when a caller wants the spread without randomness. */
  public static List<DataRow> sample(List<DataRow> rows, double fraction) {
    if (rows.isEmpty() || fraction <= 0) {
      return List.of();
    }
    if (fraction >= 1.0) {
      return List.copyOf(rows);
    }
    int n = Math.max(1, (int) Math.ceil(rows.size() * fraction));
    List<DataRow> out = new ArrayList<>(n);
    var random = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++) {
      out.add(rows.get(random.nextInt(rows.size())));
    }
    return out;
  }

  /** Stable hash of a row over the given columns — exposed for tests and reporting. */
  public static long hashOf(DataRow row, List<String> columns) {
    return RowValues.hash(row, columns);
  }
}
