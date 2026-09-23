package io.weir.core.extract;

import io.weir.config.JobConfig;
import io.weir.dialect.JdbcDialect;
import io.weir.model.SplitMode;
import io.weir.model.SplitRange;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decides how to cut a snapshot into shards.
 *
 * <p>The naive planner assumed every split column is a numeric id and called {@code rs.getLong()}
 * on MIN/MAX. Against a VARCHAR business key that either throws or silently returns 0 and shard
 * everything into one range. This planner probes the column type first and degrades gracefully.
 */
public final class SplitPlanner {
  private SplitPlanner() {}

  public record Plan(List<Shard> shards, SplitMode resolvedMode, List<String> warnings) {}

  public static Plan plan(
      Connection conn, JdbcDialect dialect, JobConfig.SourceConfig.ReadConfig read)
      throws SQLException {
    JobConfig.SourceConfig.ReadConfig.SplitConfig splits = read.splits;
    List<String> warnings = new ArrayList<>();
    String fromClause = io.weir.core.util.Sql.fromClause(read);
    int parts = Math.max(1, splits == null ? 1 : splits.numPartitions);
    if (splits == null || parts <= 1 || splits.column == null || splits.column.isBlank()) {
      return new Plan(List.of(Shard.single()), SplitMode.NONE, warnings);
    }

    SplitMode requested = splits.strategy == null ? SplitMode.AUTO : splits.strategy;

    // Probe MIN/MAX once — tells us both the bounds and whether the column is numeric.
    long[] minMax = new long[] {0L, 0L};
    boolean probed = false;
    if (requested == SplitMode.AUTO || requested == SplitMode.RANGE) {
      try {
        minMax = dialect.minMax(conn, fromClause, splits.column);
        probed = true;
      } catch (SQLException e) {
        warnings.add("MIN/MAX probe failed on " + splits.column + ": " + e.getMessage());
      }
    }
    boolean numeric = !probed || minMax[0] != Long.MIN_VALUE;

    SplitMode resolved = requested;
    if (requested == SplitMode.AUTO) {
      if (numeric) {
        resolved = SplitMode.RANGE;
      } else if (dialect.supportsHashSplit()) {
        resolved = SplitMode.HASH;
        warnings.add(
            "split column " + splits.column + " is non-numeric; using hash sharding");
      } else {
        resolved = SplitMode.NONE;
        warnings.add(
            "split column "
                + splits.column
                + " is non-numeric and "
                + dialect.name()
                + " cannot hash-partition it; falling back to a single shard");
      }
    } else if (!numeric && (requested == SplitMode.RANGE || requested == SplitMode.MOD)) {
      if (dialect.supportsHashSplit()) {
        resolved = SplitMode.HASH;
        warnings.add(
            "requested "
                + requested
                + " but split column is non-numeric; using hash sharding instead");
      } else {
        resolved = SplitMode.NONE;
        warnings.add(
            "requested " + requested + " but split column is non-numeric; using a single shard");
      }
    }

    if (resolved == SplitMode.NONE) {
      return new Plan(List.of(Shard.single()), resolved, warnings);
    }
    if (resolved == SplitMode.HASH || resolved == SplitMode.MOD) {
      List<Shard> shards = new ArrayList<>(parts);
      for (int i = 0; i < parts; i++) {
        shards.add(resolved == SplitMode.HASH ? Shard.ofHash(parts, i) : Shard.ofMod(parts, i));
      }
      return new Plan(shards, resolved, warnings);
    }
    return new Plan(rangeShards(parts, minMax), SplitMode.RANGE, warnings);
  }

  private static List<Shard> rangeShards(int parts, long[] minMax) {
    long min = minMax[0];
    long max = minMax[1];
    if (max < min) {
      return List.of(Shard.single());
    }
    long span = max - min + 1;
    long step = Math.max(1L, (span + parts - 1) / parts);
    List<Shard> shards = new ArrayList<>();
    long lo = min;
    while (lo <= max) {
      long hi = lo + step;
      if (hi < lo) { // overflow: last shard is open-ended
        shards.add(Shard.ofRange(new SplitRange(lo, null)));
        break;
      }
      // Last shard takes the remainder so MAX(id) is always covered.
      shards.add(Shard.ofRange(new SplitRange(lo, hi > max ? null : hi)));
      lo = hi;
      if (hi > max) {
        break;
      }
    }
    if (shards.isEmpty()) {
      shards.add(Shard.single());
    }
    return shards;
  }
}
