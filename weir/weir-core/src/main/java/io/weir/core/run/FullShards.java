package io.weir.core.run;

import io.weir.config.JobConfig;
import io.weir.core.extract.JdbcExtractor;
import io.weir.core.extract.Shard;
import io.weir.core.extract.SplitPlanner;
import io.weir.dialect.JdbcDialect;
import io.weir.model.DataRow;
import java.sql.SQLException;
import java.util.function.Consumer;

/**
 * Shared plumbing for the full-snapshot shard plan: a stable plan fingerprint and the
 * run-one-shard primitive that both the checkpointed runner and external engines use.
 *
 * <p>{@link #planId} is what makes resume safe: progress recorded under one plan is discarded the
 * moment the plan changes (table, projection, split column or shard bounds), so a stale checkpoint
 * can never skip rows that the new plan is responsible for.
 */
public final class FullShards {
  private FullShards() {}

  /**
   * Fingerprint of the shard plan. Includes the source identity, projection and every shard's
   * dialect-rendered predicate, so any change that shifts row ownership invalidates progress.
   */
  public static String planId(SplitPlanner.Plan plan, JobConfig config, JdbcDialect dialect) {
    JobConfig.SourceConfig.ReadConfig read = config.source.read;
    StringBuilder sb = new StringBuilder();
    sb.append("table=").append(
        read.table == null || read.table.isBlank() ? String.valueOf(read.query) : read.table);
    sb.append(";columns=").append(read.columns);
    sb.append(";filter=").append(read.filter);
    sb.append(";split=").append(read.splits == null ? "" : read.splits.column);
    sb.append(";mode=").append(plan.resolvedMode());
    for (Shard shard : plan.shards()) {
      sb.append(';').append(shard.predicate(dialect, read.splits == null ? null : read.splits.column));
    }
    // String.hashCode is spec-stable across JVMs; the length guards against trivial collisions.
    int h = sb.toString().hashCode();
    return String.format("%08x-%d", h, sb.length());
  }

  /** Extract one shard of the plan into the sink. The caller owns batching, writing and state. */
  public static JdbcExtractor.ExtractStats runShard(
      JobConfig config,
      JdbcExtractor extractor,
      SplitPlanner.Plan plan,
      int shardIndex,
      Consumer<DataRow> sink)
      throws SQLException {
    if (shardIndex < 0 || shardIndex >= plan.shards().size()) {
      throw new IllegalArgumentException(
          "shard index " + shardIndex + " out of range 0.." + (plan.shards().size() - 1));
    }
    return extractor.extractShard(plan.shards().get(shardIndex), sink);
  }
}
