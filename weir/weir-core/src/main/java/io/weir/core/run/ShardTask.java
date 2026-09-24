package io.weir.core.run;

import io.weir.api.RunMetrics;
import io.weir.config.JobConfig;
import io.weir.core.extract.JdbcExtractor;
import io.weir.core.extract.SplitPlanner;
import io.weir.core.state.StateStores;
import io.weir.model.Watermark;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import io.weir.spi.ShardProgress;
import io.weir.spi.StateStore;

/**
 * Runs exactly one shard of the full-snapshot plan: extract → write → checkpoint.
 *
 * <p>This is the seam external engines plug into. A Flink or Spark job plans the shards once
 * (CLI {@code weir plan} prints them as JSON), then executes one {@link ShardTask} per parallel
 * subtask — in-process, since weir-core is an ordinary Java library with no engine dependencies.
 * Progress lands in the same state store the CLI uses, so a mix of engine runs and CLI runs stays
 * consistent, and {@code weir full} picks up wherever the engine left off.
 */
public final class ShardTask {
  private ShardTask() {}

  /** Outcome of one shard execution. */
  public record Result(int shardIndex, long rows, Watermark maxWatermark, boolean skipped, String shard) {

    public String toLine() {
      return "shard="
          + shardIndex
          + " rows="
          + rows
          + " wm="
          + maxWatermark
          + (skipped ? " skipped (already done)" : "")
          + " bounds="
          + shard;
    }
  }

  /**
   * @param expectedPlanId when non-null, fail fast if the freshly computed plan differs — protects
   *     against an engine executing a stale plan against a grown table
   */
  public static Result run(JobConfig config, int shardIndex, String expectedPlanId) throws Exception {
    RowWriterFactory factory = WeirRunner.resolveFactory(config);
    try (StateStore state = StateStores.create(config);
        JdbcExtractor extractor = new JdbcExtractor(config);
        RowWriter writer = factory.create(config)) {
      writer.open();

      SplitPlanner.Plan plan = extractor.planShards();
      String planId = FullShards.planId(plan, config, extractor.dialect());
      if (expectedPlanId != null && !expectedPlanId.isBlank() && !expectedPlanId.equals(planId)) {
        throw new IllegalStateException(
            "shard plan changed (expected "
                + expectedPlanId
                + ", got "
                + planId
                + ") — re-run 'weir plan' and reschedule the remaining shards");
      }

      ShardProgress progress =
          state
              .loadFullProgress(config.name)
              .filter(p -> p.planId().equals(planId))
              .orElseGet(() -> ShardProgress.empty(planId));
      String bounds = plan.shards().get(shardIndex).toString();
      if (progress.done().contains(shardIndex)) {
        return new Result(shardIndex, 0, Watermark.empty(), true, bounds);
      }

      WeirRunner.BatchSink sink =
          new WeirRunner.BatchSink(writer, Math.max(1, config.runtime.writeBatchSize), new RunMetrics());
      JdbcExtractor.ExtractStats stats = FullShards.runShard(config, extractor, plan, shardIndex, sink);
      sink.flush();
      state.saveFullProgress(config.name, progress.withShardDone(shardIndex, stats.maxWatermark));
      return new Result(shardIndex, stats.rows, stats.maxWatermark, false, bounds);
    }
  }
}
