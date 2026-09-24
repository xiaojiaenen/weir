package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.extract.JdbcExtractor;
import io.weir.core.extract.SplitPlanner;
import io.weir.core.run.FullShards;
import io.weir.core.run.ShardTask;
import io.weir.core.run.WeirRunner;
import io.weir.core.state.StateStores;
import io.weir.model.RunMode;
import io.weir.spi.ShardProgress;
import io.weir.spi.StateStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Full-sync shard checkpointing: a crashed or partially-executed run must resume from the first
 * unfinished shard, and progress must be discarded whenever the shard plan changes.
 */
class FullCheckpointTest {

  @TempDir Path tmp;

  private JobConfig config() throws Exception {
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    String yaml =
        """
        name: checkpoint-job
        mode: full
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: orders
            columns: [id, amount]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            splits:
              column: id
              numPartitions: 3
        target:
          type: file
          path: %s
          format: jsonl
        state:
          type: file
          path: %s
        runtime:
          writeBatchSize: 2
        """
            .formatted(
                url,
                tmp.resolve("out/orders.jsonl").toAbsolutePath(),
                tmp.resolve("state").toAbsolutePath());
    return ConfigLoader.fromYaml(yaml);
  }

  private void seedOrders() throws Exception {
    String url = "jdbc:h2:" + tmp.resolve("src").toAbsolutePath() + ";MODE=MySQL";
    try (Connection conn = DriverManager.getConnection(url, "sa", "");
        Statement st = conn.createStatement()) {
      st.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount DECIMAL(10,2))");
      for (int i = 1; i <= 6; i++) {
        st.execute("INSERT INTO orders VALUES (" + i + ", " + i + ".5)");
      }
    }
  }

  private String planIdOf(JobConfig config) throws Exception {
    try (JdbcExtractor extractor = new JdbcExtractor(config)) {
      SplitPlanner.Plan plan = extractor.planShards();
      assertEquals(3, plan.shards().size());
      return FullShards.planId(plan, config, extractor.dialect());
    }
  }

  @Test
  void resumedFullRunSkipsShardsDoneByShardTask() throws Exception {
    seedOrders();
    JobConfig config = config();

    ShardTask.Result first = ShardTask.run(config, 0, null);
    assertFalse(first.skipped());
    assertEquals(2, first.rows());
    ShardTask.Result second = ShardTask.run(config, 1, null);
    assertEquals(2, second.rows());

    try (StateStore state = StateStores.create(config)) {
      ShardProgress progress = state.loadFullProgress(config.name).orElseThrow();
      assertEquals(2, progress.done().size());
      assertEquals(4L, progress.maxWatermark().id(), "wm = max over completed shards");
    }

    SyncResult full = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(full.success(), full.message());
    assertEquals(2, full.rowsRead(), "only the third shard should be re-read");
    assertEquals(6L, full.endWatermark().id(), "watermark merges persisted and fresh shards");

    String content = Files.readString(tmp.resolve("out/orders.jsonl"));
    for (int i = 1; i <= 6; i++) {
      assertTrue(content.contains("\"id\":" + i), "missing id " + i + " in\n" + content);
    }
    try (StateStore state = StateStores.create(config)) {
      assertFalse(state.loadFullProgress(config.name).isPresent(), "progress must be cleared");
    }
  }

  @Test
  void planChangeDiscardsStaleProgress() throws Exception {
    seedOrders();
    JobConfig config = config();
    try (StateStore state = StateStores.create(config)) {
      state.saveFullProgress(
          config.name, new ShardProgress("bogus-plan", java.util.Set.of(0, 1, 2), io.weir.model.Watermark.ofId(99L)));
    }
    SyncResult full = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(full.success(), full.message());
    assertEquals(6, full.rowsRead(), "stale plan must not skip any shard");
    assertEquals(6L, full.endWatermark().id());
  }

  @Test
  void fullRunCompletesAndClearsProgress() throws Exception {
    seedOrders();
    JobConfig config = config();
    SyncResult full = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(full.success(), full.message());
    assertEquals(6, full.rowsRead());
    try (StateStore state = StateStores.create(config)) {
      assertFalse(state.loadFullProgress(config.name).isPresent());
    }
    // A second full run starts from scratch (merge makes it idempotent).
    SyncResult again = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(again.success(), again.message());
    assertEquals(6, again.rowsRead());
  }

  @Test
  void shardTaskSkipsAlreadyDoneShardAndRejectsStalePlanId() throws Exception {
    seedOrders();
    JobConfig config = config();
    String planId = planIdOf(config);

    ShardTask.Result first = ShardTask.run(config, 2, planId);
    assertEquals(2, first.rows());
    ShardTask.Result again = ShardTask.run(config, 2, planId);
    assertTrue(again.skipped());
    assertEquals(0, again.rows());

    assertThrows(IllegalStateException.class, () -> ShardTask.run(config, 1, "stale-plan-id"));
  }

  @Test
  void checkpointDisabledRunsEverythingEveryTime() throws Exception {
    seedOrders();
    JobConfig config = config();
    config.runtime.fullCheckpoint = false;
    SyncResult full = new WeirRunner(config).run(RunMode.FULL);
    assertTrue(full.success(), full.message());
    assertEquals(6, full.rowsRead());
    try (StateStore state = StateStores.create(config)) {
      assertFalse(state.loadFullProgress(config.name).isPresent());
    }
  }
}
