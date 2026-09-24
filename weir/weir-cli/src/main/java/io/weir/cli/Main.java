package io.weir.cli;

import io.weir.api.RunReport;
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
import io.weir.model.Watermark;
import io.weir.spi.StateStore;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * CLI entry: {@code weir run|full|incremental|diff|check|validate|state|runs|reset -c job.yaml}
 */
public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length == 0 || "-h".equals(args[0]) || "--help".equals(args[0])) {
      usage();
      return;
    }
    String command = args[0];
    String configPath = null;
    int limit = 20;
    int shardIndex = -1;
    String planId = null;
    for (int i = 1; i < args.length; i++) {
      switch (args[i]) {
        case "-c", "--config" -> {
          if (i + 1 >= args.length) {
            System.err.println("missing value for " + args[i]);
            System.exit(2);
          }
          configPath = args[++i];
        }
        case "-n", "--limit" -> {
          if (i + 1 >= args.length) {
            System.err.println("missing value for " + args[i]);
            System.exit(2);
          }
          limit = Integer.parseInt(args[++i]);
        }
        case "-s", "--shard-index" -> {
          if (i + 1 >= args.length) {
            System.err.println("missing value for " + args[i]);
            System.exit(2);
          }
          shardIndex = Integer.parseInt(args[++i]);
        }
        case "--plan-id" -> {
          if (i + 1 >= args.length) {
            System.err.println("missing value for " + args[i]);
            System.exit(2);
          }
          planId = args[++i];
        }
        default -> {
          // ignore unknown flags
        }
      }
    }
    if (configPath == null) {
      System.err.println("missing -c <job.yaml>");
      usage();
      System.exit(2);
    }
    JobConfig config = ConfigLoader.load(Path.of(configPath));

    switch (command) {
      case "state" -> {
        try (StateStore state = StateStores.create(config)) {
          Map<String, Watermark> all = state.loadAllWatermarks();
          Watermark one = state.loadWatermark(config.name).orElse(Watermark.empty());
          System.out.println("job=" + config.name + " watermark=" + one);
          if (all.isEmpty()) {
            System.out.println("(no state rows)");
          } else {
            all.forEach((k, v) -> System.out.println(k + " -> " + v));
          }
        }
        return;
      }
      case "runs" -> {
        try (StateStore state = StateStores.create(config)) {
          List<RunReport> runs = state.loadRuns(config.name, limit);
          if (runs.isEmpty()) {
            System.out.println("(no run history)");
            return;
          }
          runs.forEach(r -> System.out.println(r.toLine()));
        }
        return;
      }
      case "reset" -> {
        try (StateStore state = StateStores.create(config)) {
          state.resetWatermark(config.name);
          state.clearFullProgress(config.name);
          System.out.println("watermark and full-sync progress reset for job=" + config.name);
        }
        return;
      }
      case "plan" -> {
        try (JdbcExtractor extractor = new JdbcExtractor(config)) {
          SplitPlanner.Plan plan = extractor.planShards();
          System.out.println(planJson(config, plan, FullShards.planId(plan, config, extractor.dialect())));
        } catch (SQLException e) {
          System.err.println("plan failed: " + e.getMessage());
          System.exit(1);
        }
        return;
      }
      case "exec-shard" -> {
        if (shardIndex < 0) {
          System.err.println("exec-shard requires --shard-index <n>");
          System.exit(2);
        }
        try {
          System.out.println(ShardTask.run(config, shardIndex, planId).toLine());
        } catch (Exception e) {
          System.err.println("exec-shard " + shardIndex + " failed: " + e.getMessage());
          System.exit(1);
        }
        return;
      }
      default -> {
        // fall through to run modes
      }
    }

    RunMode mode =
        switch (command) {
          case "run" -> config.mode;
          case "full" -> RunMode.FULL;
          case "incremental", "incr" -> RunMode.INCREMENTAL;
          case "diff" -> RunMode.DIFF;
          case "check", "validate" -> RunMode.CHECK;
          default -> {
            System.err.println("unknown command: " + command);
            usage();
            System.exit(2);
            yield RunMode.INCREMENTAL;
          }
        };
    if (mode == null) {
      mode = RunMode.INCREMENTAL;
    }

    WeirRunner runner = new WeirRunner(config);
    SyncResult result = runner.run(mode);
    System.out.println(result.toLine());
    if (mode == RunMode.CHECK && result.report() != null) {
      result.report().qualityFindings().forEach(f -> System.out.println("  - " + f));
    }
    if (!result.success()) {
      System.exit(1);
    }
  }

  /** Machine-readable shard contract consumed by external engines (Flink/Spark/Shell). */
  private static String planJson(JobConfig config, io.weir.core.extract.SplitPlanner.Plan plan, String planId) {
    JobConfig.SourceConfig.ReadConfig read = config.source.read;
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"job\": \"").append(esc(config.name)).append("\",\n");
    sb.append("  \"planId\": \"").append(esc(planId)).append("\",\n");
    sb.append("  \"resolvedSplit\": \"").append(plan.resolvedMode()).append("\",\n");
    sb.append("  \"splitColumn\": \"").append(esc(read.splits == null ? "" : read.splits.column)).append("\",\n");
    sb.append("  \"shards\": [\n");
    for (int i = 0; i < plan.shards().size(); i++) {
      io.weir.core.extract.Shard shard = plan.shards().get(i);
      String predicate = shard.predicate(
          io.weir.dialect.Dialects.forType(config.source.type),
          read.splits == null ? null : read.splits.column);
      sb.append("    {\"index\": ")
          .append(i)
          .append(", \"bounds\": \"")
          .append(esc(shard.toString()))
          .append("\", \"predicate\": \"")
          .append(esc(predicate))
          .append("\", \"fullScan\": ")
          .append(shard.isFullScan())
          .append("}")
          .append(i < plan.shards().size() - 1 ? "," : "")
          .append('\n');
    }
    sb.append("  ],\n");
    sb.append("  \"command\": \"weir exec-shard -c <job.yaml> --shard-index <index> --plan-id ")
        .append(esc(planId))
        .append("\"\n");
    sb.append("}");
    return sb.toString();
  }

  private static String esc(String s) {
    return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static void usage() {
    System.out.println(
        """
        Weir — JDBC full/incremental sync without CDC

        Usage:
          weir <command> -c <job.yaml> [-n <limit>]

        Commands:
          run           use job.mode from the config
          full          full snapshot (sharded, per-shard checkpoint, optionally parallel)
          incremental   poll by id / update_time (+id tie-break)
          diff          soft-delete / pk-diff / fingerprint correction pass
          check         validate config, schema and connectivity (alias: validate)
          state         show the stored watermark for this job
          runs          show recent run history (-n limits rows)
          reset         clear watermark and full-sync checkpoint
          plan          print the full-sync shard plan as JSON (external-engine contract)
          exec-shard    run exactly one shard: --shard-index N [--plan-id <id>]

        Exit codes:
          0 success   1 run failed or quality gate tripped   2 bad usage

        Example:
          weir full -c examples/mysql-to-file.yaml
          weir incremental -c examples/mysql-to-file.yaml
          weir check -c examples/mysql-to-file.yaml
          weir runs -c examples/mysql-to-file.yaml -n 10
          weir plan -c examples/mysql-to-file.yaml
          weir exec-shard -c examples/mysql-to-file.yaml --shard-index 1
        """);
  }
}
