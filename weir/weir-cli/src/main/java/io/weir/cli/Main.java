package io.weir.cli;

import io.weir.api.RunReport;
import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.core.state.StateStores;
import io.weir.model.RunMode;
import io.weir.model.Watermark;
import io.weir.spi.StateStore;
import java.nio.file.Path;
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
          System.out.println("watermark reset for job=" + config.name);
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

  private static void usage() {
    System.out.println(
        """
        Weir — JDBC full/incremental sync without CDC

        Usage:
          weir <command> -c <job.yaml> [-n <limit>]

        Commands:
          run           use job.mode from the config
          full          full snapshot (sharded, optionally parallel)
          incremental   poll by id / update_time (+id tie-break)
          diff          soft-delete / pk-diff / fingerprint correction pass
          check         validate config, schema and connectivity (alias: validate)
          state         show the stored watermark for this job
          runs          show recent run history (-n limits rows)
          reset         clear the watermark so the next run bootstraps again

        Exit codes:
          0 success   1 run failed or quality gate tripped   2 bad usage

        Example:
          weir full -c examples/mysql-to-file.yaml
          weir incremental -c examples/mysql-to-file.yaml
          weir check -c examples/mysql-to-file.yaml
          weir runs -c examples/mysql-to-file.yaml -n 10
        """);
  }
}
