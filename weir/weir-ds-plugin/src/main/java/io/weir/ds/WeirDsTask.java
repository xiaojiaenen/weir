package io.weir.ds;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.model.RunMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Thin DolphinScheduler task adapter. Mirrors the shape of DS task plugins (see
 * dolphinscheduler-task-datax / seatunnel) without hard-depending on DS SPI at compile time.
 *
 * <p>Integrate by adding a DS TaskChannel that constructs {@link WeirDsTask} from task params:
 *
 * <pre>
 *   configFile | configYaml
 *   mode = full | incremental | diff | check
 * </pre>
 *
 * Or invoke as main for Shell-style tasks: {@code java io.weir.ds.WeirDsTask --config-file a.yaml
 * --mode incremental}
 */
public final class WeirDsTask {

  public static final class Params {
    public String configFile;
    public String configYaml;
    public String mode = "incremental";
  }

  public int execute(Map<String, String> taskParams) throws Exception {
    return execute(WeirTaskContract.toParams(taskParams));
  }

  public int execute(Params params) throws Exception {
    JobConfig config;
    if (params.configYaml != null && !params.configYaml.isBlank()) {
      config = ConfigLoader.fromYaml(params.configYaml);
    } else if (params.configFile != null && !params.configFile.isBlank()) {
      config = ConfigLoader.load(Path.of(params.configFile));
    } else {
      System.err.println("weir-ds: require configFile or configYaml");
      return 2;
    }
    RunMode mode =
        switch (params.mode == null ? "incremental" : params.mode.toLowerCase()) {
          case "full" -> RunMode.FULL;
          case "diff" -> RunMode.DIFF;
          case "check" -> RunMode.CHECK;
          default -> RunMode.INCREMENTAL;
        };
    SyncResult result = new WeirRunner(config).run(mode);
    System.out.println(
        "weir-ds: success="
            + result.success()
            + " rowsRead="
            + result.rowsRead()
            + " rowsWritten="
            + result.rowsWritten()
            + " msg="
            + result.message());
    return result.success() ? 0 : 1;
  }

  /** Helper for DS UI forms / tests. */
  public static Params fromMap(Map<String, String> map) {
    Params p = new Params();
    p.configFile = map.get("configFile");
    p.configYaml = map.get("configYaml");
    p.mode = map.getOrDefault("mode", "incremental");
    return p;
  }

  public static void main(String[] args) throws Exception {
    Params params = new Params();
    for (int i = 0; i < args.length - 1; i++) {
      switch (args[i]) {
        case "--config-file" -> params.configFile = args[++i];
        case "--config-yaml" -> params.configYaml = args[++i];
        case "--mode" -> params.mode = args[++i];
        default -> {
          // skip
        }
      }
    }
    if (params.configFile == null && params.configYaml == null && args.length > 0) {
      // allow positional config path
      params.configFile = args[args.length - 1];
    }
    System.exit(new WeirDsTask().execute(params));
  }

  /** Write a temp yaml from string — used by DS tests and ad-hoc runs. */
  public static Path writeTempConfig(String yaml) throws Exception {
    Path f = Files.createTempFile("weir-ds-", ".yaml");
    Files.writeString(f, yaml, StandardCharsets.UTF_8);
    return f;
  }
}
