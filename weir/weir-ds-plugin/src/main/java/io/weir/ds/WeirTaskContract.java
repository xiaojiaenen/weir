package io.weir.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DolphinScheduler TaskChannel surface (version-portable).
 *
 * <p>When building a drop-in plugin for your DS version, implement the official SPI:
 *
 * <pre>
 *   org.apache.dolphinscheduler.spi.task.TaskChannel
 *   org.apache.dolphinscheduler.spi.task.TaskChannelFactory
 *   org.apache.dolphinscheduler.spi.task.request.TaskRequest
 * </pre>
 *
 * (package names differ slightly across DS 3.1 / 3.2 / 3.3 — see docs/ds-taskchannel.md)
 *
 * <p>This class keeps the contract in one place so adapters stay thin.
 */
public final class WeirTaskContract {
  public static final String TASK_TYPE = "WEIR";

  public static final String PARAM_CONFIG_FILE = "configFile";
  public static final String PARAM_CONFIG_YAML = "configYaml";
  public static final String PARAM_MODE = "mode";

  private WeirTaskContract() {}

  /** Form options shown in the DS UI task definition. */
  public static List<String> optionLabels() {
    return List.of(PARAM_CONFIG_FILE, PARAM_CONFIG_YAML, PARAM_MODE);
  }

  /** Map DS task params (string map) → {@link WeirDsTask.Params}. */
  public static WeirDsTask.Params toParams(Map<String, String> taskParams) {
    WeirDsTask.Params p = new WeirDsTask.Params();
    if (taskParams == null) {
      return p;
    }
    p.configFile = taskParams.get(PARAM_CONFIG_FILE);
    p.configYaml = taskParams.get(PARAM_CONFIG_YAML);
    p.mode = taskParams.getOrDefault(PARAM_MODE, "incremental");
    return p;
  }

  /** Sample params for docs / tests. */
  public static Map<String, String> sampleParams() {
    Map<String, String> m = new HashMap<>();
    m.put(PARAM_CONFIG_FILE, "/opt/weir/conf/orders.yaml");
    m.put(PARAM_MODE, "incremental");
    return m;
  }

  public static List<String> requiredKeys() {
    List<String> keys = new ArrayList<>();
    keys.add(PARAM_MODE);
    return keys;
  }
}
