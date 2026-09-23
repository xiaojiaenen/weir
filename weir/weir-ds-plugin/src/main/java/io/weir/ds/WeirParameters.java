package io.weir.ds;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

/** Task parameters for WEIR nodes. */
public class WeirParameters extends AbstractParameters {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public String configFile;
  public String configYaml;
  public String mode = "incremental";

  @Override
  public boolean checkParameters() {
    return (configFile != null && !configFile.isBlank())
        || (configYaml != null && !configYaml.isBlank());
  }

  public static WeirParameters parse(String taskParams) {
    WeirParameters p = new WeirParameters();
    if (taskParams == null || taskParams.isBlank()) {
      return p;
    }
    try {
      var node = MAPPER.readTree(taskParams);
      if (node.hasNonNull("configFile")) {
        p.configFile = node.get("configFile").asText();
      }
      if (node.hasNonNull("configYaml")) {
        p.configYaml = node.get("configYaml").asText();
      }
      if (node.hasNonNull("mode")) {
        p.mode = node.get("mode").asText();
      }
    } catch (Exception e) {
      // keep defaults; checkParameters will fail if empty
    }
    return p;
  }

  public WeirDsTask.Params toWeirParams() {
    WeirDsTask.Params params = new WeirDsTask.Params();
    params.configFile = configFile;
    params.configYaml = configYaml;
    params.mode = Objects.requireNonNullElse(mode, "incremental");
    return params;
  }
}
