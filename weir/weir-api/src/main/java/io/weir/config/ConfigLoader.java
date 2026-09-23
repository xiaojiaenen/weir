package io.weir.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Loads {@link JobConfig} from YAML or JSON. */
public final class ConfigLoader {
  private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON = new ObjectMapper();

  private ConfigLoader() {}

  public static JobConfig load(Path path) throws IOException {
    boolean yaml =
        path.getFileName().toString().endsWith(".yaml")
            || path.getFileName().toString().endsWith(".yml");
    ObjectMapper mapper = yaml ? YAML : JSON;
    try (InputStream in = Files.newInputStream(path)) {
      JobConfig config = mapper.readValue(in, JobConfig.class);
      config.validate();
      return config;
    }
  }

  public static JobConfig fromYaml(String yaml) throws IOException {
    JobConfig config = YAML.readValue(yaml, JobConfig.class);
    config.validate();
    return config;
  }

  public static void save(JobConfig config, Path path) throws IOException {
    boolean yaml =
        path.getFileName().toString().endsWith(".yaml")
            || path.getFileName().toString().endsWith(".yml");
    ObjectMapper mapper = yaml ? YAML : JSON;
    mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), config);
  }
}
