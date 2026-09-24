package io.weir.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.weir.model.DeleteDetectMode;
import io.weir.model.IncrementalStrategy;
import io.weir.model.RunMode;
import io.weir.model.SplitMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Root job configuration loaded from YAML/JSON. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobConfig {
  public String name = "weir-job";
  public RunMode mode = RunMode.INCREMENTAL;
  public SourceConfig source = new SourceConfig();
  public TargetConfig target = new TargetConfig();
  public StateConfig state = new StateConfig();
  public RuntimeConfig runtime = new RuntimeConfig();
  public QualityConfig quality = new QualityConfig();

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceConfig {
    /** mysql | h2 | postgres | oracle | sqlserver */
    public String type = "mysql";
    public String url;
    public String user;
    public String password;
    /** Upper bound on pooled source connections used by parallel shards. */
    public int poolMax = 8;
    public ReadConfig read = new ReadConfig();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReadConfig {
      /** table | query */
      public String mode = "table";
      public String table;
      /** User SQL for query mode; must project contract columns. */
      public String query;
      public List<String> columns = new ArrayList<>();
      /** Optional constant predicate pushed down. */
      public String filter;
      public List<String> primaryKey = new ArrayList<>();
      public IncrementalConfig incremental = new IncrementalConfig();
      public SplitConfig splits = new SplitConfig();
      public DeleteConfig deleteDetect = new DeleteConfig();
      /** SQL-mode contract aliases. */
      public Map<String, String> contract = new LinkedHashMap<>();

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class IncrementalConfig {
        public IncrementalStrategy strategy = IncrementalStrategy.UPDATE_TIME_ID;
        public String column = "update_time";
        public String idColumn = "id";
        /** Overlap to avoid boundary loss on update_time. */
        public String overlap = "PT5M";
        public int batchRows = 50_000;
        public int fetchSize = 5_000;
        public int queryTimeoutSeconds = 0;
        /**
         * Upper bound for one incremental pass. 0 means "drain everything available". Prevents a
         * huge backlog from pinning the source for hours.
         */
        public long maxRowsPerRun = 0L;
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class SplitConfig {
        public String column = "id";
        public int numPartitions = 1;
        /** parallel | sequential */
        public String mode = "sequential";
        /** auto | range | mod | none — how shards are cut. */
        public SplitMode strategy = SplitMode.AUTO;
      }

      @JsonIgnoreProperties(ignoreUnknown = true)
      public static class DeleteConfig {
        public DeleteDetectMode mode = DeleteDetectMode.NONE;
        public String softColumn;
        /** Values of {@code softColumn} that mean "deleted". Defaults to truthy 1/true/Y. */
        public List<String> softTrueValues = new ArrayList<>();
        /** Partition/date column used by FINGERPRINT mode to narrow the drill-down. */
        public String fingerprintColumn;
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TargetConfig {
    /** file | jdbc | console | kafka | doris | starrocks | paimon */
    public String type = "file";
    /** file writer */
    public String path;
    /** json | csv | tsv */
    public String format = "json";
    /** jdbc writer */
    public String url;
    public String user;
    public String password;
    public String table;
    /** merge | append */
    public String writeMode = "merge";
    public List<String> primaryKey = new ArrayList<>();
    /** kafka */
    public String bootstrapServers;
    public String topic;
    public String acks = "all";
    /** doris / starrocks stream load */
    public String feHost;
    public int fePort = 8030;
    public String database;
    /** paimon */
    public String warehouse;
    public String catalogDb;
    /** paimon table name (alias if table not used) */
    public String paimonTable;
    /** extra writer options */
    public Map<String, String> options = new LinkedHashMap<>();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class StateConfig {
    /** jdbc | file */
    public String type = "file";
    public String path = ".weir/state";
    public String url;
    public String user;
    public String password;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RuntimeConfig {
    public int extractThreads = 4;
    public int writeBatchSize = 1_000;
    public int maxRetries = 3;
    public long retryBackoffMs = 500L;
    /** Cap on exponential backoff so a flapping source cannot stall a run forever. */
    public long retryBackoffMaxMs = 10_000L;
    /** Randomised jitter added to each backoff; avoids lockstep retries across shards. */
    public long retryJitterMs = 250L;
    public long maxRowsPerSec = 0L;
    public int maxConcurrentQueries = 4;
    /** Directory for JSON run reports; empty disables report files. */
    public String reportPath = "";
    /** Emit metric lines to the log at the end of each run. */
    public boolean metricsEnabled = true;
    /**
     * Checkpoint per-shard progress during FULL syncs. A crashed run resumes from the first
     * unfinished shard instead of re-scanning the whole table; progress is discarded when the shard
     * plan changes.
     */
    public boolean fullCheckpoint = true;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class QualityConfig {
    public boolean rowCountCheck = true;
    /** Fraction of rows (0.0–1.0) whose content hash is re-verified after write. */
    public double sampleHashCheck = 0.0;
    /** fail when watermark lag exceeds this; disabled when <=0 */
    public long watermarkMaxLagMs = 0L;
    /** git-style PK diff compares non-key columns and rewrites changed rows */
    public boolean compareRowContent = true;
    /** ALTER TABLE ADD COLUMN on jdbc targets when source has new columns */
    public boolean autoAddColumns = true;
    /** Run connectivity/schema/index preflight before every run (see §6.4 of the design). */
    public boolean preflightEnabled = false;
    /** Abort the run when the incremental column has no usable index. */
    public boolean failOnMissingIndex = false;
    /** Abort when the stored watermark is ahead of the source's current max. */
    public boolean failOnWatermarkRegression = true;
    /** Abort when row-count or sample-hash checks disagree. */
    public boolean failOnQualityMismatch = false;
  }

  // ------------------------------------------------------------------ helpers

  /** Primary keys for merge/diff: target config wins, source contract is the fallback. */
  public List<String> effectivePrimaryKeys() {
    if (target != null && target.primaryKey != null && !target.primaryKey.isEmpty()) {
      return target.primaryKey;
    }
    return source.read.primaryKey == null ? List.of() : source.read.primaryKey;
  }

  public void validate() {
    if (source == null || source.url == null || source.url.isBlank()) {
      throw new IllegalArgumentException("source.url is required");
    }
    if (source.read == null) {
      throw new IllegalArgumentException("source.read is required");
    }
    SourceConfig.ReadConfig read = source.read;
    if ("table".equalsIgnoreCase(read.mode)) {
      if (read.table == null || read.table.isBlank()) {
        throw new IllegalArgumentException("source.read.table is required for table mode");
      }
    } else if ("query".equalsIgnoreCase(read.mode)) {
      if (read.query == null || read.query.isBlank()) {
        throw new IllegalArgumentException("source.read.query is required for query mode");
      }
      if (read.primaryKey.isEmpty()) {
        throw new IllegalArgumentException(
            "query mode requires source.read.primaryKey (contract) for merge/incremental");
      }
    } else {
      throw new IllegalArgumentException("source.read.mode must be table or query");
    }
    if (target == null || target.type == null) {
      throw new IllegalArgumentException("target is required");
    }
    String tType = target.type.toLowerCase();
    switch (tType) {
      case "file" -> {
        if (target.path == null || target.path.isBlank()) {
          throw new IllegalArgumentException("target.path is required for file writer");
        }
      }
      case "console" -> {
        // no extra fields
      }
      case "kafka" -> {
        if (target.bootstrapServers == null || target.bootstrapServers.isBlank()) {
          throw new IllegalArgumentException("target.bootstrapServers is required for kafka");
        }
        if (target.topic == null || target.topic.isBlank()) {
          throw new IllegalArgumentException("target.topic is required for kafka");
        }
      }
      case "doris", "starrocks" -> {
        if (target.feHost == null || target.feHost.isBlank()) {
          throw new IllegalArgumentException("target.feHost is required for doris");
        }
        if (target.table == null || target.table.isBlank()) {
          throw new IllegalArgumentException("target.table is required for doris");
        }
      }
      case "paimon" -> {
        if (target.warehouse == null || target.warehouse.isBlank()) {
          throw new IllegalArgumentException("target.warehouse is required for paimon");
        }
        String pTable = target.paimonTable != null ? target.paimonTable : target.table;
        if (pTable == null || pTable.isBlank()) {
          throw new IllegalArgumentException(
              "target.table (or paimonTable) is required for paimon");
        }
      }
      case "jdbc" -> {
        if (target.url == null || target.url.isBlank()) {
          throw new IllegalArgumentException("target.url is required for jdbc writer");
        }
        if (target.table == null || target.table.isBlank()) {
          throw new IllegalArgumentException("target.table is required for jdbc writer");
        }
        if ("merge".equalsIgnoreCase(target.writeMode) && target.primaryKey.isEmpty()) {
          throw new IllegalArgumentException("merge writeMode requires target.primaryKey");
        }
      }
      default -> throw new IllegalArgumentException("unknown target.type: " + target.type);
    }

    // Semantics the design doc says must fail fast rather than silently lose data.
    if ("query".equalsIgnoreCase(read.mode)
        && read.deleteDetect != null
        && read.deleteDetect.mode != DeleteDetectMode.NONE
        && read.deleteDetect.mode != DeleteDetectMode.SOFT_COLUMN) {
      throw new IllegalArgumentException(
          "query mode supports deleteDetect none|soft_column only; PK diff needs a real table");
    }
    if (read.deleteDetect != null && read.deleteDetect.mode == DeleteDetectMode.SOFT_COLUMN) {
      if (read.deleteDetect.softColumn == null || read.deleteDetect.softColumn.isBlank()) {
        throw new IllegalArgumentException("deleteDetect.softColumn is required for soft_column");
      }
    }
    if (read.splits != null && read.splits.numPartitions < 1) {
      throw new IllegalArgumentException("splits.numPartitions must be >= 1");
    }
    if (runtime != null && runtime.writeBatchSize < 1) {
      throw new IllegalArgumentException("runtime.writeBatchSize must be >= 1");
    }
    if (quality != null && (quality.sampleHashCheck < 0.0 || quality.sampleHashCheck > 1.0)) {
      throw new IllegalArgumentException("quality.sampleHashCheck must be within 0.0..1.0");
    }
  }
}
