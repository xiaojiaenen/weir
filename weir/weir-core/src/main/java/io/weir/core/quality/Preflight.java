package io.weir.core.quality;

import io.weir.api.WeirException;
import io.weir.config.JobConfig;
import io.weir.core.diff.TargetReader;
import io.weir.core.util.Durations;
import io.weir.dialect.JdbcDialect;
import io.weir.model.IncrementalStrategy;
import io.weir.model.Watermark;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Pre-flight checks run before a sync touches a source (design §6.4).
 *
 * <p>The point is to fail loudly during configuration rather than silently: an incremental column
 * that does not exist, a watermark that has gone backwards, or an unindexed cursor column will each
 * produce wrong data or a scanned-to-death OLTP box if discovered mid-run.
 */
public final class Preflight {

  /** Severity of a single finding. */
  public enum Level {
    INFO,
    WARN,
    ERROR
  }

  public record Finding(Level level, String code, String message) {
    @Override
    public String toString() {
      return level + "[" + code + "] " + message;
    }
  }

  public record Report(List<Finding> findings) {
    public boolean hasErrors() {
      return findings.stream().anyMatch(f -> f.level() == Level.ERROR);
    }

    public boolean hasWarnings() {
      return findings.stream().anyMatch(f -> f.level() == Level.WARN);
    }

    public List<String> messages() {
      return findings.stream().map(Finding::toString).toList();
    }
  }

  private Preflight() {}

  /**
   * Run all checks against the live source.
   *
   * @param startWatermark the cursor this run would start from, for regression detection
   */
  public static Report check(
      Connection conn, JdbcDialect dialect, JobConfig config, Watermark startWatermark) {
    List<Finding> findings = new ArrayList<>();
    JobConfig.SourceConfig.ReadConfig read = config.source.read;

    Set<String> columns;
    try {
      columns = sourceColumns(conn, dialect, config);
    } catch (SQLException e) {
      findings.add(
          new Finding(Level.ERROR, "SOURCE_UNREADABLE", "cannot read source metadata: " + e.getMessage()));
      return new Report(List.copyOf(findings));
    }
    if (columns.isEmpty()) {
      findings.add(
          new Finding(
              Level.WARN,
              "NO_METADATA",
              "source metadata unavailable (query mode?) — column checks skipped"));
    }

    // 1. Incremental columns must exist.
    IncrementalStrategy strategy = read.incremental.strategy;
    if (strategy == IncrementalStrategy.ID || strategy == IncrementalStrategy.UPDATE_TIME_ID) {
      checkColumn(findings, columns, read.incremental.idColumn, "incremental.idColumn");
    }
    if (strategy == IncrementalStrategy.UPDATE_TIME
        || strategy == IncrementalStrategy.UPDATE_TIME_ID) {
      checkColumn(findings, columns, read.incremental.column, "incremental.column");
    }
    if (read.splits != null && read.splits.numPartitions > 1) {
      checkColumn(findings, columns, read.splits.column, "splits.column");
    }
    if (read.deleteDetect != null && read.deleteDetect.mode != null) {
      if (read.deleteDetect.mode == io.weir.model.DeleteDetectMode.SOFT_COLUMN) {
        checkColumn(findings, columns, read.deleteDetect.softColumn, "deleteDetect.softColumn");
      }
      if (read.deleteDetect.mode == io.weir.model.DeleteDetectMode.FINGERPRINT) {
        checkColumn(
            findings, columns, read.deleteDetect.fingerprintColumn, "deleteDetect.fingerprintColumn");
      }
    }
    if (!read.columns.isEmpty()) {
      for (String c : read.columns) {
        checkColumn(findings, columns, c, "read.columns");
      }
    }

    // 2. Primary keys must exist when we plan to merge or diff.
    List<String> pk = config.effectivePrimaryKeys();
    if (pk.isEmpty() && "merge".equalsIgnoreCase(config.target.writeMode)) {
      findings.add(
          new Finding(
              Level.ERROR,
              "PK_MISSING",
              "writeMode=merge requires target.primaryKey (or source.read.primaryKey)"));
    }
    for (String c : pk) {
      checkColumn(findings, columns, c, "primaryKey");
    }

    // 3. Watermark regression — the cursor must not be ahead of the source.
    checkWatermarkRegression(conn, dialect, config, startWatermark, findings);

    // 4. Cursor column should be indexed; running unindexed means a full scan per poll.
    if (config.quality.failOnMissingIndex || config.quality.preflightEnabled) {
      String cursorCol =
          (strategy == IncrementalStrategy.ID)
              ? read.incremental.idColumn
              : read.incremental.column;
      if (cursorCol != null && !cursorCol.isBlank()) {
        boolean indexed = hasIndex(conn, config, cursorCol);
        if (!indexed) {
          Finding f =
              new Finding(
                  Level.WARN,
                  "NO_INDEX",
                  "cursor column "
                      + cursorCol
                      + " appears unindexed; each poll may full-scan the source");
          findings.add(config.quality.failOnMissingIndex ? escalate(f) : f);
        }
      }
    }

    // 5. Target writability.
    if ("jdbc".equalsIgnoreCase(config.target.type) && !TargetReader.reachable(config)) {
      findings.add(
          new Finding(
              Level.ERROR,
              "TARGET_UNREACHABLE",
              "target table " + config.target.table + " is not readable/writable"));
    }

    // 6. Overlap sanity — a zero overlap with an update_time cursor risks boundary loss.
    if ((strategy == IncrementalStrategy.UPDATE_TIME
            || strategy == IncrementalStrategy.UPDATE_TIME_ID)
        && Durations.parse(read.incremental.overlap).isZero()) {
      findings.add(
          new Finding(
              Level.WARN,
              "NO_OVERLAP",
              "overlap=0 with an update_time cursor: rows written at the exact watermark "
                  + "boundary may be missed"));
    }

    return new Report(List.copyOf(findings));
  }

  /** Promote a warning to an error when the operator asked for strict mode. */
  private static Finding escalate(Finding f) {
    return new Finding(Level.ERROR, f.code(), f.message());
  }

  private static void checkColumn(
      List<Finding> findings, Set<String> columns, String column, String role) {
    if (column == null || column.isBlank()) {
      findings.add(new Finding(Level.ERROR, "COLUMN_UNSET", role + " is not configured"));
      return;
    }
    if (columns.isEmpty()) {
      return;
    }
    if (!containsIgnoreCase(columns, column)) {
      findings.add(
          new Finding(
              Level.ERROR,
              "COLUMN_MISSING",
              role + " '" + column + "' does not exist on the source"));
    }
  }

  private static boolean containsIgnoreCase(Set<String> set, String value) {
    for (String s : set) {
      if (s.equalsIgnoreCase(value)) {
        return true;
      }
    }
    return false;
  }

  private static void checkWatermarkRegression(
      Connection conn,
      JdbcDialect dialect,
      JobConfig config,
      Watermark wm,
      List<Finding> findings) {
    if (wm == null || wm.ts() == null) {
      return;
    }
    JobConfig.SourceConfig.ReadConfig.IncrementalConfig inc = config.source.read.incremental;
    String col = inc.column;
    if (col == null || col.isBlank()) {
      return;
    }
    String sql =
        dialect.buildSelect(
            List.of("MAX(" + dialect.quote(col) + ")"),
            io.weir.core.util.Sql.fromClause(config.source.read),
            config.source.read.filter,
            null,
            null);
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setQueryTimeout(30);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return;
        }
        Object v = rs.getObject(1);
        if (v == null) {
          return;
        }
        Instant sourceMax = toInstant(v);
        if (sourceMax == null) {
          return;
        }
        if (sourceMax.isBefore(wm.ts().minus(Duration.ofMinutes(1)))) {
          findings.add(
              new Finding(
                  config.quality.failOnWatermarkRegression ? Level.ERROR : Level.WARN,
                  "WATERMARK_REGRESSION",
                  "stored watermark "
                      + wm.ts()
                      + " is ahead of source max "
                      + sourceMax
                      + "; the source may have been reset or restored"));
        }
      }
    } catch (SQLException e) {
      findings.add(
          new Finding(Level.WARN, "WATERMARK_CHECK_FAILED", "could not verify watermark: " + e.getMessage()));
    }
  }

  private static Instant toInstant(Object v) {
    if (v instanceof Instant i) {
      return i;
    }
    if (v instanceof java.sql.Timestamp t) {
      return t.toInstant();
    }
    if (v instanceof java.util.Date d) {
      return d.toInstant();
    }
    return null;
  }

  private static boolean hasIndex(Connection conn, JobConfig config, String column) {
    try {
      DatabaseMetaData md = conn.getMetaData();
      String table = config.source.read.table;
      if (table == null || table.isBlank()) {
        return true; // query mode: cannot reason about a subquery's indexes
      }
      String simple = table;
      int dot = table.lastIndexOf('.');
      if (dot > 0) {
        simple = table.substring(dot + 1);
      }
      for (String pattern :
          new String[] {
            simple, simple.toUpperCase(Locale.ROOT), simple.toLowerCase(Locale.ROOT)
          }) {
        try (ResultSet rs = md.getIndexInfo(null, null, pattern, false, false)) {
          while (rs.next()) {
            String col = rs.getString("COLUMN_NAME");
            if (col != null && col.equalsIgnoreCase(column)) {
              return true;
            }
          }
        }
      }
    } catch (SQLException ignored) {
      // Metadata unavailable: treat as indexed rather than crying wolf.
      return true;
    }
    return false;
  }

  /**
   * Column labels of the source projection. Falls back to {@code SELECT *} when the configured
   * projection is itself invalid — otherwise a typo'd column would surface as "unreadable source"
   * instead of the precise "column missing" finding.
   */
  public static Set<String> sourceColumns(Connection conn, JdbcDialect dialect, JobConfig config)
      throws SQLException {
    Set<String> out = new LinkedHashSet<>();
    String where = io.weir.core.util.Sql.and(List.of());
    List<String> projection =
        config.source.read.columns.isEmpty()
            ? List.of()
            : config.source.read.columns.stream().map(dialect::quote).toList();
    String sql =
        dialect.buildSelect(
            projection,
            io.weir.core.util.Sql.fromClause(config.source.read),
            where,
            null,
            1);
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setQueryTimeout(30);
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          String label = md.getColumnLabel(i);
          if (label != null && !label.isBlank()) {
            out.add(label);
          }
        }
      }
    } catch (SQLException e) {
      if (projection.isEmpty()) {
        throw e;
      }
      // Retry with the full projection to distinguish "bad column name" from "cannot connect".
      String all = dialect.buildSelect(
          List.of(), io.weir.core.util.Sql.fromClause(config.source.read), where, null, 1);
      try (PreparedStatement ps = conn.prepareStatement(all)) {
        ps.setQueryTimeout(30);
        try (ResultSet rs = ps.executeQuery()) {
          ResultSetMetaData md = rs.getMetaData();
          for (int i = 1; i <= md.getColumnCount(); i++) {
            String label = md.getColumnLabel(i);
            if (label != null && !label.isBlank()) {
              out.add(label);
            }
          }
        }
      }
    }
    return out;
  }

  /** Throw when the report contains errors — used by CHECK and strict runs. */
  public static void failOnErrors(Report report) {
    if (report.hasErrors()) {
      throw new WeirException(
          "preflight failed: "
              + report.findings().stream()
                  .filter(f -> f.level() == Level.ERROR)
                  .map(Finding::message)
                  .reduce((a, b) -> a + "; " + b)
                  .orElse("unknown"));
    }
  }

  /** Unused type constant kept so callers can reason about comparable cursor types. */
  public static boolean isComparableCursorType(int sqlType) {
    return switch (sqlType) {
      case Types.TIMESTAMP,
          Types.TIMESTAMP_WITH_TIMEZONE,
          Types.DATE,
          Types.TIME,
          Types.TIME_WITH_TIMEZONE,
          Types.BIGINT,
          Types.INTEGER,
          Types.SMALLINT,
          Types.TINYINT,
          Types.NUMERIC,
          Types.DECIMAL ->
          true;
      default -> false;
    };
  }
}
