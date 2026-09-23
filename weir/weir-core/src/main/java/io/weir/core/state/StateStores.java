package io.weir.core.state;

import io.weir.api.RunReport;
import io.weir.api.RunReportCodec;
import io.weir.config.JobConfig;
import io.weir.core.util.Jdbc;
import io.weir.model.RunMode;
import io.weir.model.Watermark;
import io.weir.spi.StateStore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** File-backed state (properties + JSONL run log) and JDBC-backed state. */
public final class StateStores {
  private static final Logger log = LoggerFactory.getLogger(StateStores.class);

  private StateStores() {}

  public static StateStore create(JobConfig config) {
    if ("jdbc".equalsIgnoreCase(config.state.type)) {
      return new JdbcStateStore(config.state);
    }
    return new FileStateStore(
        Path.of(config.state.path == null ? ".weir/state" : config.state.path));
  }

  // ------------------------------------------------------------------ file

  public static final class FileStateStore implements StateStore {
    private final Path dir;
    private final ConcurrentHashMap<String, Watermark> cache = new ConcurrentHashMap<>();

    public FileStateStore(Path dir) {
      this.dir = dir;
      try {
        Files.createDirectories(dir);
      } catch (IOException e) {
        throw new IllegalStateException("Cannot create state dir " + dir, e);
      }
      loadAll();
    }

    private void loadAll() {
      Path wm = dir.resolve("watermark.properties");
      if (!Files.exists(wm)) {
        return;
      }
      Properties props = new Properties();
      try (var in = Files.newInputStream(wm)) {
        props.load(in);
        for (String name : props.stringPropertyNames()) {
          cache.put(name, parse(props.getProperty(name)));
        }
      } catch (IOException e) {
        throw new IllegalStateException("Cannot read watermark file", e);
      }
    }

    private static String encode(Watermark w) {
      return (w.ts() == null ? "" : w.ts().toString()) + "|" + (w.id() == null ? "" : w.id());
    }

    private static Watermark parse(String raw) {
      if (raw == null || raw.isBlank()) {
        return Watermark.empty();
      }
      String[] parts = raw.split("\\|", -1);
      Instant ts = parts[0].isEmpty() ? null : Instant.parse(parts[0]);
      Long id = parts.length < 2 || parts[1].isEmpty() ? null : Long.parseLong(parts[1]);
      return Watermark.ofTsId(ts, id);
    }

    @Override
    public Optional<Watermark> loadWatermark(String jobId) {
      return Optional.ofNullable(cache.get(jobId));
    }

    @Override
    public Map<String, Watermark> loadAllWatermarks() {
      return Map.copyOf(cache);
    }

    @Override
    public void saveWatermark(String jobId, Watermark watermark) {
      cache.put(jobId, watermark == null ? Watermark.empty() : watermark);
      Properties props = new Properties();
      cache.forEach((k, v) -> props.setProperty(k, encode(v)));
      Path wm = dir.resolve("watermark.properties");
      try (var out = Files.newOutputStream(wm)) {
        props.store(out, "weir watermarks");
      } catch (IOException e) {
        throw new IllegalStateException("Cannot persist watermark", e);
      }
    }

    @Override
    public void saveRun(
        String jobId, String runId, String status, long rowsRead, long rowsWritten, String message) {
      String line =
          Instant.now()
              + "\t"
              + jobId
              + "\t"
              + runId
              + "\t"
              + status
              + "\t"
              + rowsRead
              + "\t"
              + rowsWritten
              + "\t"
              + (message == null ? "" : message.replace('\n', ' '));
      try {
        Files.writeString(
            dir.resolve("runs.log"),
            line + System.lineSeparator(),
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
      } catch (IOException e) {
        throw new IllegalStateException("Cannot append run log", e);
      }
    }

    @Override
    public void saveReport(RunReport report) {
      saveRun(
          report.jobId(),
          report.runId(),
          report.status(),
          report.rowsRead(),
          report.rowsWritten(),
          report.message());
      Path json = dir.resolve("runs.jsonl");
      try {
        String line = RunReportCodec.toJson(report);
        Files.writeString(
            json,
            line + System.lineSeparator(),
            StandardCharsets.UTF_8,
            java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
      } catch (IOException e) {
        // The tab-separated log above is the durable fallback; JSON is for `weir runs`.
        log.warn("Cannot append JSON run report: {}", e.getMessage());
      }
    }

    @Override
    public List<RunReport> loadRuns(String jobId, int limit) {
      Path json = dir.resolve("runs.jsonl");
      if (!Files.exists(json)) {
        return List.of();
      }
      List<RunReport> out = new ArrayList<>();
      try (var lines = Files.lines(json, StandardCharsets.UTF_8)) {
        lines.forEach(
            line -> {
              if (line.isBlank()) {
                return;
              }
              try {
                RunReport r = RunReportCodec.fromJson(line);
                if (r != null && (jobId == null || jobId.equals(r.jobId()))) {
                  out.add(r);
                }
              } catch (RuntimeException ignored) {
                // Skip malformed or truncated tail lines from a crashed run.
              }
            });
      } catch (IOException e) {
        log.warn("Cannot read run reports: {}", e.getMessage());
      }
      // Newest first.
      out.sort(
          (a, b) -> {
            Instant x = a.startedAt();
            Instant y = b.startedAt();
            if (x == null && y == null) {
              return 0;
            }
            if (x == null) {
              return 1;
            }
            if (y == null) {
              return -1;
            }
            return y.compareTo(x);
          });
      int n = limit <= 0 ? out.size() : Math.min(limit, out.size());
      return List.copyOf(out.subList(0, n));
    }

    @Override
    public void saveSnapshotMeta(String jobId, String key, String value) {
      Path file = dir.resolve("snapshot.properties");
      Properties props = new Properties();
      if (Files.exists(file)) {
        try (var in = Files.newInputStream(file)) {
          props.load(in);
        } catch (IOException ignored) {
          // start from empty rather than losing the write
        }
      }
      props.setProperty(jobId + "|" + key, value == null ? "" : value);
      try (var out = Files.newOutputStream(file)) {
        props.store(out, "weir snapshot metadata");
      } catch (IOException e) {
        throw new IllegalStateException("Cannot persist snapshot metadata", e);
      }
    }

    @Override
    public String loadSnapshotMeta(String jobId, String key) {
      Path file = dir.resolve("snapshot.properties");
      if (!Files.exists(file)) {
        return null;
      }
      Properties props = new Properties();
      try (var in = Files.newInputStream(file)) {
        props.load(in);
      } catch (IOException e) {
        return null;
      }
      String v = props.getProperty(jobId + "|" + key);
      return v == null || v.isEmpty() ? null : v;
    }
  }

  // ------------------------------------------------------------------ jdbc

  public static final class JdbcStateStore implements StateStore {
    private final Connection connection;

    public JdbcStateStore(JobConfig.StateConfig cfg) {
      try {
        this.connection = Jdbc.open(cfg.url, cfg.user, cfg.password);
        init();
      } catch (SQLException e) {
        throw new IllegalStateException("Cannot open state DB", e);
      }
    }

    private void init() throws SQLException {
      try (Statement st = connection.createStatement()) {
        st.execute(
            "CREATE TABLE IF NOT EXISTS weir_state ("
                + "job_id VARCHAR(128) PRIMARY KEY, "
                + "wm_ts TIMESTAMP, "
                + "wm_id BIGINT)");
        st.execute(
            "CREATE TABLE IF NOT EXISTS weir_run ("
                + "run_id VARCHAR(64) PRIMARY KEY, "
                + "job_id VARCHAR(128), "
                + "mode VARCHAR(16), "
                + "status VARCHAR(32), "
                + "started_at TIMESTAMP, "
                + "finished_at TIMESTAMP, "
                + "duration_ms BIGINT, "
                + "rows_read BIGINT, "
                + "rows_written BIGINT, "
                + "wm_ts TIMESTAMP, "
                + "wm_id BIGINT, "
                + "message VARCHAR(2000))");
        st.execute(
            "CREATE TABLE IF NOT EXISTS weir_snapshot ("
                + "job_id VARCHAR(128), "
                + "meta_key VARCHAR(128), "
                + "meta_value VARCHAR(2000), "
                + "PRIMARY KEY (job_id, meta_key))");
      }
      connection.commit();
    }

    @Override
    public Optional<Watermark> loadWatermark(String jobId) {
      String sql = "SELECT wm_ts, wm_id FROM weir_state WHERE job_id = ?";
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, jobId);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return Optional.empty();
          }
          Timestamp ts = rs.getTimestamp(1);
          long id = rs.getLong(2);
          boolean idNull = rs.wasNull();
          return Optional.of(Watermark.ofTsId(ts == null ? null : ts.toInstant(), idNull ? null : id));
        }
      } catch (SQLException e) {
        throw new IllegalStateException("loadWatermark failed", e);
      }
    }

    @Override
    public Map<String, Watermark> loadAllWatermarks() {
      String sql = "SELECT job_id, wm_ts, wm_id FROM weir_state";
      Map<String, Watermark> out = new java.util.LinkedHashMap<>();
      try (PreparedStatement ps = connection.prepareStatement(sql);
          ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Timestamp ts = rs.getTimestamp(2);
          long id = rs.getLong(3);
          boolean idNull = rs.wasNull();
          out.put(
              rs.getString(1), Watermark.ofTsId(ts == null ? null : ts.toInstant(), idNull ? null : id));
        }
      } catch (SQLException e) {
        throw new IllegalStateException("loadAllWatermarks failed", e);
      }
      return out;
    }

    @Override
    public void saveWatermark(String jobId, Watermark watermark) {
      // Upsert via delete+insert: portable across H2/MySQL/PG without MERGE syntax differences.
      try (PreparedStatement del = connection.prepareStatement("DELETE FROM weir_state WHERE job_id = ?")) {
        del.setString(1, jobId);
        del.executeUpdate();
      } catch (SQLException e) {
        throw new IllegalStateException("saveWatermark (clear) failed", e);
      }
      try (PreparedStatement ps =
          connection.prepareStatement("INSERT INTO weir_state (job_id, wm_ts, wm_id) VALUES (?, ?, ?)")) {
        ps.setString(1, jobId);
        ps.setTimestamp(2, watermark == null || watermark.ts() == null ? null : Timestamp.from(watermark.ts()));
        if (watermark == null || watermark.id() == null) {
          ps.setNull(3, java.sql.Types.BIGINT);
        } else {
          ps.setLong(3, watermark.id());
        }
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw new IllegalStateException("saveWatermark failed", e);
      }
    }

    @Override
    public void saveRun(
        String jobId, String runId, String status, long rowsRead, long rowsWritten, String message) {
      try (PreparedStatement del = connection.prepareStatement("DELETE FROM weir_run WHERE run_id = ?")) {
        del.setString(1, runId);
        del.executeUpdate();
      } catch (SQLException e) {
        throw new IllegalStateException("saveRun (clear) failed", e);
      }
      try (PreparedStatement ps =
          connection.prepareStatement(
              "INSERT INTO weir_run (run_id, job_id, status, rows_read, rows_written, message, finished_at) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?)")) {
        ps.setString(1, runId);
        ps.setString(2, jobId);
        ps.setString(3, status);
        ps.setLong(4, rowsRead);
        ps.setLong(5, rowsWritten);
        ps.setString(6, message == null ? "" : message.substring(0, Math.min(message.length(), 1900)));
        ps.setTimestamp(7, Timestamp.from(Instant.now()));
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw new IllegalStateException("saveRun failed", e);
      }
    }

    @Override
    public void saveReport(RunReport report) {
      try (PreparedStatement del = connection.prepareStatement("DELETE FROM weir_run WHERE run_id = ?")) {
        del.setString(1, report.runId());
        del.executeUpdate();
      } catch (SQLException e) {
        throw new IllegalStateException("saveReport (clear) failed", e);
      }
      try (PreparedStatement ps =
          connection.prepareStatement(
              "INSERT INTO weir_run (run_id, job_id, mode, status, started_at, finished_at, "
                  + "duration_ms, rows_read, rows_written, wm_ts, wm_id, message) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
        ps.setString(1, report.runId());
        ps.setString(2, report.jobId());
        ps.setString(3, report.mode() == null ? null : report.mode().name());
        ps.setString(4, report.status());
        ps.setTimestamp(5, report.startedAt() == null ? null : Timestamp.from(report.startedAt()));
        ps.setTimestamp(6, Timestamp.from(Instant.now()));
        ps.setLong(7, report.durationMs());
        ps.setLong(8, report.rowsRead());
        ps.setLong(9, report.rowsWritten());
        ps.setTimestamp(
            10,
            report.endWatermark() == null || report.endWatermark().ts() == null
                ? null
                : Timestamp.from(report.endWatermark().ts()));
        if (report.endWatermark() == null || report.endWatermark().id() == null) {
          ps.setNull(11, java.sql.Types.BIGINT);
        } else {
          ps.setLong(11, report.endWatermark().id());
        }
        ps.setString(12, report.message() == null ? "" : report.message());
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw new IllegalStateException("saveReport failed", e);
      }
    }

    @Override
    public List<RunReport> loadRuns(String jobId, int limit) {
      String sql =
          "SELECT run_id, job_id, mode, status, started_at, duration_ms, rows_read, rows_written, "
              + "wm_ts, wm_id, message FROM weir_run"
              + (jobId == null ? "" : " WHERE job_id = ?")
              + " ORDER BY finished_at DESC";
      List<RunReport> out = new ArrayList<>();
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        if (jobId != null) {
          ps.setString(1, jobId);
        }
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            Timestamp wmTs = rs.getTimestamp(9);
            long wmId = rs.getLong(10);
            boolean wmIdNull = rs.wasNull();
            Timestamp started = rs.getTimestamp(5);
            out.add(
                RunReport.builder(rs.getString(2), rs.getString(1), modeOf(rs.getString(3)))
                    .status(rs.getString(4))
                    .success(!"FAILED".equalsIgnoreCase(rs.getString(4)))
                    .startedAt(started == null ? null : started.toInstant())
                    .durationMs(rs.getLong(6))
                    .rowsRead(rs.getLong(7))
                    .rowsWritten(rs.getLong(8))
                    .endWatermark(
                        Watermark.ofTsId(wmTs == null ? null : wmTs.toInstant(), wmIdNull ? null : wmId))
                    .message(rs.getString(11))
                    .build());
            if (limit > 0 && out.size() >= limit) {
              break;
            }
          }
        }
      } catch (SQLException e) {
        throw new IllegalStateException("loadRuns failed", e);
      }
      return out;
    }

    private static RunMode modeOf(String s) {
      if (s == null || s.isBlank()) {
        return RunMode.INCREMENTAL;
      }
      try {
        return RunMode.from(s);
      } catch (IllegalArgumentException e) {
        return RunMode.INCREMENTAL;
      }
    }

    @Override
    public void saveSnapshotMeta(String jobId, String key, String value) {
      try (PreparedStatement del =
          connection.prepareStatement("DELETE FROM weir_snapshot WHERE job_id = ? AND meta_key = ?")) {
        del.setString(1, jobId);
        del.setString(2, key);
        del.executeUpdate();
      } catch (SQLException e) {
        throw new IllegalStateException("saveSnapshotMeta (clear) failed", e);
      }
      try (PreparedStatement ps =
          connection.prepareStatement(
              "INSERT INTO weir_snapshot (job_id, meta_key, meta_value) VALUES (?, ?, ?)")) {
        ps.setString(1, jobId);
        ps.setString(2, key);
        ps.setString(3, value == null ? "" : value);
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw new IllegalStateException("saveSnapshotMeta failed", e);
      }
    }

    @Override
    public String loadSnapshotMeta(String jobId, String key) {
      String sql = "SELECT meta_value FROM weir_snapshot WHERE job_id = ? AND meta_key = ?";
      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setString(1, jobId);
        ps.setString(2, key);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            return null;
          }
          String v = rs.getString(1);
          return v == null || v.isEmpty() ? null : v;
        }
      } catch (SQLException e) {
        throw new IllegalStateException("loadSnapshotMeta failed", e);
      }
    }

    @Override
    public void close() {
      try {
        connection.close();
      } catch (SQLException ignored) {
        // ignore
      }
    }
  }
}
