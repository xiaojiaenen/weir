package io.weir.writer.jdbc;

import io.weir.config.JobConfig;
import io.weir.dialect.Dialects;
import io.weir.dialect.JdbcDialect;
import io.weir.model.DataRow;
import io.weir.schema.SchemaEvolution;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * JDBC sink with merge (upsert) or plain batch insert.
 *
 * <p>All DML is produced by the {@link JdbcDialect} resolved from the target URL. Previously this
 * class re-implemented five vendor upsert dialects by sniffing the URL, which silently drifted from
 * the dialect classes the source side used.
 *
 * <p>Merge is what makes retries safe: replaying the same batch is a no-op, so at-least-once
 * delivery plus PK merge gives effectively-once business state.
 */
public final class JdbcSinkWriter implements RowWriter {
  private final JobConfig config;
  private final JdbcDialect dialect;
  private Connection connection;
  /** DML cache keyed by column set: a delete marker and a full row must not share one statement. */
  private final Map<List<String>, String> dmlCache = new HashMap<>();
  private String deleteSql;
  private List<String> pk;

  public JdbcSinkWriter(JobConfig config) {
    this.config = config;
    this.dialect = Dialects.forUrl(config.target.url);
  }

  @Override
  public void open() throws SQLException {
    Properties props = new Properties();
    if (config.target.user != null) {
      props.setProperty("user", config.target.user);
    }
    if (config.target.password != null) {
      props.setProperty("password", config.target.password);
    }
    connection = DriverManager.getConnection(config.target.url, props);
    try {
      connection.setAutoCommit(false);
    } catch (SQLException e) {
      // Some drivers/URLs reject transaction control; batching still works.
    }
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    if (batch.isEmpty()) {
      return;
    }
    if (config.quality != null && config.quality.autoAddColumns) {
      evolveSchema(batch.get(0));
    }
    resolveKeys();

    List<DataRow> upserts = new ArrayList<>();
    List<DataRow> deletes = new ArrayList<>();
    for (DataRow row : batch) {
      if (isDelete(row)) {
        deletes.add(row);
      } else {
        upserts.add(row);
      }
    }

    try {
      if (!upserts.isEmpty()) {
        // Rows in one batch can carry different projections (e.g. after schema evolution),
        // so group them by column set rather than assuming one shape.
        Map<List<String>, List<DataRow>> groups = new LinkedHashMap<>();
        for (DataRow row : upserts) {
          groups.computeIfAbsent(columnsOf(row), k -> new ArrayList<>()).add(row);
        }
        for (Map.Entry<List<String>, List<DataRow>> g : groups.entrySet()) {
          List<String> cols = g.getKey();
          String sql = dmlFor(cols);
          try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (DataRow row : g.getValue()) {
              bind(ps, row, cols);
              ps.addBatch();
            }
            ps.executeBatch();
          }
        }
      }
      if (!deletes.isEmpty()) {
        if (deleteSql == null) {
          throw new SQLException("soft-delete (_op=d) requires target.primaryKey");
        }
        try (PreparedStatement ps = connection.prepareStatement(deleteSql)) {
          for (DataRow row : deletes) {
            for (int i = 0; i < pk.size(); i++) {
              setObject(ps, i + 1, row.get(pk.get(i)));
            }
            ps.addBatch();
          }
          ps.executeBatch();
        }
      }
      connection.commit();
    } catch (SQLException e) {
      try {
        connection.rollback();
      } catch (SQLException ignored) {
        // keep the original error
      }
      throw e;
    }
  }

  /** Additive schema evolution: new source columns appear on the target automatically. */
  private void evolveSchema(DataRow sample) {
    try {
      SchemaEvolution.Plan plan =
          SchemaEvolution.planAddColumns(connection, config.target.table, sample.values());
      if (!plan.missingColumns().isEmpty()) {
        List<String> added =
            SchemaEvolution.applyAddColumns(connection, config.target.table, plan, dialect);
        if (!added.isEmpty()) {
          // The target's column set changed — cached statements are now stale.
          dmlCache.clear();
        }
      }
    } catch (SQLException e) {
      // Non-fatal when ALTER is unsupported; the write below will fail with a clear SQL error.
      System.err.println("[weir-jdbc] schema evolution skipped: " + e.getMessage());
    }
  }

  private void resolveKeys() {
    if (pk != null) {
      return;
    }
    pk = config.effectivePrimaryKeys();
    if (!pk.isEmpty()) {
      deleteSql = dialect.buildDelete(config.target.table, pk);
    }
  }

  /** Statement for one column set, built through the dialect and cached. */
  private String dmlFor(List<String> cols) {
    return dmlCache.computeIfAbsent(
        cols,
        c -> {
          boolean merge = "merge".equalsIgnoreCase(config.target.writeMode) && !pk.isEmpty();
          if (merge) {
            if (!dialect.supportsMerge()) {
              throw new IllegalStateException(
                  "target dialect "
                      + dialect.name()
                      + " has no native upsert; set target.writeMode=append or use another sink");
            }
            return dialect.buildMerge(config.target.table, pk, c);
          }
          return dialect.buildInsert(config.target.table, c);
        });
  }

  /** Column projection of a row without the internal op marker, in insertion order. */
  private static List<String> columnsOf(DataRow row) {
    Set<String> cols = new LinkedHashSet<>(row.values().keySet());
    cols.remove("_op");
    return List.copyOf(cols);
  }

  private static boolean isDelete(DataRow row) {
    Object op = row.get("_op");
    if (op == null) {
      return false;
    }
    String s = String.valueOf(op);
    return "d".equalsIgnoreCase(s) || "delete".equalsIgnoreCase(s);
  }

  private static void bind(PreparedStatement ps, DataRow row, List<String> columns)
      throws SQLException {
    for (int i = 0; i < columns.size(); i++) {
      setObject(ps, i + 1, row.get(columns.get(i)));
    }
  }

  private static void setObject(PreparedStatement ps, int index, Object v) throws SQLException {
    if (v == null) {
      ps.setNull(index, Types.NULL);
    } else if (v instanceof Instant instant) {
      ps.setTimestamp(index, java.sql.Timestamp.from(instant));
    } else if (v instanceof java.time.LocalDate ld) {
      ps.setDate(index, java.sql.Date.valueOf(ld));
    } else {
      ps.setObject(index, v);
    }
  }

  @Override
  public void close() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "jdbc";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new JdbcSinkWriter(config);
    }
  }
}
