package io.weir.core.extract;

import io.weir.api.RunMetrics;
import io.weir.api.WeirException;
import io.weir.config.JobConfig;
import io.weir.core.util.Guard;
import io.weir.core.util.JdbcPool;
import io.weir.core.util.Sql;
import io.weir.dialect.Dialects;
import io.weir.dialect.JdbcDialect;
import io.weir.model.DataRow;
import io.weir.model.IncrementalStrategy;
import io.weir.model.Watermark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * JDBC extractor: full snapshot (shardable), incremental cursors, and the narrow projections used
 * by diff passes.
 *
 * <p>Rows are streamed to a consumer; the caller owns watermark advance so that a cursor is only
 * committed after the batch has actually landed in the target.
 */
public final class JdbcExtractor implements AutoCloseable {
  private final JobConfig config;
  private final JdbcDialect dialect;
  private final JdbcPool pool;
  private final Guard guard;
  private final RunMetrics metrics;

  public JdbcExtractor(JobConfig config) throws SQLException {
    this(config, new RunMetrics(), newGuard(config));
  }

  public JdbcExtractor(JobConfig config, RunMetrics metrics, Guard guard) throws SQLException {
    this.config = config;
    this.metrics = metrics == null ? new RunMetrics() : metrics;
    this.guard = guard == null ? newGuard(config) : guard;
    this.dialect = Dialects.forType(config.source.type);
    this.pool =
        JdbcPool.of(
            config.source.url,
            config.source.user,
            config.source.password,
            Math.max(1, config.source.poolMax));
  }

  private static Guard newGuard(JobConfig config) {
    JobConfig.RuntimeConfig rt = config.runtime;
    return new Guard(
        rt.maxConcurrentQueries,
        rt.maxRowsPerSec,
        rt.maxRetries,
        rt.retryBackoffMs,
        rt.retryBackoffMaxMs,
        rt.retryJitterMs);
  }

  /**
   * Borrow a pooled connection for a short, non-streaming operation (metadata probes, preflight).
   * The body must not throw checked exceptions.
   */
  public <T> T withConnection(java.util.function.Function<Connection, T> body) throws SQLException {
    guard.acquireQuery();
    Connection conn = pool.borrow(Duration.ofSeconds(60));
    try {
      return body.apply(conn);
    } finally {
      pool.release(conn);
      guard.releaseQuery();
    }
  }

  public JdbcDialect dialect() {
    return dialect;
  }

  public Guard guard() {
    return guard;
  }

  public RunMetrics metrics() {
    return metrics;
  }

  public String fromClause() {
    return Sql.fromClause(config.source.read);
  }

  /** Plan the full-snapshot shards without running them (used by the checkpointed FULL path). */
  public SplitPlanner.Plan planShards() throws SQLException {
    try (Connection probe = pool.borrow(Duration.ofSeconds(30))) {
      return SplitPlanner.plan(probe, dialect, config.source.read);
    }
  }

  /** Extract exactly one planned shard — the unit of work an external engine (Flink/Spark) runs. */
  public ExtractStats extractShard(Shard shard, Consumer<DataRow> consumer) throws SQLException {
    ExtractStats stats = new ExtractStats();
    stats.shards = 1;
    fetchShard(shard, config.source.read.splits == null ? null : config.source.read.splits.column,
        null, stats, consumer);
    return stats;
  }

  public List<String> projection() {
    List<String> cols = config.source.read.columns;
    return cols == null || cols.isEmpty() ? List.of() : List.copyOf(cols);
  }

  /** Observed max cursor while extracting. */
  public static final class ExtractStats {
    public long rows;
    public long shards;
    public long failedShards;
    public Watermark maxWatermark = Watermark.empty();

    public void observe(Watermark candidate) {
      rows++;
      Instant ts = candidate == null ? null : candidate.ts();
      Long id = candidate == null ? null : candidate.id();
      Instant curTs = maxWatermark.ts();
      Long curId = maxWatermark.id();
      boolean better = false;
      if (ts != null) {
        if (curTs == null || ts.isAfter(curTs)) {
          better = true;
        } else if (ts.equals(curTs) && id != null && (curId == null || id > curId)) {
          better = true;
        }
      } else if (id != null && (curId == null || id > curId)) {
        better = true;
      }
      if (better) {
        maxWatermark = Watermark.ofTsId(ts, id);
      }
    }

    public void merge(ExtractStats other) {
      rows += other.rows;
      shards += other.shards;
      failedShards += other.failedShards;
      maxWatermark = betterOf(maxWatermark, other.maxWatermark);
    }

    private static Watermark betterOf(Watermark a, Watermark b) {
      if (a.isEmpty()) {
        return b;
      }
      if (b.isEmpty()) {
        return a;
      }
      Instant at = a.ts();
      Instant bt = b.ts();
      if (at != null && bt != null) {
        int c = at.compareTo(bt);
        if (c != 0) {
          return c > 0 ? a : b;
        }
      } else if (at != null) {
        return a;
      } else if (bt != null) {
        return b;
      }
      Long ai = a.id();
      Long bi = b.id();
      if (ai == null) {
        return b;
      }
      if (bi == null) {
        return a;
      }
      return ai >= bi ? a : b;
    }
  }

  // ------------------------------------------------------------------ full

  /**
   * Full snapshot. Shards are planned by {@link SplitPlanner} and, when
   * {@code splits.mode=parallel}, run on a bounded pool bounded further by {@code source.poolMax}.
   *
   * <p>The consumer is invoked from several threads; the runner is responsible for serialising
   * writes.
   */
  public ExtractStats extractFull(Consumer<DataRow> consumer) throws SQLException {
    return extractFull(consumer, null);
  }

  /** Full snapshot with an extra predicate (used by diff passes that only need part of a table). */
  public ExtractStats extractFull(Consumer<DataRow> consumer, String extraWhere)
      throws SQLException {
    ExtractStats stats = new ExtractStats();
    SplitPlanner.Plan plan;
    try (Connection probe = pool.borrow(Duration.ofSeconds(30))) {
      plan = SplitPlanner.plan(probe, dialect, config.source.read);
    }
    plan.warnings().forEach(w -> org.slf4j.LoggerFactory.getLogger(JdbcExtractor.class).warn(w));
    List<Shard> shards = plan.shards();

    String splitCol = config.source.read.splits.column;
    boolean parallel =
        shards.size() > 1
            && ("parallel".equalsIgnoreCase(config.source.read.splits.mode)
                || config.runtime.extractThreads > 1);
    int threads =
        Math.max(
            1,
            Math.min(
                parallel ? config.runtime.extractThreads : 1,
                Math.max(1, config.runtime.maxConcurrentQueries)));

    if (threads <= 1 || shards.size() <= 1) {
      for (Shard shard : shards) {
        stats.shards++;
        fetchShard(shard, splitCol, extraWhere, stats, consumer);
      }
    } else {
      ExecutorService executor = Executors.newFixedThreadPool(threads);
    List<Future<ExtractStats>> futures = new ArrayList<>();
    try {
      for (Shard shard : shards) {
        futures.add(
            executor.submit(
                (Callable<ExtractStats>)
                    () -> {
                      ExtractStats local = new ExtractStats();
                      local.shards = 1;
                      fetchShard(shard, splitCol, extraWhere, local, consumer);
                      return local;
                    }));
      }
      for (Future<ExtractStats> f : futures) {
        try {
          stats.merge(f.get());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException("parallel extract interrupted", e);
        } catch (java.util.concurrent.ExecutionException e) {
          Throwable cause = e.getCause() == null ? e : e.getCause();
          stats.failedShards++;
          if (cause instanceof SQLException sql) {
            throw sql;
          }
          throw new SQLException("shard extract failed: " + cause.getMessage(), cause);
        }
      }
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    }
    metrics.shards(stats.shards);
    return stats;
  }

  private void fetchShard(
      Shard shard, String splitCol, String extraWhere, ExtractStats stats, Consumer<DataRow> consumer)
      throws SQLException {
    String shardWhere =
        (splitCol != null && !splitCol.isBlank() && !shard.isFullScan())
            ? shard.predicate(dialect, splitCol)
            : null;
    String where = Sql.and(Sql.list(config.source.read.filter, extraWhere, shardWhere));
    String order = config.source.read.incremental.idColumn;
    order = (order == null || order.isBlank()) ? null : dialect.quote(order);
    String sql = dialect.buildSelect(projection(), fromClause(), where, order, null);
    fetchWithRetry(sql, List.of(), stats, consumer);
  }

  // ------------------------------------------------------------------ incremental

  /**
   * Incremental extract from {@code from}. Pages by {@code batchRows} on the cursor so a large
   * backlog is drained in bounded queries instead of one monster result set.
   *
   * <p>Does not advance state — the caller commits after the write succeeds.
   */
  public ExtractStats extractIncremental(Watermark from, Consumer<DataRow> consumer)
      throws SQLException {
    ExtractStats stats = new ExtractStats();
    JobConfig.SourceConfig.ReadConfig.IncrementalConfig inc = config.source.read.incremental;
    IncrementalStrategy strategy = inc.strategy;
    if (strategy == IncrementalStrategy.FULL) {
      return extractFull(consumer);
    }
    Duration overlap = io.weir.core.util.Durations.parse(inc.overlap);
    String orderBy =
        switch (strategy) {
          case ID -> dialect.quote(inc.idColumn);
          case UPDATE_TIME -> dialect.quote(inc.column);
          case UPDATE_TIME_ID ->
              dialect.quote(inc.column) + ", " + dialect.quote(inc.idColumn);
          default -> dialect.quote(inc.idColumn);
        };
    int batch = Math.max(1, inc.batchRows);
    long maxRows = inc.maxRowsPerRun <= 0 ? Long.MAX_VALUE : inc.maxRowsPerRun;
    Watermark cursor = from == null ? Watermark.empty() : from;

    for (int page = 0; ; page++) {
      String pageWhere =
          Sql.and(
              Sql.list(
                  config.source.read.filter,
                  dialect.buildIncrementWhere(
                      strategy, cursor, inc.column, inc.idColumn, overlap)));
      // Never ask for more than the per-run cap allows, otherwise the cap only kicks in after
      // the (potentially huge) first page has already been pulled from the source.
      int limit = (int) Math.min(batch, maxRows - stats.rows);
      String sql = dialect.buildSelect(projection(), fromClause(), pageWhere, orderBy, limit);
      ExtractStats pageStats = new ExtractStats();
      pageStats.shards = 1;
      fetchWithRetry(sql, List.of(), pageStats, consumer);

      stats.rows += pageStats.rows;
      if (pageStats.rows == 0 || pageStats.maxWatermark.isEmpty()) {
        break;
      }
      stats.observe(pageStats.maxWatermark);
      if (pageStats.rows < batch || stats.rows >= maxRows) {
        break;
      }
      Watermark next = pageStats.maxWatermark;
      if (next.equals(cursor)) {
        // Cursor stuck — same rows would be returned forever. Stop instead of looping.
        break;
      }
      cursor = next;
      if (page > 1_000_000) {
        break;
      }
    }
    stats.shards = 1;
    return stats;
  }

  // ------------------------------------------------------------------ narrow reads

  /** Only the primary-key columns — the cheap projection used for set diffing. */
  public List<Object[]> extractPrimaryKeySet(List<String> pkColumns) throws SQLException {
    List<Object[]> out = new ArrayList<>();
    String sql =
        dialect.buildSelect(
            pkColumns.stream().map(dialect::quote).toList(),
            fromClause(),
            config.source.read.filter,
            null,
            null);
    fetchWithRetry(
        sql,
        List.of(),
        new ExtractStats(),
        row -> {
          Object[] key = new Object[pkColumns.size()];
          for (int i = 0; i < key.length; i++) {
            key[i] = row.get(pkColumns.get(i));
          }
          out.add(key);
        });
    return out;
  }

  /** Full rows as a list — single scan, used by diff passes that need content comparison. */
  public List<DataRow> extractAllRows() throws SQLException {
    List<DataRow> out = new ArrayList<>();
    extractFull(out::add);
    return out;
  }

  /**
   * Stream source rows whose single key column falls in the half-open window
   * {@code (fromExclusive, toInclusive]}, in key order. A {@code null} {@code fromExclusive} means
   * unbounded below, a {@code null} {@code toInclusive} means unbounded above — the diff runner
   * walks the table one bounded window at a time so memory stays O(window).
   *
   * <p>Parameters are bound (not inlined) so a key taken from the target as {@code Long} still
   * compares correctly against a source {@code Integer} column.
   */
  public ExtractStats extractKeyWindow(
      String keyColumn, Object fromExclusive, Object toInclusive, Consumer<DataRow> consumer)
      throws SQLException {
    ExtractStats stats = new ExtractStats();
    stats.shards = 1;
    String key = dialect.quote(keyColumn);
    List<String> conds = new ArrayList<>();
    List<Object> params = new ArrayList<>();
    if (fromExclusive != null) {
      conds.add(key + " > ?");
      params.add(fromExclusive);
    }
    if (toInclusive != null) {
      conds.add(key + " <= ?");
      params.add(toInclusive);
    }
    List<String> allConds = Sql.list(config.source.read.filter);
    allConds.addAll(conds);
    String where = Sql.and(allConds);
    String sql = dialect.buildSelect(projection(), fromClause(), where, key, null);
    fetchWithRetry(sql, params, stats, consumer);
    return stats;
  }

  /** Aggregate fingerprint over a window: count + sum of row hashes. */
  public Fingerprint fingerprint(String partitionColumn, Instant from, Instant to)
      throws SQLException {
    String where =
        Sql.and(
            Sql.list(
                config.source.read.filter,
                partitionColumn == null || partitionColumn.isBlank()
                    ? null
                    : dialect.quote(partitionColumn)
                        + " >= "
                        + dialect.tsLiteral(from)
                        + " AND "
                        + dialect.quote(partitionColumn)
                        + " < "
                        + dialect.tsLiteral(to)));
    List<DataRow> rows = new ArrayList<>();
    String sql = dialect.buildSelect(projection(), fromClause(), where, null, null);
    fetchWithRetry(sql, List.of(), new ExtractStats(), rows::add);
    return Fingerprint.of(rows, config.source.read.columns);
  }

  // ------------------------------------------------------------------ plumbing

  private void fetchWithRetry(
      String sql, List<Object> params, ExtractStats stats, Consumer<DataRow> consumer)
      throws SQLException {
    int attempts = Math.max(1, config.runtime.maxRetries);
    SQLException last = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      guard.acquireQuery();
      Connection conn = null;
      try {
        conn = pool.borrow(Duration.ofSeconds(60));
        fetchOnce(conn, sql, params, stats, consumer);
        return;
      } catch (SQLException e) {
        last = e;
        if (conn != null) {
          pool.release(conn);
          conn = null;
        }
        if (attempt == attempts) {
          break;
        }
        metrics.addRetry();
        long backoff =
            Math.min(
                config.runtime.retryBackoffMaxMs,
                Math.max(1L, config.runtime.retryBackoffMs) * (1L << (attempt - 1)));
        if (config.runtime.retryJitterMs > 0) {
          backoff +=
              java.util.concurrent.ThreadLocalRandom.current()
                  .nextLong(config.runtime.retryJitterMs);
        }
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      } finally {
        if (conn != null) {
          pool.release(conn);
        }
        guard.releaseQuery();
      }
    }
    throw last == null ? new SQLException("extract failed") : last;
  }

  private void fetchOnce(
      Connection conn, String sql, List<Object> params, ExtractStats stats, Consumer<DataRow> consumer)
      throws SQLException {
    JobConfig.SourceConfig.ReadConfig.IncrementalConfig inc = config.source.read.incremental;
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      if (inc.fetchSize > 0) {
        ps.setFetchSize(inc.fetchSize);
      }
      int timeout = inc.queryTimeoutSeconds > 0 ? inc.queryTimeoutSeconds : 0;
      if (timeout > 0) {
        ps.setQueryTimeout(timeout);
      }
      for (int i = 0; i < params.size(); i++) {
        ps.setObject(i + 1, params.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        String[] labels = new String[n + 1];
        List<String> wanted = projection();
        for (int i = 1; i <= n; i++) {
          labels[i] = normalizeLabel(md.getColumnLabel(i), wanted);
        }
        while (rs.next()) {
          LinkedHashMap<String, Object> map = new LinkedHashMap<>();
          for (int i = 1; i <= n; i++) {
            map.put(labels[i], readValue(rs, md.getColumnType(i), i));
          }
          DataRow row = new DataRow(map);
          if (stats != null) {
            stats.observe(cursorOf(row));
          }
          metrics.addRead(1);
          guard.accountRows(1);
          if (consumer != null) {
            consumer.accept(row);
          }
        }
      }
    }
  }

  private Watermark cursorOf(DataRow row) {
    JobConfig.SourceConfig.ReadConfig.IncrementalConfig inc = config.source.read.incremental;
    Instant ts = null;
    Long id = null;
    Object tsVal = row.get(inc.column);
    if (tsVal instanceof Instant instant) {
      ts = instant;
    } else if (tsVal instanceof Timestamp timestamp) {
      ts = timestamp.toInstant();
    } else if (tsVal instanceof java.util.Date date) {
      ts = date.toInstant();
    } else if (tsVal instanceof Number number) {
      ts = Instant.ofEpochMilli(number.longValue());
    } else if (tsVal instanceof String s && !s.isBlank()) {
      try {
        ts = Instant.parse(s);
      } catch (Exception ignored) {
        // leave null
      }
    }
    Object idVal = row.get(inc.idColumn);
    if (idVal instanceof Number number) {
      id = number.longValue();
    } else if (idVal instanceof String s && !s.isBlank()) {
      try {
        id = Long.parseLong(s);
      } catch (NumberFormatException ignored) {
        // leave null
      }
    }
    return Watermark.ofTsId(ts, id);
  }

  /** Map a JDBC label back to the configured column name so cursors match the YAML. */
  private static String normalizeLabel(String label, List<String> wanted) {
    if (label == null || wanted.isEmpty()) {
      return label;
    }
    for (String w : wanted) {
      if (w.equalsIgnoreCase(label)) {
        return w;
      }
    }
    return label;
  }

  private static Object readValue(ResultSet rs, int type, int index) throws SQLException {
    Object v = rs.getObject(index);
    if (v == null) {
      return null;
    }
    return switch (type) {
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
        Timestamp ts = rs.getTimestamp(index);
        yield ts == null ? null : ts.toInstant();
      }
      case Types.DATE -> {
        java.sql.Date d = rs.getDate(index);
        yield d == null ? null : d.toLocalDate().toString();
      }
      default -> v;
    };
  }

  @Override
  public void close() throws SQLException {
    pool.close();
  }
}
