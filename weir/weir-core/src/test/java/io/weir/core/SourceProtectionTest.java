package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.api.SyncResult;
import io.weir.config.ConfigLoader;
import io.weir.config.JobConfig;
import io.weir.core.run.WeirRunner;
import io.weir.core.util.Guard;
import io.weir.core.util.JdbcPool;
import io.weir.model.RunMode;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Weir reads a production OLTP database it does not own. These checks pin the brakes: bounded
 * connections, bounded rows/sec, bounded retries, and fingerprint short-circuiting so a diff pass
 * does not scan a huge table unnecessarily.
 */
class SourceProtectionTest {

  @TempDir Path tmp;

  private String db(String name) {
    return "jdbc:h2:" + tmp.resolve(name).toAbsolutePath() + ";MODE=MySQL";
  }

  @Test
  void poolEnforcesAnUpperBoundOnConnections() throws Exception {
    try (JdbcPool pool = JdbcPool.of(db("pool"), "sa", "", 2)) {
      Connection a = pool.borrow(Duration.ofSeconds(5));
      Connection b = pool.borrow(Duration.ofSeconds(5));
      assertEquals(2, pool.inUse());
      // A third caller must wait, not open another connection behind the operator's back.
      assertThrows(
          java.sql.SQLException.class, () -> pool.borrow(Duration.ofMillis(200)),
          "poolMax is a hard ceiling");
      pool.release(a);
      try (Connection c = pool.borrow(Duration.ofSeconds(5))) {
        assertTrue(!c.isClosed(), "a freed slot is handed to the next caller");
      }
      pool.release(b);
    }
  }

  @Test
  void poolReusesConnections() throws Exception {
    try (JdbcPool pool = JdbcPool.of(db("reuse"), "sa", "", 1)) {
      Connection first = pool.borrow(Duration.ofSeconds(5));
      pool.release(first);
      Connection second = pool.borrow(Duration.ofSeconds(5));
      assertEquals(first, second, "an idle connection is reused rather than re-opened");
      pool.release(second);
    }
  }

  @Test
  void throttleSlowsDownWhenRowsExceedTheBudget() {
    Guard guard = new Guard(4, 2_000L, 3, 1L, 1L, 0L);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      guard.accountRows(500);
    }
    long elapsed = System.currentTimeMillis() - start;
    // 5000 rows at 2000/s requires at least ~1.5s of sleeping.
    assertTrue(elapsed >= 1000, "rows/sec is actually enforced, elapsed=" + elapsed + "ms");
    assertTrue(guard.throttledMs() > 0, "time spent throttled is recorded");
  }

  @Test
  void retryBacksOffAndEventuallyGivesUp() {
    Guard guard = new Guard(4, 0L, 3, 10L, 50L, 0L);
    AtomicInteger attempts = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            guard.withRetry(
                () -> {
                  attempts.incrementAndGet();
                  throw new IllegalStateException("boom");
                }));
    assertEquals(3, attempts.get(), "maxRetries bounds how many times we hit the source");
    assertEquals(2, guard.retries(), "each retry is counted for the run report");
  }

  @Test
  void retrySucceedsOnALaterAttempt() {
    Guard guard = new Guard(4, 0L, 3, 1L, 5L, 0L);
    AtomicInteger attempts = new AtomicInteger();
    String value =
        guard.withRetry(
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw new IllegalStateException("transient");
              }
              return "ok";
            });
    assertEquals("ok", value);
    assertEquals(3, attempts.get());
  }

  @Test
  void fingerprintSkipsTheRowLevelDiffWhenNothingChanged() throws Exception {
    try (Connection c = DriverManager.getConnection(db("fp-src"), "sa", "");
        Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
      st.execute("INSERT INTO t VALUES (1,'a'),(2,'b')");
    }
    try (Connection c = DriverManager.getConnection(db("fp-dst"), "sa", "");
        Statement st = c.createStatement()) {
      st.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(20))");
      st.execute("INSERT INTO t VALUES (1,'a')");
    }
    String yml =
        """
        name: fp
        mode: diff
        source:
          type: h2
          url: %s
          user: sa
          password: ""
          read:
            mode: table
            table: t
            columns: [id, name]
            primaryKey: [id]
            incremental:
              strategy: id
              idColumn: id
            deleteDetect:
              mode: fingerprint
        target:
          type: jdbc
          url: %s
          user: sa
          password: ""
          table: t
          writeMode: merge
          primaryKey: [id]
        state:
          type: file
          path: %s
        """
            .formatted(db("fp-src"), db("fp-dst"), tmp.resolve("st").toAbsolutePath());
    JobConfig config = ConfigLoader.fromYaml(yml);

    SyncResult first = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(first.success(), first.message());
    assertTrue(
        first.message().contains("drilling"), "no stored fingerprint yet, so it reconciles: " + first.message());
    assertEquals(2, countTarget());

    SyncResult second = new WeirRunner(config).run(RunMode.DIFF);
    assertTrue(second.success(), second.message());
    assertTrue(
        second.message().contains("skipped"),
        "an unchanged fingerprint skips the row-level scan: " + second.message());
    assertEquals(0, second.report().rowsWritten(), "nothing was written on the skipped pass");
  }

  private int countTarget() throws Exception {
    try (Connection c = DriverManager.getConnection(db("fp-dst"), "sa", "");
        Statement st = c.createStatement();
        var rs = st.executeQuery("SELECT COUNT(*) FROM t")) {
      assertTrue(rs.next());
      return rs.getInt(1);
    }
  }
}
