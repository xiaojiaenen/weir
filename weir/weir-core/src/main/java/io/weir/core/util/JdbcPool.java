package io.weir.core.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Minimal fixed-capacity JDBC connection pool.
 *
 * <p>Parallel shard extraction needs several connections at once, but an unbounded "one connection
 * per shard" strategy is exactly how a sync job takes down an OLTP source. This pool enforces
 * {@code source.poolMax} and makes shards wait instead.
 */
public final class JdbcPool implements AutoCloseable {
  private final String url;
  private final String user;
  private final String password;
  private final int max;
  private final Deque<Connection> idle = new ArrayDeque<>();
  private final Object lock = new Object();
  private int inUse;
  private int created;
  private boolean closed;

  public JdbcPool(String url, String user, String password, int max) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.max = Math.max(1, max);
  }

  public static JdbcPool of(String url, String user, String password, int max) {
    return new JdbcPool(url, user, password, max);
  }

  /** Borrow a connection, waiting up to {@code timeout} when the pool is saturated. */
  public Connection borrow(Duration timeout) throws SQLException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    synchronized (lock) {
      while (true) {
        if (closed) {
          throw new SQLException("connection pool is closed");
        }
        Connection pooled = idle.poll();
        if (pooled != null) {
          if (isUsable(pooled)) {
            inUse++;
            return pooled;
          }
          created--;
          closeQuietly(pooled);
          continue;
        }
        if (created < max) {
          Connection c = newConnection();
          created++;
          inUse++;
          return c;
        }
        long wait = deadline - System.currentTimeMillis();
        if (wait <= 0) {
          throw new SQLException(
              "timed out waiting for a source connection (poolMax=" + max + ")");
        }
        try {
          lock.wait(Math.min(wait, 1000L));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException("interrupted waiting for a source connection", e);
        }
      }
    }
  }

  /** Return a connection; broken ones are discarded instead of handed to the next shard. */
  public void release(Connection c) {
    if (c == null) {
      return;
    }
    synchronized (lock) {
      inUse--;
      if (closed || !isUsable(c)) {
        created--;
        closeQuietly(c);
      } else {
        try {
          c.setAutoCommit(true);
        } catch (SQLException ignored) {
          created--;
          closeQuietly(c);
          lock.notifyAll();
          return;
        }
        idle.push(c);
      }
      lock.notifyAll();
    }
  }

  private Connection newConnection() throws SQLException {
    Connection c = Jdbc.open(url, user, password);
    try {
      c.setAutoCommit(false);
      c.setReadOnly(true);
    } catch (SQLException e) {
      closeQuietly(c);
      throw e;
    }
    return c;
  }

  private static boolean isUsable(Connection c) {
    try {
      return !c.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  private static void closeQuietly(Connection c) {
    try {
      c.close();
    } catch (SQLException ignored) {
      // ignore
    }
  }

  public int maxSize() {
    return max;
  }

  public int inUse() {
    synchronized (lock) {
      return inUse;
    }
  }

  @Override
  public void close() {
    List<Connection> toClose = new ArrayList<>();
    synchronized (lock) {
      closed = true;
      toClose.addAll(idle);
      idle.clear();
      lock.notifyAll();
    }
    toClose.forEach(JdbcPool::closeQuietly);
  }
}
