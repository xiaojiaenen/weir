package io.weir.core.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Source protection: caps concurrent queries, throttles rows/sec with a token bucket, and retries
 * with capped exponential backoff plus jitter.
 *
 * <p>The sync target is a live OLTP database we do not own. Everything here exists to keep Weir
 * from being the reason it falls over.
 */
public final class Guard {
  private final Semaphore queryPermits;
  private final long maxRowsPerSec;
  private final double refillPerMs;
  private final int maxRetries;
  private final long backoffMs;
  private final long backoffMaxMs;
  private final long jitterMs;
  private final AtomicLong retries = new AtomicLong();
  private final AtomicLong throttledMs = new AtomicLong();
  private final AtomicLong tokens = new AtomicLong();
  private volatile long lastRefill = System.currentTimeMillis();

  public Guard(int maxConcurrentQueries, long maxRowsPerSec) {
    this(maxConcurrentQueries, maxRowsPerSec, 3, 500L, 10_000L, 250L);
  }

  public Guard(
      int maxConcurrentQueries,
      long maxRowsPerSec,
      int maxRetries,
      long backoffMs,
      long backoffMaxMs,
      long jitterMs) {
    this.queryPermits = new Semaphore(Math.max(1, maxConcurrentQueries));
    this.maxRowsPerSec = maxRowsPerSec;
    this.refillPerMs = maxRowsPerSec <= 0 ? 0d : maxRowsPerSec / 1000d;
    this.maxRetries = Math.max(1, maxRetries);
    this.backoffMs = Math.max(0L, backoffMs);
    this.backoffMaxMs = Math.max(this.backoffMs, backoffMaxMs);
    this.jitterMs = Math.max(0L, jitterMs);
  }

  /** Block until this run is allowed to hold another source query. */
  public void acquireQuery() {
    try {
      if (!queryPermits.tryAcquire(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timeout waiting for query permit");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted acquiring query permit", e);
    }
  }

  public void releaseQuery() {
    queryPermits.release();
  }

  /**
   * Consume {@code n} rows from the token bucket, sleeping when the budget is exhausted. Unlike a
   * fixed {@code sleep(50)} this actually tracks elapsed time, so a configured 10k rows/s means
   * 10k rows/s.
   */
  public void accountRows(long n) {
    if (refillPerMs <= 0 || n <= 0) {
      return;
    }
    refill();
    long remaining = n;
    while (remaining > 0) {
      long available = tokens.get();
      if (available <= 0) {
        long sleepMs =
            Math.min(100L, Math.max(1L, (long) Math.ceil((double) remaining / refillPerMs)));
        sleep(sleepMs);
        refill();
        continue;
      }
      long take = Math.min(available, remaining);
      if (tokens.compareAndSet(available, available - take)) {
        remaining -= take;
      }
    }
  }

  private void refill() {
    long now = System.currentTimeMillis();
    long elapsed = now - lastRefill;
    if (elapsed <= 0) {
      return;
    }
    synchronized (this) {
      long again = now - lastRefill;
      if (again <= 0) {
        return;
      }
      lastRefill = now;
      long add = (long) (again * refillPerMs);
      if (add <= 0) {
        return;
      }
      long cur = tokens.get();
      long cap = maxRowsPerSec;
      long next = Math.min(cap, cur + add);
      tokens.set(next);
    }
  }

  private void sleep(long ms) {
    throttledMs.addAndGet(ms);
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Retry count accumulated across the run. */
  public long retries() {
    return retries.get();
  }

  /** Milliseconds spent throttled. */
  public long throttledMs() {
    return throttledMs.get();
  }

  /**
   * Run {@code body} with capped exponential backoff and jitter. Rethrows the last failure once
   * attempts are exhausted.
   */
  public <T> T withRetry(Supplier<T> body) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return body.get();
      } catch (RuntimeException e) {
        last = e;
        if (attempt >= maxRetries) {
          break;
        }
        long backoff = Math.min(backoffMaxMs, backoffMs * (1L << (attempt - 1)));
        if (jitterMs > 0) {
          backoff += ThreadLocalRandom.current().nextLong(jitterMs);
        }
        retries.incrementAndGet();
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    throw last == null ? new IllegalStateException("retry failed") : last;
  }

  public int maxRetries() {
    return maxRetries;
  }
}
