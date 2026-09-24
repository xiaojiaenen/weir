package io.weir.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Sync cursor. For ID strategy only id is used. For update_time strategies ts is the cursor and id
 * is the tie-break for UPDATE_TIME_ID.
 */
public final class Watermark {
  private final Instant ts;
  private final Long id;

  public Watermark(Instant ts, Long id) {
    this.ts = ts;
    this.id = id;
  }

  public static Watermark empty() {
    return new Watermark(null, null);
  }

  public static Watermark ofId(long id) {
    return new Watermark(null, id);
  }

  public static Watermark ofTs(Instant ts) {
    return new Watermark(ts, null);
  }

  public static Watermark ofTsId(Instant ts, Long id) {
    return new Watermark(ts, id);
  }

  public Instant ts() {
    return ts;
  }

  public Long id() {
    return id;
  }

  public boolean isEmpty() {
    return ts == null && id == null;
  }

  /** The further-ahead of two cursors: newer timestamp wins, then larger id. */
  public static Watermark max(Watermark a, Watermark b) {
    if (a == null || a.isEmpty()) {
      return b == null ? a : b;
    }
    if (b == null || b.isEmpty()) {
      return a;
    }
    if (a.ts != null && b.ts != null) {
      int c = a.ts.compareTo(b.ts);
      if (c != 0) {
        return c > 0 ? a : b;
      }
    } else if (a.ts != null) {
      return a;
    } else if (b.ts != null) {
      return b;
    }
    if (a.id == null) {
      return b;
    }
    if (b.id == null) {
      return a;
    }
    return a.id >= b.id ? a : b;
  }

  public Watermark withTs(Instant newTs) {
    return new Watermark(newTs, this.id);
  }

  public Watermark withId(Long newId) {
    return new Watermark(this.ts, newId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Watermark watermark)) {
      return false;
    }
    return Objects.equals(ts, watermark.ts) && Objects.equals(id, watermark.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ts, id);
  }

  @Override
  public String toString() {
    return "Watermark{ts=" + ts + ", id=" + id + '}';
  }
}
