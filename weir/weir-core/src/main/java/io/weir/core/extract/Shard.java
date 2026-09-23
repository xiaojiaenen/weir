package io.weir.core.extract;

import io.weir.config.JobConfig;
import io.weir.dialect.JdbcDialect;
import io.weir.model.SplitMode;
import io.weir.model.SplitRange;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * One unit of parallel work: either a half-open numeric range, or a modulus/hash remainder.
 *
 * <p>Ranges are preferred because they read contiguous index pages. Modulus sharding is used when
 * ids are sparse; hash sharding when the key is not numeric at all.
 */
public record Shard(SplitRange range, int modulus, int remainder, boolean hash) {

  public static Shard single() {
    return new Shard(new SplitRange(null, null), 0, 0, false);
  }

  public static Shard ofRange(SplitRange range) {
    return new Shard(range, 0, 0, false);
  }

  public static Shard ofMod(int modulus, int remainder) {
    return new Shard(null, modulus, remainder, false);
  }

  public static Shard ofHash(int modulus, int remainder) {
    return new Shard(null, modulus, remainder, true);
  }

  /** Predicate fragment for this shard, or empty for a full scan. */
  public String predicate(JdbcDialect dialect, String splitColumn) {
    if (hash) {
      return dialect.buildHashSplitWhere(splitColumn, modulus, remainder);
    }
    if (modulus > 0) {
      return dialect.buildModSplitWhere(splitColumn, modulus, remainder);
    }
    if (range == null) {
      return "";
    }
    return dialect.buildSplitWhere(range, splitColumn);
  }

  public boolean isFullScan() {
    return !hash && modulus <= 0 && (range == null || (range.lo() == null && range.hi() == null));
  }

  @Override
  public String toString() {
    if (hash) {
      return "hash(mod " + modulus + ")=" + remainder;
    }
    if (modulus > 0) {
      return "mod " + modulus + "=" + remainder;
    }
    if (range == null) {
      return "full";
    }
    return "[" + range.lo() + "," + (range.hi() == null ? "∞" : range.hi()) + ")";
  }
}
