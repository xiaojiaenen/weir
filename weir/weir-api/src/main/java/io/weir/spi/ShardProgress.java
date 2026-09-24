package io.weir.spi;

import io.weir.model.Watermark;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Per-shard progress of a full (snapshot) sync, persisted between runs so a crashed run resumes
 * from the first unfinished shard instead of re-scanning the whole table.
 *
 * @param planId fingerprint of the shard plan; progress from a different plan is discarded
 * @param done indexes of shards whose rows were fully written and committed
 * @param maxWatermark running cursor max over completed shards (seed for the final watermark)
 */
public record ShardProgress(String planId, Set<Integer> done, Watermark maxWatermark) {

  public static ShardProgress empty(String planId) {
    return new ShardProgress(planId, Set.of(), Watermark.empty());
  }

  public ShardProgress {
    done = done == null ? Set.of() : Set.copyOf(done);
  }

  public ShardProgress withShardDone(int index, Watermark candidate) {
    Set<Integer> next = new TreeSet<>(done);
    next.add(index);
    return new ShardProgress(planId, next, Watermark.max(maxWatermark, candidate));
  }

  /** Comma form used by the state stores; stable and human-readable. */
  public String doneAsText() {
    StringBuilder sb = new StringBuilder();
    for (Integer i : new TreeSet<>(done)) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append(i);
    }
    return sb.toString();
  }

  public static Set<Integer> parseDone(String text) {
    if (text == null || text.isBlank()) {
      return Set.of();
    }
    TreeSet<Integer> out = new TreeSet<>();
    for (String part : text.split(",")) {
      String t = part.trim();
      if (!t.isEmpty()) {
        out.add(Integer.parseInt(t));
      }
    }
    return out;
  }

  /** Encode a watermark the same way the file state store does: {@code ts|id}. */
  public static String encodeWatermark(Watermark w) {
    if (w == null) {
      return "";
    }
    return (w.ts() == null ? "" : w.ts().toString()) + "|" + (w.id() == null ? "" : w.id());
  }

  public static Watermark decodeWatermark(String raw) {
    if (raw == null || raw.isBlank()) {
      return Watermark.empty();
    }
    String[] parts = raw.split("\\|", -1);
    java.time.Instant ts = parts[0].isEmpty() ? null : java.time.Instant.parse(parts[0]);
    Long id = parts.length < 2 || parts[1].isEmpty() ? null : Long.parseLong(parts[1]);
    return Watermark.ofTsId(ts, id);
  }

  /** Keys used underneath {@code saveSnapshotMeta}. */
  public static List<String> metaKeys() {
    return List.of("full.planId", "full.done", "full.wm");
  }
}
