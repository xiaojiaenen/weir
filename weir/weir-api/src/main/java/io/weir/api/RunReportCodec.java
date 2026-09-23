package io.weir.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.weir.model.RunMode;
import io.weir.model.Watermark;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON encoding for {@link RunReport} that needs no extra Jackson modules.
 *
 * <p>Reports contain {@code Instant} and are Java records. Serialising them directly requires
 * jackson-datatype-jsr310 plus parameter-name metadata, and deserialising a record without those
 * silently yields nothing. Going through a plain map keeps the codec self-contained — the run
 * history is operational data, and it must never depend on classpath luck.
 */
public final class RunReportCodec {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private RunReportCodec() {}

  public static String toJson(RunReport r) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("jobId", r.jobId());
    m.put("runId", r.runId());
    m.put("mode", r.mode() == null ? null : r.mode().name());
    m.put("status", r.status());
    m.put("success", r.success());
    m.put("startedAt", str(r.startedAt()));
    m.put("finishedAt", str(r.finishedAt()));
    m.put("durationMs", r.durationMs());
    m.put("rowsRead", r.rowsRead());
    m.put("rowsWritten", r.rowsWritten());
    m.put("rowsDeleted", r.rowsDeleted());
    m.put("rowsRewritten", r.rowsRewritten());
    m.put("batches", r.batches());
    m.put("retries", r.retries());
    m.put("shards", r.shards());
    m.put("startWatermark", wm(r.startWatermark()));
    m.put("endWatermark", wm(r.endWatermark()));
    m.put("lagMillis", r.lagMillis());
    m.put("rowsPerSecond", r.rowsPerSecond());
    m.put("qualityFindings", new ArrayList<>(r.qualityFindings()));
    m.put("message", r.message());
    try {
      return MAPPER.writeValueAsString(m);
    } catch (Exception e) {
      // A run must never fail because its own report could not be encoded.
      return "{\"jobId\":" + quote(r.jobId()) + ",\"status\":" + quote(r.status()) + "}";
    }
  }

  public static String toPrettyJson(RunReport r) {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(MAPPER.readTree(toJson(r)));
    } catch (Exception e) {
      return toJson(r);
    }
  }

  public static RunReport fromJson(String json) {
    Map<String, Object> m;
    try {
      m = MAPPER.readValue(json, Map.class);
    } catch (Exception e) {
      return null;
    }
    if (m == null) {
      return null;
    }
    RunReport.Builder b =
        RunReport.builder(str(m.get("jobId")), str(m.get("runId")), mode(m.get("mode")))
            .status(str(m.get("status")))
            .success(bool(m.get("success")))
            .startedAt(instant(m.get("startedAt")))
            .finishedAt(instant(m.get("finishedAt")))
            .durationMs(lng(m.get("durationMs")))
            .rowsRead(lng(m.get("rowsRead")))
            .rowsWritten(lng(m.get("rowsWritten")))
            .rowsDeleted(lng(m.get("rowsDeleted")))
            .rowsRewritten(lng(m.get("rowsRewritten")))
            .batches(lng(m.get("batches")))
            .retries(lng(m.get("retries")))
            .shards(lng(m.get("shards")))
            .lagMillis(lng(m.get("lagMillis")))
            .rowsPerSecond(dbl(m.get("rowsPerSecond")))
            .startWatermark(watermark(m.get("startWatermark")))
            .endWatermark(watermark(m.get("endWatermark")))
            .message(str(m.get("message")));
    Object findings = m.get("qualityFindings");
    if (findings instanceof List<?> list) {
      for (Object o : list) {
        b.addFinding(o == null ? null : String.valueOf(o));
      }
    }
    return b.build();
  }

  private static Map<String, Object> wm(Watermark w) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ts", w == null ? null : str(w.ts()));
    m.put("id", w == null ? null : w.id());
    return m;
  }

  private static Watermark watermark(Object o) {
    if (!(o instanceof Map<?, ?> m)) {
      return Watermark.empty();
    }
    Instant ts = instant(m.get("ts"));
    Object id = m.get("id");
    Long idVal = id == null ? null : ((Number) id).longValue();
    return Watermark.ofTsId(ts, idVal);
  }

  private static String str(Instant i) {
    return i == null ? null : i.toString();
  }

  private static String str(Object o) {
    return o == null ? null : String.valueOf(o);
  }

  private static Instant instant(Object o) {
    if (o == null) {
      return null;
    }
    try {
      return Instant.parse(String.valueOf(o));
    } catch (Exception e) {
      return null;
    }
  }

  private static long lng(Object o) {
    return o instanceof Number n ? n.longValue() : 0L;
  }

  private static double dbl(Object o) {
    return o instanceof Number n ? n.doubleValue() : 0d;
  }

  private static boolean bool(Object o) {
    return Boolean.TRUE.equals(o);
  }

  private static RunMode mode(Object o) {
    if (o == null) {
      return RunMode.INCREMENTAL;
    }
    try {
      return RunMode.from(String.valueOf(o));
    } catch (IllegalArgumentException e) {
      return RunMode.INCREMENTAL;
    }
  }

  private static String quote(String s) {
    if (s == null) {
      return "null";
    }
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
