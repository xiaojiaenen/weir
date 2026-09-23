package io.weir.writer.doris;

import io.weir.config.JobConfig;
import io.weir.model.DataRow;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Doris / StarRocks FE Stream Load writer (HTTP). Supports unique-key upsert and soft-delete via
 * {@code __DORIS_DELETE_SIGN__=1}. Batches rows as JSON array into one stream load per writeBatch.
 */
public final class DorisStreamLoadWriter implements RowWriter {
  private final JobConfig config;
  private List<String> columns = List.of();
  private List<String> pk = List.of();

  public DorisStreamLoadWriter(JobConfig config) {
    this.config = config;
  }

  @Override
  public void open() {
    pk =
        config.target.primaryKey != null && !config.target.primaryKey.isEmpty()
            ? config.target.primaryKey
            : (config.source.read.primaryKey == null ? List.of() : config.source.read.primaryKey);
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    if (batch.isEmpty()) {
      return;
    }
    if (columns.isEmpty()) {
      columns = new ArrayList<>(batch.get(0).values().keySet());
      columns.remove("_op");
    }
    StringBuilder body = new StringBuilder("[");
    for (int i = 0; i < batch.size(); i++) {
      if (i > 0) {
        body.append(',');
      }
      body.append(toJson(batch.get(i)));
    }
    body.append(']');
    streamLoad(body.toString());
  }

  private String toJson(DataRow row) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    Object op = row.get("_op");
    boolean del = op != null && ("d".equalsIgnoreCase(String.valueOf(op)) || "delete".equalsIgnoreCase(String.valueOf(op)));
    for (String c : columns) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(esc(c)).append("\":").append(val(row.get(c)));
    }
    if (del) {
      sb.append(",\"__DORIS_DELETE_SIGN__\":\"1\"");
    }
    return sb.append('}').toString();
  }

  private void streamLoad(String json) throws SQLException {
    String db = config.target.database == null ? "default_cluster" : config.target.database;
    String table = config.target.table;
    String url =
        "http://"
            + config.target.feHost
            + ":"
            + config.target.fePort
            + "/api/"
            + db
            + "/"
            + table
            + "/_stream_load";
    try {
      HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
      conn.setRequestMethod("PUT");
      conn.setDoOutput(true);
      conn.setConnectTimeout(30_000);
      conn.setReadTimeout(120_000);
      String user = config.target.user == null ? "root" : config.target.user;
      String pass = config.target.password == null ? "" : config.target.password;
      String auth = Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
      conn.setRequestProperty("Authorization", "Basic " + auth);
      conn.setRequestProperty("format", "json");
      conn.setRequestProperty("strip_outer_array", "true");
      conn.setRequestProperty("Expect", "100-continue");
      conn.setRequestProperty("column_separator", ",");
      String cols = String.join(",", columns);
      conn.setRequestProperty("columns", cols);
      // unique key upsert is default when table model is UNIQUE
      conn.setRequestProperty("merge_type", "MERGE");
      try (OutputStream os = conn.getOutputStream()) {
        os.write(json.getBytes(StandardCharsets.UTF_8));
      }
      int code = conn.getResponseCode();
      String resp =
          new String(
                  (code >= 200 && code < 300 ? conn.getInputStream() : conn.getErrorStream())
                      .readAllBytes(),
                  StandardCharsets.UTF_8);
      if (code != 200) {
        throw new SQLException("doris stream load HTTP " + code + ": " + resp);
      }
      // body contains Status: Success / Label / NumberLoadedRows
      if (!resp.contains("Success") && !resp.contains("\"Status\":\"Success\"") && !resp.contains("OK")) {
        // some versions return JSON with Status
        if (resp.contains("Fail") || resp.contains("Error")) {
          throw new SQLException("doris stream load failed: " + resp);
        }
      }
    } catch (IOException e) {
      throw new SQLException("doris stream load error: " + e.getMessage(), e);
    }
  }

  private static String val(Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof Number || v instanceof Boolean) {
      return String.valueOf(v);
    }
    if (v instanceof Instant i) {
      return '"' + i.toString() + '"';
    }
    return '"' + esc(String.valueOf(v)) + '"';
  }

  private static String esc(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  @Override
  public void close() {}

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "doris";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new DorisStreamLoadWriter(config);
    }
  }
}
