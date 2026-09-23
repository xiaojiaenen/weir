package io.weir.writer.file;

import io.weir.config.JobConfig;
import io.weir.model.DataRow;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/** JSONL / CSV file writer. Append-only; merge semantics are consumer-side (latest by pk/ts). */
public final class FileWriter implements RowWriter {
  private final JobConfig config;
  private final ReentrantLock lock = new ReentrantLock();
  private BufferedWriter writer;
  private List<String> headerOrder;

  public FileWriter(JobConfig config) {
    this.config = config;
  }

  @Override
  public void open() throws SQLException {
    try {
      Path path = Path.of(config.target.path);
      if (path.getParent() != null) {
        Files.createDirectories(path.getParent());
      }
      writer =
          Files.newBufferedWriter(
              path,
              StandardCharsets.UTF_8,
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND);
    } catch (IOException e) {
      throw new SQLException("Cannot open file writer " + config.target.path, e);
    }
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    if (batch.isEmpty()) {
      return;
    }
    lock.lock();
    try {
      String format = config.target.format == null ? "json" : config.target.format.toLowerCase();
      for (DataRow row : batch) {
        if ("csv".equals(format) || "tsv".equals(format)) {
          writeDelimited(row, format);
        } else {
          writer.write(toJson(row));
          writer.newLine();
        }
      }
      writer.flush();
    } catch (IOException e) {
      throw new SQLException("file write failed", e);
    } finally {
      lock.unlock();
    }
  }

  private void writeDelimited(DataRow row, String format) throws IOException {
    char sep = "tsv".equals(format) ? '\t' : ',';
    if (headerOrder == null) {
      headerOrder = List.copyOf(row.values().keySet());
      writer.write(String.join(String.valueOf(sep), headerOrder));
      writer.newLine();
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < headerOrder.size(); i++) {
      if (i > 0) {
        sb.append(sep);
      }
      Object v = row.get(headerOrder.get(i));
      sb.append(escape(v, sep));
    }
    writer.write(sb.toString());
    writer.newLine();
  }

  private static String escape(Object v, char sep) {
    if (v == null) {
      return "";
    }
    String s = String.valueOf(v);
    if (s.indexOf(sep) >= 0 || s.indexOf('"') >= 0 || s.indexOf('\n') >= 0) {
      return '"' + s.replace("\"", "\"\"") + '"';
    }
    return s;
  }

  private static String toJson(DataRow row) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, Object> e : row.values().entrySet()) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      sb.append('"').append(escapeJson(e.getKey())).append("\":").append(jsonValue(e.getValue()));
    }
    return sb.append('}').toString();
  }

  private static String jsonValue(Object v) {
    if (v == null) {
      return "null";
    }
    if (v instanceof Number || v instanceof Boolean) {
      return String.valueOf(v);
    }
    if (v instanceof Instant instant) {
      return '"' + instant.toString() + '"';
    }
    return '"' + escapeJson(String.valueOf(v)) + '"';
  }

  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
  }

  @Override
  public void close() throws SQLException {
    lock.lock();
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      throw new SQLException(e);
    } finally {
      lock.unlock();
    }
  }

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "file";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new FileWriter(config);
    }
  }
}
