package io.weir.writer.console;

import io.weir.config.JobConfig;
import io.weir.model.DataRow;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;

/** Debug / demo sink: prints rows (or just counts when quiet). */
public final class ConsoleWriter implements RowWriter {
  private final JobConfig config;
  private final PrintStream out;
  private long count;

  public ConsoleWriter(JobConfig config) {
    this(config, System.out);
  }

  public ConsoleWriter(JobConfig config, PrintStream out) {
    this.config = config;
    this.out = out;
  }

  @Override
  public void open() {
    out.println("[weir-console] open job=" + config.name);
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    for (DataRow row : batch) {
      count++;
      out.println("[weir-console] " + row.values());
    }
    out.println("[weir-console] batch=" + batch.size() + " total=" + count);
  }

  @Override
  public void close() {
    out.println("[weir-console] close total=" + count);
  }

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "console";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new ConsoleWriter(config);
    }
  }
}
