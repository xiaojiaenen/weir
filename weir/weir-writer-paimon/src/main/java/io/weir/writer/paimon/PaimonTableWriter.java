package io.weir.writer.paimon;

import io.weir.config.JobConfig;
import io.weir.model.DataRow;
import io.weir.spi.RowWriter;
import io.weir.spi.RowWriterFactory;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

/**
 * Paimon FileStoreTable writer (paimon-bundle 1.0.x Java API).
 *
 * <p>Soft-delete {@code _op=d} → {@link RowKind#DELETE}. Requires primary-key table for upsert.
 */
public final class PaimonTableWriter implements RowWriter {
  private final JobConfig config;
  private Catalog catalog;
  private FileStoreTable fst;
  private String commitUser;
  private TableWriteImpl<?> write;
  private TableCommitImpl commit;
  private List<String> fieldNames = List.of();

  public PaimonTableWriter(JobConfig config) {
    this.config = config;
  }

  @Override
  public void open() throws SQLException {
    try {
      Options options = new Options();
      options.set("warehouse", config.target.warehouse);
      if (config.target.options != null) {
        config.target.options.forEach(options::set);
      }
      CatalogContext context = CatalogContext.create(options);
      catalog = CatalogFactory.createCatalog(context);
      String db = config.target.catalogDb == null ? Catalog.DEFAULT_DATABASE : config.target.catalogDb;
      String tableName =
          config.target.paimonTable != null ? config.target.paimonTable : config.target.table;
      Identifier id = Identifier.create(db, tableName);
      Table table = catalog.getTable(id);
      if (!(table instanceof FileStoreTable fstTable)) {
        throw new SQLException("table is not FileStoreTable: " + id);
      }
      this.fst = fstTable;
      this.commitUser = "weir-" + config.name;
      write = fst.newWrite(commitUser);
      commit = fst.newCommit(commitUser);
      RowType schema = fst.rowType();
      fieldNames = new ArrayList<>();
      for (DataField f : schema.getFields()) {
        fieldNames.add(f.name());
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("paimon open failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void writeBatch(List<DataRow> batch) throws SQLException {
    if (batch.isEmpty()) {
      return;
    }
    try {
      for (DataRow row : batch) {
        Object op = row.get("_op");
        boolean del =
            op != null
                && ("d".equalsIgnoreCase(String.valueOf(op))
                    || "delete".equalsIgnoreCase(String.valueOf(op)));
        write.write(toRow(row, del));
      }
      List<CommitMessage> messages = write.prepareCommit();
      commit.commit(messages);
      // BatchTableWrite supports only one prepareCommit — reopen for the next batch.
      write.close();
      commit.close();
      write = fst.newWrite(commitUser);
      commit = fst.newCommit(commitUser);
    } catch (Exception e) {
      throw new SQLException("paimon write failed: " + e.getMessage(), e);
    }
  }

  private GenericRow toRow(DataRow row, boolean delete) {
    Object[] values = new Object[fieldNames.size()];
    for (int i = 0; i < fieldNames.size(); i++) {
      values[i] = convert(row.get(fieldNames.get(i)));
    }
    GenericRow genericRow = GenericRow.of(values);
    genericRow.setRowKind(delete ? RowKind.DELETE : RowKind.INSERT);
    return genericRow;
  }

  private static Object convert(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof String s) {
      return BinaryString.fromString(s);
    }
    if (v instanceof Instant i) {
      return Timestamp.fromInstant(i);
    }
    return v;
  }

  @Override
  public void close() throws SQLException {
    try {
      if (write != null) {
        write.close();
      }
      if (commit != null) {
        commit.close();
      }
      if (catalog != null) {
        catalog.close();
      }
    } catch (Exception e) {
      throw new SQLException("paimon close failed", e);
    }
  }

  public static final class Factory implements RowWriterFactory {
    @Override
    public String type() {
      return "paimon";
    }

    @Override
    public RowWriter create(JobConfig config) {
      return new PaimonTableWriter(config);
    }
  }
}
