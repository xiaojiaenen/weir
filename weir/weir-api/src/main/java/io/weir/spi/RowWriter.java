package io.weir.spi;

import io.weir.model.DataRow;
import java.sql.SQLException;
import java.util.List;

/**
 * Sink that is idempotent under retries when primary keys are provided (merge), or append-only when
 * the target has no key.
 *
 * <p>Implementations must tolerate the same batch being written twice — the runner replays a batch
 * whenever a run failed after a partial write.
 */
public interface RowWriter extends AutoCloseable {
  void open() throws SQLException;

  /** Write one batch. Implementations must be safe to retry the same batch. */
  void writeBatch(List<DataRow> batch) throws SQLException;

  @Override
  void close() throws SQLException;
}
