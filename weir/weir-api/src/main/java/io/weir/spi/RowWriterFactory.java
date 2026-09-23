package io.weir.spi;

/** Factory for {@link RowWriter} selected by target.type. */
public interface RowWriterFactory {
  String type();

  RowWriter create(io.weir.config.JobConfig config);
}
