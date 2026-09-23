package io.weir.model;

import com.fasterxml.jackson.annotation.JsonCreator;

/** How the next batch of rows is selected from JDBC. */
public enum IncrementalStrategy {
  /** Whole table snapshot (or range split on split column). */
  FULL,
  /** Monotonic id only: id > watermark. Does not see updates. */
  ID,
  /** update_time (or version) column. */
  UPDATE_TIME,
  /** update_time primary cursor, id as tie-break. */
  UPDATE_TIME_ID;

  @JsonCreator
  public static IncrementalStrategy from(String value) {
    return IncrementalStrategy.valueOf(value.trim().toUpperCase());
  }
}
