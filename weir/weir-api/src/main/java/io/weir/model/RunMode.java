package io.weir.model;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Runtime mode for one Weir run. */
public enum RunMode {
  FULL,
  INCREMENTAL,
  /** PK set / soft-delete correction pass. */
  DIFF,
  /** Preflight checks only. */
  CHECK;

  @JsonCreator
  public static RunMode from(String value) {
    return RunMode.valueOf(value.trim().toUpperCase());
  }
}
