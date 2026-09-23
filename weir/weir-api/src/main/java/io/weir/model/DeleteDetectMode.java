package io.weir.model;

import com.fasterxml.jackson.annotation.JsonCreator;

/** How deletes are discovered without CDC. */
public enum DeleteDetectMode {
  NONE,
  /** Soft-delete / deleted_at column. */
  SOFT_COLUMN,
  /** Primary-key set diff (git-like). */
  PK_DIFF,
  /** Partition fingerprint then drill down. */
  FINGERPRINT;

  @JsonCreator
  public static DeleteDetectMode from(String value) {
    return DeleteDetectMode.valueOf(value.trim().toUpperCase());
  }
}
