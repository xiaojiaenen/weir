package io.weir.model;

/** Half-open range on a numeric split column: [lo, hi). lo/hi may be null for unbounded. */
public record SplitRange(Long lo, Long hi) {
  public boolean contains(long value) {
    if (lo != null && value < lo) {
      return false;
    }
    return hi == null || value < hi;
  }
}
