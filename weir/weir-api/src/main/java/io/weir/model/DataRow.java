package io.weir.model;

/** One physical row as column name → value (JDBC-compatible objects or String). */
public record DataRow(java.util.Map<String, Object> values) {
  public DataRow {
    values = java.util.Map.copyOf(values);
  }

  public Object get(String column) {
    if (values.containsKey(column)) {
      return values.get(column);
    }
    for (var e : values.entrySet()) {
      if (e.getKey() != null && e.getKey().equalsIgnoreCase(column)) {
        return e.getValue();
      }
    }
    return null;
  }
}
