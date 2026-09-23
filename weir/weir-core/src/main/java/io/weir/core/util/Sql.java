package io.weir.core.util;

import io.weir.config.JobConfig;
import io.weir.dialect.JdbcDialect;
import java.util.ArrayList;
import java.util.List;

/** SQL fragment helpers. Predicates are assembled from already-quoted identifiers and literals. */
public final class Sql {
  private Sql() {}

  /** Join non-blank predicates with AND, each parenthesised so precedence cannot bite. */
  public static String and(List<String> parts) {
    List<String> nonEmpty = new ArrayList<>();
    for (String p : parts) {
      if (p != null && !p.isBlank()) {
        nonEmpty.add("(" + p + ")");
      }
    }
    return String.join(" AND ", nonEmpty);
  }

  /** Null-safe list builder (List.of rejects nulls). */
  public static String listOrDefault(String... parts) {
    return and(list(parts));
  }

  public static List<String> list(String... parts) {
    List<String> out = new ArrayList<>();
    for (String p : parts) {
      if (p != null) {
        out.add(p);
      }
    }
    return out;
  }

  /** Table name, or the wrapped subquery alias in query mode. */
  public static String fromClause(JobConfig.SourceConfig.ReadConfig read) {
    if ("query".equalsIgnoreCase(read.mode)) {
      return "(" + read.query + ") weir_src";
    }
    return read.table;
  }

  public static List<String> quoteAll(JdbcDialect dialect, List<String> columns) {
    List<String> out = new ArrayList<>(columns.size());
    for (String c : columns) {
      out.add(dialect.quote(c));
    }
    return out;
  }
}
