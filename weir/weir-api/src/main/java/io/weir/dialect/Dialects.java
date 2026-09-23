package io.weir.dialect;

/**
 * Dialect registry. Resolved either from the configured source type or, for sinks, straight from a
 * JDBC URL — a sync may read MySQL and write Postgres, so the two sides need their own dialect.
 */
public final class Dialects {
  private Dialects() {}

  public static JdbcDialect forType(String type) {
    if (type == null || type.isBlank()) {
      return new StandardDialect("standard");
    }
    String t = type.toLowerCase();
    return switch (t) {
      case "mysql", "mariadb" -> new MySqlDialect();
      case "h2" -> new H2Dialect();
      case "postgres", "postgresql", "pg" -> new PostgresDialect();
      case "oracle" -> new OracleDialect();
      case "sqlserver", "mssql", "sql_server" -> new SqlServerDialect();
      case "standard", "generic", "ansi" -> new StandardDialect("standard");
      default -> new StandardDialect(t);
    };
  }

  /** Resolve the dialect from a JDBC URL, e.g. {@code jdbc:postgresql://host/db}. */
  public static JdbcDialect forUrl(String url) {
    if (url == null || url.isBlank()) {
      return new StandardDialect("standard");
    }
    String u = url.toLowerCase();
    if (u.startsWith("jdbc:mysql:") || u.startsWith("jdbc:mariadb:")) {
      return new MySqlDialect();
    }
    if (u.startsWith("jdbc:h2:")) {
      return new H2Dialect();
    }
    if (u.startsWith("jdbc:postgresql:") || u.startsWith("jdbc:pgsql:")) {
      return new PostgresDialect();
    }
    if (u.startsWith("jdbc:oracle:")) {
      return new OracleDialect();
    }
    if (u.startsWith("jdbc:sqlserver:") || u.startsWith("jdbc:jtds:")) {
      return new SqlServerDialect();
    }
    return new StandardDialect("standard");
  }
}
