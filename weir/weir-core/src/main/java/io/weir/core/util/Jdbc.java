package io.weir.core.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** Single-connection opener (pooled access goes through {@link JdbcPool}). */
public final class Jdbc {
  private Jdbc() {}

  public static Connection open(String url, String user, String password) throws SQLException {
    Properties props = new Properties();
    if (user != null) {
      props.setProperty("user", user);
    }
    if (password != null) {
      props.setProperty("password", password);
    }
    applyVendorDefaults(url, props);
    return DriverManager.getConnection(url, props);
  }

  private static void applyVendorDefaults(String url, Properties props) {
    if (url == null) {
      return;
    }
    String u = url.toLowerCase();
    if (u.startsWith("jdbc:mysql:") || u.startsWith("jdbc:mariadb:")) {
      // Cursor fetch streams instead of buffering the whole result set in the driver.
      props.setProperty("useCursorFetch", "true");
      props.setProperty("rewriteBatchedStatements", "true");
    } else if (u.startsWith("jdbc:postgresql:")) {
      // Without this, PG buffers the entire ResultSet client-side.
      props.setProperty("defaultRowFetchSize", "5000");
    } else if (u.startsWith("jdbc:oracle:")) {
      props.setProperty("defaultRowPrefetch", "5000");
    }
  }
}
