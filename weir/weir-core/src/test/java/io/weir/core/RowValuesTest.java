package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.model.DataRow;
import io.weir.model.RowValues;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Canonical value form — the basis of "did this row actually change?". */
class RowValuesTest {

  private static DataRow row(Object... kv) {
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put((String) kv[i], kv[i + 1]);
    }
    return new DataRow(m);
  }

  @Test
  void sameMomentInDifferentTypesIsEqual() {
    Instant i = Instant.parse("2024-06-01T00:00:00Z");
    assertTrue(RowValues.equal(i, Timestamp.from(i)), "Instant vs java.sql.Timestamp");
    assertTrue(RowValues.equal(i, i.toString()), "Instant vs ISO-8601 string");
    assertTrue(RowValues.equal(i, Timestamp.from(i).toString()), "Instant vs JDBC timestamp string");
    assertTrue(RowValues.equal(LocalDate.of(2024, 6, 1), java.sql.Date.valueOf("2024-06-01")));
  }

  @Test
  void numericWidthsDoNotSpuriouslyDiffer() {
    assertTrue(RowValues.equal(1, 1L), "int vs long");
    assertTrue(RowValues.equal(1.0, 1), "double vs int");
    assertTrue(RowValues.equal(new BigDecimal("1.00"), 1), "BigDecimal vs int");
    assertTrue(RowValues.equal(new BigDecimal("1.5000"), 1.5), "trailing zeros are irrelevant");
    assertFalse(RowValues.equal(1, 2), "different values still differ");
  }

  @Test
  void nullsAndBlanks() {
    assertTrue(RowValues.equal(null, null));
    assertFalse(RowValues.equal(null, "x"));
    assertFalse(RowValues.equal("x", null));
    assertFalse(RowValues.equal("", " "), "empty string is not a space");
  }

  @Test
  void hashesAreStableAndOrderSensitive() {
    DataRow a = row("id", 1L, "name", "a");
    DataRow b = row("id", 1L, "name", "a");
    DataRow c = row("id", 1L, "name", "b");
    assertEquals(RowValues.hash(a, List.of("id", "name")), RowValues.hash(b, List.of("id", "name")));
    assertNotEquals(
        RowValues.hash(a, List.of("id", "name")), RowValues.hash(c, List.of("id", "name")));
    // Projecting fewer columns yields a different (but deterministic) hash.
    assertEquals(RowValues.hash(a, List.of("name")), RowValues.hash(b, List.of("name")));
  }

  @Test
  void hashesIgnoreRepresentationButNotValue() {
    DataRow withInstant = row("id", 1L, "ts", Instant.parse("2024-06-01T00:00:00Z"));
    DataRow withTimestamp = row("id", 1L, "ts", Timestamp.from(Instant.parse("2024-06-01T00:00:00Z")));
    assertEquals(
        RowValues.hash(withInstant, List.of("id", "ts")),
        RowValues.hash(withTimestamp, List.of("id", "ts")),
        "the same moment hashes the same regardless of Java type");
  }
}
