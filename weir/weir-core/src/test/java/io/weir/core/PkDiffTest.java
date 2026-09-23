package io.weir.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.weir.core.diff.PkDiff;
import io.weir.model.DataRow;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PkDiffTest {
  private static DataRow row(Object id, Object name) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", id);
    m.put("name", name);
    return new DataRow(m);
  }

  @Test
  void gitStyleSetDiff() {
    var src = List.<Object[]>of(new Object[] {1L}, new Object[] {2L}, new Object[] {3L});
    var tgt = List.<Object[]>of(new Object[] {2L}, new Object[] {4L});
    var r = PkDiff.diffKeys(src, tgt);
    assertEquals(2, r.sourceOnly().size()); // 1,3
    assertEquals(1, r.targetOnly().size()); // 4
  }

  @Test
  void detectsContentChange() {
    DataRow s = row(1L, "a2");
    DataRow t = row(1L, "a");
    assertTrue(PkDiff.rowDiffers(s, t, List.of("id"), List.of("id", "name")));
  }
}
