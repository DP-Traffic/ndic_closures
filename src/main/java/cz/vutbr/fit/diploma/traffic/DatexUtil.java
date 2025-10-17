package cz.vutbr.fit.diploma.traffic;

import java.util.Map;

public final class DatexUtil {

  public static boolean isFullClosure(Map<String, Object> rec) {
    return containsAny(
            rec.get("roadOrCarriagewayOrLaneManagementType"), "roadClosed", "carriagewayClosed")
        || containsAny(rec.get("networkManagementType"), "roadClosed", "carriagewayClosed");
  }

  public static boolean isLaneClosure(Map<String, Object> rec) {
    return containsAny(rec.get("roadOrCarriagewayOrLaneManagementType"), "laneClosures");
  }

  /** Celková „uzavírka?“ (plná nebo pruhová) */
  public static boolean isAnyClosure(Map<String, Object> rec) {
    return isFullClosure(rec) || isLaneClosure(rec);
  }

  // --- helpers ---
  private static boolean containsAny(Object value, String... needles) {
    if (!(value instanceof String s) || s.isBlank()) return false;
    String low = s.toLowerCase();
    for (String n : needles) {
      if (low.contains(n.toLowerCase())) return true;
    }
    return false;
  }

  private DatexUtil() {}
}
