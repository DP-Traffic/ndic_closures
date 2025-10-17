package cz.vutbr.fit.diploma.traffic;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.*;
import javax.xml.stream.*;

/**
 * Parser pro DATEX II (D2) SituationPublication. Pokrývá vybrané elementy (typy událostí, validity,
 * komentáře) a metody lokalizace: - Alert-C (linear/point), - Global Network (globalNetworkLinear),
 * - adresnou metodu (linearWithinLinearElement), - textové labely (road,
 * areaName/locationDescriptor).
 *
 * <p>Pozn.: Lookup Alert-C tabulek, převody S-JTSK→WGS-84 a linear-referencing řeš mimo parser (na
 * výstupních datech).
 */
public final class DatexParser {

  /** Výsledek: čas publikace + seznam záznamů jako mapy. */
  public record ParseResult(OffsetDateTime publicationTime, List<Map<String, Object>> items) {}

  public static ParseResult parse(InputStream is) {
    XMLInputFactory f = XMLInputFactory.newFactory();
    f.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, true);

    OffsetDateTime pubTime = null;
    List<Map<String, Object>> items = new ArrayList<>(256);

    // --- interní pomocné třídy ---------------------------------------------

    final class AlertCLinear {
      String country, tableNumber, tableVersion, directionCoded;
      String primarySpecificLocation, secondarySpecificLocation;

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "countryCode", country);
        put(m, "tableNumber", tableNumber);
        put(m, "tableVersion", tableVersion);
        put(m, "directionCoded", directionCoded);
        put(m, "primarySpecificLocation", primarySpecificLocation);
        put(m, "secondarySpecificLocation", secondarySpecificLocation);
        return m;
      }
    }

    final class AlertCPoint {
      String country, tableNumber, tableVersion, directionCoded;
      String primarySpecificLocation;

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "countryCode", country);
        put(m, "tableNumber", tableNumber);
        put(m, "tableVersion", tableVersion);
        put(m, "directionCoded", directionCoded);
        put(m, "primarySpecificLocation", primarySpecificLocation);
        return m;
      }
    }

    final class GNElement {
      String sectionId, direction, order, fromPercent, toPercent;

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "sectionId", sectionId);
        put(m, "direction", direction);
        put(m, "order", order);
        put(m, "fromPercent", fromPercent);
        put(m, "toPercent", toPercent);
        return m;
      }
    }

    final class GNLinear {
      String networkVersion, linearGeometryType;
      String startX, startY, endX, endY;
      List<GNElement> segments = new ArrayList<>();

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "networkVersion", networkVersion);
        put(m, "linearGeometryType", linearGeometryType);
        put(m, "startSjtskX", startX);
        put(m, "startSjtskY", startY);
        put(m, "endSjtskX", endX);
        put(m, "endSjtskY", endY);
        if (!segments.isEmpty()) {
          List<Map<String, Object>> segs = new ArrayList<>(segments.size());
          for (GNElement s : segments) segs.add(s.toMap());
          m.put("segments", segs);
        }
        return m;
      }
    }

    final class LinearWithin {
      String roadNumber, roadName, direction;
      String fromDistanceAlong, toDistanceAlong;

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        put(m, "roadNumber", roadNumber);
        put(m, "roadName", roadName);
        put(m, "direction", direction);
        put(m, "fromDistanceAlong", fromDistanceAlong);
        put(m, "toDistanceAlong", toDistanceAlong);
        return m;
      }
    }

    final class SituationRecord {
      // situation header context
      String situationId, situationVersion, situationVersionTime, informationStatus, urgency;

      // record-level attrs
      String id,
          recType /*xsi:type*/,
          validityStatus,
          overallStartTime,
          overallEndTime,
          probabilityOfOccurrence,
          comment;

      // management / restrictions / types
      String networkMgmtType,
          roadworksType,
          restrictionType,
          roadOrCarriagewayOrLaneMgmtType,
          trafficControlType,
          speedLimit;
      String accidentType, vehicleObstructionType, authorityOperationType;

      // simple text location
      String roadNameOrNumber, locationDescription, areaName;

      // nested location methods
      List<AlertCLinear> alertCLinears = new ArrayList<>();
      List<AlertCPoint> alertCPoints = new ArrayList<>();
      List<GNLinear> gnLinears = new ArrayList<>();
      List<LinearWithin> linearsWithin = new ArrayList<>();

      boolean isClosure() {
        String s =
            (roadOrCarriagewayOrLaneMgmtType != null)
                ? roadOrCarriagewayOrLaneMgmtType
                : ((networkMgmtType != null) ? networkMgmtType : "");
        return s.toLowerCase(Locale.ROOT).contains("closed");
      }

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();

        // situation context
        put(m, "situationId", situationId);
        put(m, "situationVersion", situationVersion);
        put(m, "situationVersionTime", situationVersionTime);
        put(m, "informationStatus", informationStatus);
        put(m, "urgency", urgency);

        // record basics
        put(m, "situationRecordId", id);
        put(m, "xsiType", recType);
        put(m, "validityStatus", validityStatus);
        put(m, "overallStartTime", overallStartTime);
        put(m, "overallEndTime", overallEndTime);
        put(m, "probabilityOfOccurrence", probabilityOfOccurrence);
        put(m, "comment", comment);

        // types
        put(m, "networkManagementType", networkMgmtType);
        put(m, "roadworksType", roadworksType);
        put(m, "restrictionType", restrictionType);
        put(m, "roadOrCarriagewayOrLaneManagementType", roadOrCarriagewayOrLaneMgmtType);
        put(m, "trafficControlType", trafficControlType);
        put(m, "speedLimit", speedLimit);
        put(m, "accidentType", accidentType);
        put(m, "vehicleObstructionType", vehicleObstructionType);
        put(m, "authorityOperationType", authorityOperationType);

        // simple location labels
        put(m, "road", roadNameOrNumber);
        put(
            m,
            "locationText",
            (areaName != null && !areaName.isBlank()) ? areaName : locationDescription);

        // structured locations
        if (!alertCLinears.isEmpty()) {
          List<Map<String, Object>> arr = new ArrayList<>(alertCLinears.size());
          for (AlertCLinear a : alertCLinears) arr.add(a.toMap());
          m.put("alertCLinear", arr);
        }
        if (!alertCPoints.isEmpty()) {
          List<Map<String, Object>> arr = new ArrayList<>(alertCPoints.size());
          for (AlertCPoint a : alertCPoints) arr.add(a.toMap());
          m.put("alertCPoint", arr);
        }
        if (!gnLinears.isEmpty()) {
          List<Map<String, Object>> arr = new ArrayList<>(gnLinears.size());
          for (GNLinear g : gnLinears) arr.add(g.toMap());
          m.put("globalNetworkLinear", arr);
        }
        if (!linearsWithin.isEmpty()) {
          List<Map<String, Object>> arr = new ArrayList<>(linearsWithin.size());
          for (LinearWithin l : linearsWithin) arr.add(l.toMap());
          m.put("linearWithinLinearElement", arr);
        }

        m.put("isClosure", isClosure());
        return m;
      }
    }

    // --- parsovací stav -----------------------------------------------------

    SituationRecord sr = null;

    // situation context
    String currentSituationId = null,
        currentSituationVersion = null,
        currentSituationVersionTime = null;
    String currentInformationStatus = null, currentUrgency = null;
    boolean inHeaderInformation = false;

    // komentáře a wrapper value-sekce
    boolean inGeneralPublicComment = false;
    boolean inGNNetworkVersion = false;
    boolean inAreaName = false;

    // Alert-C (stav)
    AlertCLinear currentAlertCLinear = null;
    boolean inACPrimary = false, inACSecondary = false;
    AlertCPoint currentAlertCPoint = null;

    // GN (stav)
    GNLinear currentGN = null;
    GNElement currentGNEl = null;
    boolean inGNStartPoint = false, inGNEndPoint = false;
    boolean inGNFromPoint = false, inGNToPoint = false;

    // LinearWithin (adresná metoda)
    LinearWithin currentLW = null;
    boolean inLWFromPoint = false, inLWToPoint = false;

    // --- vlastní streaming --------------------------------------------------

    try {
      XMLStreamReader r = f.createXMLStreamReader(is);
      while (r.hasNext()) {
        int ev = r.next();

        if (ev == XMLStreamConstants.START_ELEMENT) {
          String name = r.getLocalName();

          switch (name) {
            // --- publication header ---
            case "publicationTime" -> {
              if (pubTime == null) {
                try {
                  pubTime = OffsetDateTime.parse(r.getElementText().trim());
                } catch (Exception ignored) {
                }
              }
            }

            // --- situation-level context ---
            case "situation" -> {
              currentSituationId = attr(r, null, "id");
              currentSituationVersion = attr(r, null, "version");
            }
            case "situationVersionTime" -> currentSituationVersionTime = r.getElementText().trim();
            case "headerInformation" -> inHeaderInformation = true;
            case "informationStatus" -> {
              String v = r.getElementText().trim();
              if (inHeaderInformation) currentInformationStatus = v;
            }
            case "urgency" -> {
              String v = r.getElementText().trim();
              if (inHeaderInformation) currentUrgency = v;
            }

            // --- situationRecord lifecycle ---
            case "situationRecord" -> {
              if (sr != null && sr.id != null) items.add(sr.toMap());
              sr = new SituationRecord();
              sr.id = attr(r, null, "id");
              sr.recType = attr(r, "http://www.w3.org/2001/XMLSchema-instance", "type"); // xsi:type
              sr.situationId = currentSituationId;
              sr.situationVersion = currentSituationVersion;
              sr.situationVersionTime = currentSituationVersionTime;
              sr.informationStatus = currentInformationStatus;
              sr.urgency = currentUrgency;
            }

            // --- validity & timing ---
            case "validityStatus" -> sr.validityStatus = r.getElementText().trim();
            case "overallStartTime" -> sr.overallStartTime = r.getElementText().trim();
            case "overallEndTime" -> sr.overallEndTime = r.getElementText().trim();
            case "probabilityOfOccurrence" ->
                sr.probabilityOfOccurrence = r.getElementText().trim();

            // --- comments ---
            case "generalPublicComment" -> inGeneralPublicComment = true;
            case "value" -> {
              // Univerzální handler pro <values><value>... – zapisuje kontextově:
              String v = r.getElementText().trim();
              if (v.isEmpty()) break;

              if (inGNNetworkVersion && currentGN != null && currentGN.networkVersion == null) {
                currentGN.networkVersion = v;
              } else if (inAreaName
                  && sr != null
                  && (sr.areaName == null || sr.areaName.isBlank())) {
                sr.areaName = v;
              } else if (inGeneralPublicComment && sr != null && sr.comment == null) {
                sr.comment = v;
              }
              // další <value> (např. sourceName) ignorujeme
            }

            // --- types / management ---
            case "networkManagementType" -> sr.networkMgmtType = r.getElementText().trim();
            case "roadMaintenanceType", "roadworksType" ->
                sr.roadworksType = r.getElementText().trim();
            case "restrictionType" -> sr.restrictionType = r.getElementText().trim();
            case "roadOrCarriagewayOrLaneManagementType" ->
                sr.roadOrCarriagewayOrLaneMgmtType = r.getElementText().trim();
            case "trafficControlType" -> sr.trafficControlType = r.getElementText().trim();
            case "speedLimit" -> sr.speedLimit = r.getElementText().trim();

            // more typed
            case "accidentType" -> sr.accidentType = r.getElementText().trim();
            case "vehicleObstructionType" -> sr.vehicleObstructionType = r.getElementText().trim();
            case "authorityOperationType" -> sr.authorityOperationType = r.getElementText().trim();

            // --- simple location labels (globální i uvnitř LinearWithin) ---
            case "roadNumber" -> {
              String v = r.getElementText().trim();
              if (currentLW != null) {
                if (currentLW.roadNumber == null || currentLW.roadNumber.isBlank())
                  currentLW.roadNumber = v;
              } else if (sr != null) {
                if (sr.roadNameOrNumber == null || sr.roadNameOrNumber.isBlank())
                  sr.roadNameOrNumber = v;
              }
            }
            case "roadName" -> {
              String v = r.getElementText().trim();
              if (currentLW != null) {
                if (currentLW.roadName == null || currentLW.roadName.isBlank())
                  currentLW.roadName = v;
              } else if (sr != null) {
                if (sr.roadNameOrNumber == null || sr.roadNameOrNumber.isBlank())
                  sr.roadNameOrNumber = v;
              }
            }
            case "locationDescriptor" -> {
              // u NDIC bývá plain text, ale kdyby někdy přišlo přes <values>, rozšíříme podobně
              // jako areaName
              String v = r.getElementText().trim();
              if (sr.locationDescription == null || sr.locationDescription.isBlank())
                sr.locationDescription = v;
            }
            case "areaName" -> {
              // wrapper, skutečný text přijde v <value>
              inAreaName = true;
            }

            // --- Alert-C Linear ---
            case "alertCLinear" -> {
              currentAlertCLinear = new AlertCLinear();
            }
            case "alertCLocationCountryCode" -> {
              String v = r.getElementText().trim();
              if (currentAlertCLinear != null) currentAlertCLinear.country = v;
              if (currentAlertCPoint != null) currentAlertCPoint.country = v;
            }
            case "alertCLocationTableNumber" -> {
              String v = r.getElementText().trim();
              if (currentAlertCLinear != null) currentAlertCLinear.tableNumber = v;
              if (currentAlertCPoint != null) currentAlertCPoint.tableNumber = v;
            }
            case "alertCLocationTableVersion" -> {
              String v = r.getElementText().trim();
              if (currentAlertCLinear != null) currentAlertCLinear.tableVersion = v;
              if (currentAlertCPoint != null) currentAlertCPoint.tableVersion = v;
            }
            case "alertCDirectionCoded" -> {
              String v = r.getElementText().trim();
              if (currentAlertCLinear != null) currentAlertCLinear.directionCoded = v;
              if (currentAlertCPoint != null) currentAlertCPoint.directionCoded = v;
            }
            case "alertCMethod2PrimaryPointLocation" -> inACPrimary = true;
            case "alertCMethod2SecondaryPointLocation" -> inACSecondary = true;
            case "specificLocation" -> {
              String v = r.getElementText().trim();
              if (currentAlertCLinear != null) {
                if (inACPrimary && currentAlertCLinear.primarySpecificLocation == null)
                  currentAlertCLinear.primarySpecificLocation = v;
                else if (inACSecondary && currentAlertCLinear.secondarySpecificLocation == null)
                  currentAlertCLinear.secondarySpecificLocation = v;
              } else if (currentAlertCPoint != null) {
                if (currentAlertCPoint.primarySpecificLocation == null)
                  currentAlertCPoint.primarySpecificLocation = v;
              }
            }

            // --- Alert-C Point ---
            case "alertCPoint" -> {
              currentAlertCPoint = new AlertCPoint();
            }

            // --- Global Network Linear ---
            case "globalNetworkLinear" -> currentGN = new GNLinear();
            case "networkVersion" -> inGNNetworkVersion = true; // text přijde až v <value>
            case "linearGeometryType" -> {
              String v = r.getElementText().trim();
              if (currentGN != null) currentGN.linearGeometryType = v;
            }
            case "startPoint" -> inGNStartPoint = true;
            case "endPoint" -> inGNEndPoint = true;
            case "sjtskX" -> {
              String v = r.getElementText().trim();
              if (currentGN != null) {
                if (inGNStartPoint && currentGN.startX == null) currentGN.startX = v;
                else if (inGNEndPoint && currentGN.endX == null) currentGN.endX = v;
              }
            }
            case "sjtskY" -> {
              String v = r.getElementText().trim();
              if (currentGN != null) {
                if (inGNStartPoint && currentGN.startY == null) currentGN.startY = v;
                else if (inGNEndPoint && currentGN.endY == null) currentGN.endY = v;
              }
            }
            case "linearWithinLinearGNElement" -> currentGNEl = new GNElement();
            case "sectionId" -> {
              String v = r.getElementText().trim();
              if (currentGNEl != null) currentGNEl.sectionId = v;
            }
            case "directionRelativeOnLinearSection" -> {
              String v = r.getElementText().trim();
              if (currentGNEl != null) currentGNEl.direction = v;
              if (currentLW != null && currentLW.direction == null)
                currentLW.direction = v; // sdílený název
            }
            case "orderOfSection" -> {
              String v = r.getElementText().trim();
              if (currentGNEl != null) currentGNEl.order = v;
            }
            case "fromPoint" -> {
              inGNFromPoint = (currentGNEl != null);
              inLWFromPoint = (currentLW != null);
            }
            case "toPoint" -> {
              inGNToPoint = (currentGNEl != null);
              inLWToPoint = (currentLW != null);
            }
            case "percentageDistanceAlong" -> {
              String v = r.getElementText().trim();
              if (currentGNEl != null) {
                if (inGNFromPoint && currentGNEl.fromPercent == null) currentGNEl.fromPercent = v;
                else if (inGNToPoint && currentGNEl.toPercent == null) currentGNEl.toPercent = v;
              }
            }
            case "distanceAlong" -> {
              String v = r.getElementText().trim();
              if (currentLW != null) {
                if (inLWFromPoint && currentLW.fromDistanceAlong == null)
                  currentLW.fromDistanceAlong = v;
                else if (inLWToPoint && currentLW.toDistanceAlong == null)
                  currentLW.toDistanceAlong = v;
              }
            }

            // --- LinearWithinLinearElement (adresná metoda) ---
            case "linearWithinLinearElement" -> currentLW = new LinearWithin();
            case "linearElement" -> {
              /* marker */
            }

            default -> {}
          }

        } else if (ev == XMLStreamConstants.END_ELEMENT) {
          String name = r.getLocalName();
          switch (name) {
            case "situationRecord" -> {
              if (sr != null && sr.id != null) items.add(sr.toMap());
              sr = null;

              // cleanup lokálních stavů
              currentAlertCLinear = null;
              inACPrimary = inACSecondary = false;
              currentAlertCPoint = null;
              currentGN = null;
              currentGNEl = null;
              currentLW = null;
              inGNStartPoint = inGNEndPoint = inGNFromPoint = inGNToPoint = false;
              inLWFromPoint = inLWToPoint = false;
              inGeneralPublicComment = false;
              inGNNetworkVersion = false;
              inAreaName = false;
            }
            case "headerInformation" -> inHeaderInformation = false;
            case "situation" -> {
              currentSituationId = null;
              currentSituationVersion = null;
              currentSituationVersionTime = null;
              currentInformationStatus = null;
              currentUrgency = null;
            }

            // komentáře & wrappery
            case "generalPublicComment" -> inGeneralPublicComment = false;
            case "networkVersion" -> inGNNetworkVersion = false;
            case "areaName" -> inAreaName = false;

            // Alert-C ends
            case "alertCMethod2PrimaryPointLocation" -> inACPrimary = false;
            case "alertCMethod2SecondaryPointLocation" -> inACSecondary = false;
            case "alertCLinear" -> {
              if (sr != null && currentAlertCLinear != null)
                sr.alertCLinears.add(currentAlertCLinear);
              currentAlertCLinear = null;
              inACPrimary = inACSecondary = false;
            }
            case "alertCPoint" -> {
              if (sr != null && currentAlertCPoint != null) sr.alertCPoints.add(currentAlertCPoint);
              currentAlertCPoint = null;
            }

            // GN ends
            case "startPoint" -> inGNStartPoint = false;
            case "endPoint" -> inGNEndPoint = false;
            case "fromPoint" -> {
              inGNFromPoint = false;
              inLWFromPoint = false;
            }
            case "toPoint" -> {
              inGNToPoint = false;
              inLWToPoint = false;
            }
            case "linearWithinLinearGNElement" -> {
              if (currentGN != null && currentGNEl != null) currentGN.segments.add(currentGNEl);
              currentGNEl = null;
            }
            case "globalNetworkLinear" -> {
              if (sr != null && currentGN != null) sr.gnLinears.add(currentGN);
              currentGN = null;
            }

            // LinearWithin ends
            case "linearWithinLinearElement" -> {
              if (sr != null && currentLW != null) sr.linearsWithin.add(currentLW);
              currentLW = null;
              inLWFromPoint = inLWToPoint = false;
            }

            default -> {}
          }
        }
      }

      if (sr != null && sr.id != null) items.add(sr.toMap());
    } catch (XMLStreamException e) {
      throw new RuntimeException("DATEX II parse error", e);
    }

    return new ParseResult(pubTime, items);
  }

  // --- util -----------------------------------------------------------------

  private static void put(Map<String, Object> m, String k, String v) {
    if (v != null && !v.isBlank()) m.put(k, v);
  }

  private static String attr(XMLStreamReader r, String ns, String local) {
    try {
      String v = (ns == null) ? r.getAttributeValue(null, local) : r.getAttributeValue(ns, local);
      if (v == null) {
        for (int i = 0; i < r.getAttributeCount(); i++) {
          if (local.equals(r.getAttributeLocalName(i))) return r.getAttributeValue(i);
        }
      }
      return v;
    } catch (Exception ignore) {
      return null;
    }
  }

  private DatexParser() {}
}
