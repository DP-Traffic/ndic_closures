package cz.vutbr.fit.diploma.traffic;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Minimalistický streaming parser pro DATEX II (D2) SituationPublication. Čte pouze podmnožinu
 * elementů potřebných pro Roadworks.
 */
public final class DatexParser {

  public record ParseResult(OffsetDateTime publicationTime, List<Map<String, Object>> items) {}

  public static ParseResult parse(InputStream is) {
    XMLInputFactory f = XMLInputFactory.newFactory();
    OffsetDateTime pubTime = null;
    List<Map<String, Object>> items = new ArrayList<>(256);

    class SituationRecord {
      String id,
          recType,
          validityStatus,
          overallStartTime,
          overallEndTime,
          networkMgmtType,
          roadworksType,
          restrictionType,
          roadNameOrNumber,
          locationDescription;

      Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        if (id != null) {
          m.put("situationRecordId", id);
        }
        if (recType != null) {
          m.put("recordType", recType);
        }
        if (validityStatus != null) {
          m.put("validityStatus", validityStatus);
        }
        if (overallStartTime != null) {
          m.put("overallStartTime", overallStartTime);
        }
        if (overallEndTime != null) {
          m.put("overallEndTime", overallEndTime);
        }
        if (networkMgmtType != null) {
          m.put("networkManagementType", networkMgmtType);
        }
        if (roadworksType != null) {
          m.put("roadworksType", roadworksType);
        }
        if (restrictionType != null) {
          m.put("restrictionType", restrictionType);
        }
        if (roadNameOrNumber != null) {
          m.put("road", roadNameOrNumber);
        }
        if (locationDescription != null) {
          m.put("location", locationDescription);
        }
        return m;
      }
    }
    SituationRecord situationRecord = null;

    try {
      XMLStreamReader r = f.createXMLStreamReader(is);
      while (r.hasNext()) {
        int ev = r.next();
        if (ev == XMLStreamConstants.START_ELEMENT) {
          String name = r.getLocalName();
          switch (name) {
            case "publicationTime" -> {
              if (pubTime == null) {
                try {
                  pubTime = OffsetDateTime.parse(r.getElementText().trim());
                } catch (Exception ignored) {
                }
              }
            }
            case "situationRecord" -> {
              if (situationRecord != null && situationRecord.id != null) {
                items.add(situationRecord.toMap());
              }
              situationRecord = new SituationRecord();
              for (int i = 0; i < r.getAttributeCount(); i++) {
                String an = r.getAttributeLocalName(i);
                String av = r.getAttributeValue(i);
                if ("id".equals(an)) {
                  situationRecord.id = av;
                }
                if ("type".equals(an)) {
                  situationRecord.recType = av; // např. Roadworks
                }
              }
            }
            case "validityStatus" -> situationRecord.validityStatus = r.getElementText().trim();
            case "overallStartTime" -> situationRecord.overallStartTime = r.getElementText().trim();
            case "overallEndTime" -> situationRecord.overallEndTime = r.getElementText().trim();
            case "networkManagementType" ->
                situationRecord.networkMgmtType = r.getElementText().trim();
            case "roadMaintenanceType", "roadworksType" ->
                situationRecord.roadworksType = r.getElementText().trim();
            case "restrictionType" -> situationRecord.restrictionType = r.getElementText().trim();
            case "roadNumber", "roadName" -> {
              String v = r.getElementText().trim();
              if (situationRecord.roadNameOrNumber == null
                  || situationRecord.roadNameOrNumber.isBlank())
                situationRecord.roadNameOrNumber = v;
            }
            case "locationDescriptor", "areaName" -> {
              String v = r.getElementText().trim();
              if (situationRecord.locationDescription == null
                  || situationRecord.locationDescription.isBlank())
                situationRecord.locationDescription = v;
            }
            default -> {}
          }
        } else if (ev == XMLStreamConstants.END_ELEMENT) {
          if ("situationRecord".equals(r.getLocalName())) {
            if (situationRecord != null && situationRecord.id != null) {
              items.add(situationRecord.toMap());
            }
            situationRecord = null;
          }
        }
      }
      if (situationRecord != null && situationRecord.id != null) {
        items.add(situationRecord.toMap());
      }
    } catch (XMLStreamException e) {
      throw new RuntimeException("DATEX II parse error", e);
    }
    return new ParseResult(pubTime, items);
  }

  private DatexParser() {}
}
