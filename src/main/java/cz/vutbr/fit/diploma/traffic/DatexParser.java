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
 * Minimalistický streaming parser pro DATEX II (D2) SituationPublication.
 * Čte pouze podmnožinu elementů potřebných pro Roadworks.
 */
public final class DatexParser {

    public record ParseResult(OffsetDateTime publicationTime, List<Map<String, Object>> items) {}

    public static ParseResult parse(InputStream is) {
        XMLInputFactory f = XMLInputFactory.newFactory();
        OffsetDateTime pubTime = null;
        List<Map<String, Object>> items = new ArrayList<>(256);

        class SR {
            String id, recType, validityStatus, overallStartTime, overallEndTime,
                networkMgmtType, roadworksType, restrictionType,
                roadNameOrNumber, locationDescription;

            Map<String,Object> toMap() {
                Map<String,Object> m = new LinkedHashMap<>();
                if (id != null) m.put("situationRecordId", id);
                if (recType != null) m.put("recordType", recType);
                if (validityStatus != null) m.put("validityStatus", validityStatus);
                if (overallStartTime != null) m.put("overallStartTime", overallStartTime);
                if (overallEndTime != null) m.put("overallEndTime", overallEndTime);
                if (networkMgmtType != null) m.put("networkManagementType", networkMgmtType);
                if (roadworksType != null) m.put("roadworksType", roadworksType);
                if (restrictionType != null) m.put("restrictionType", restrictionType);
                if (roadNameOrNumber != null) m.put("road", roadNameOrNumber);
                if (locationDescription != null) m.put("location", locationDescription);
                return m;
            }
        }
        SR sr = null;

        try {
            XMLStreamReader r = f.createXMLStreamReader(is);
            while (r.hasNext()) {
                int ev = r.next();
                if (ev == XMLStreamConstants.START_ELEMENT) {
                    String name = r.getLocalName();
                    switch (name) {
                    case "publicationTime" -> {
                        if (pubTime == null) {
                            try { pubTime = OffsetDateTime.parse(r.getElementText().trim()); }
                            catch (Exception ignored) {}
                        }
                    }
                    case "situationRecord" -> {
                        if (sr != null && sr.id != null) items.add(sr.toMap());
                        sr = new SR();
                        for (int i = 0; i < r.getAttributeCount(); i++) {
                            String an = r.getAttributeLocalName(i);
                            String av = r.getAttributeValue(i);
                            if ("id".equals(an))   sr.id = av;
                            if ("type".equals(an)) sr.recType = av; // např. Roadworks
                        }
                    }
                    case "validityStatus" -> sr.validityStatus = r.getElementText().trim();
                    case "overallStartTime" -> sr.overallStartTime = r.getElementText().trim();
                    case "overallEndTime" -> sr.overallEndTime = r.getElementText().trim();
                    case "networkManagementType" -> sr.networkMgmtType = r.getElementText().trim();
                    case "roadMaintenanceType", "roadworksType" -> sr.roadworksType = r.getElementText().trim();
                    case "restrictionType" -> sr.restrictionType = r.getElementText().trim();
                    case "roadNumber", "roadName" -> {
                        String v = r.getElementText().trim();
                        if (sr.roadNameOrNumber == null || sr.roadNameOrNumber.isBlank()) sr.roadNameOrNumber = v;
                    }
                    case "locationDescriptor", "areaName" -> {
                        String v = r.getElementText().trim();
                        if (sr.locationDescription == null || sr.locationDescription.isBlank()) sr.locationDescription = v;
                    }
                    default -> {}
                    }
                } else if (ev == XMLStreamConstants.END_ELEMENT) {
                    if ("situationRecord".equals(r.getLocalName())) {
                        if (sr != null && sr.id != null) items.add(sr.toMap());
                        sr = null;
                    }
                }
            }
            if (sr != null && sr.id != null) items.add(sr.toMap());
        } catch (XMLStreamException e) {
            throw new RuntimeException("DATEX II parse error", e);
        }
        return new ParseResult(pubTime, items);
    }

    private DatexParser() {}
}
