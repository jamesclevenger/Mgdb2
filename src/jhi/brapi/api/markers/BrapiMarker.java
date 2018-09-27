package jhi.brapi.api.markers;

import java.util.ArrayList;
import java.util.List;

public class BrapiMarker {
    private String markerDbId;
    private String defaultDisplayName;
    private List<String> synonyms = new ArrayList<String>();
    private List<String> refAlt = new ArrayList<String>();
    private String type;
    private List<String> analysisMethods = new ArrayList<String>();

    public String getMarkerDbId() {
        return this.markerDbId;
    }

    public void setMarkerDbId(String markerDbId) {
        this.markerDbId = markerDbId;
    }

    public String getDefaultDisplayName() {
        return this.defaultDisplayName;
    }

    public void setDefaultDisplayName(String defaultDisplayName) {
        this.defaultDisplayName = defaultDisplayName;
    }

    public List<String> getSynonyms() {
        return this.synonyms;
    }

    public void setSynonyms(List<String> synonyms) {
        this.synonyms = synonyms;
    }

    public List<String> getRefAlt() {
        return this.refAlt;
    }

    public void setRefAlt(List<String> refAlt) {
        this.refAlt = refAlt;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getAnalysisMethods() {
        return this.analysisMethods;
    }

    public void setAnalysisMethods(List<String> analysisMethods) {
        this.analysisMethods = analysisMethods;
    }

    public static enum MatchingMethod {
        EXACT,
        WILDCARD;
        

        private MatchingMethod() {
        }

        public static MatchingMethod getValue(String input) {
            if (input == null || input.equals("")) {
                return EXACT;
            }
            try {
                return MatchingMethod.valueOf(input.toUpperCase());
            }
            catch (Exception e) {
                return EXACT;
            }
        }
    }

}

