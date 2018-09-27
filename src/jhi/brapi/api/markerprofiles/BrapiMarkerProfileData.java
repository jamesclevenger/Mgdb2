package jhi.brapi.api.markerprofiles;

import java.util.List;
import java.util.Map;

public class BrapiMarkerProfileData {
    private String markerprofileDbId;
    private String germplasmDbId;
    private String extractDbId;
    private String analysisMethod;
    private List<Map<String, String>> data;

    public String getMarkerprofileDbId() {
        return this.markerprofileDbId;
    }

    public void setMarkerprofileDbId(String markerprofileDbId) {
        this.markerprofileDbId = markerprofileDbId;
    }

    public String getGermplasmDbId() {
        return this.germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

    public String getExtractDbId() {
        return this.extractDbId;
    }

    public void setExtractDbId(String extractDbId) {
        this.extractDbId = extractDbId;
    }

    public String getAnalysisMethod() {
        return this.analysisMethod;
    }

    public void setAnalysisMethod(String analysisMethod) {
        this.analysisMethod = analysisMethod;
    }

    public List<Map<String, String>> getData() {
        return this.data;
    }

    public void setData(List<Map<String, String>> data) {
        this.data = data;
    }
}

