package jhi.brapi.api.markerprofiles;

public class BrapiMarkerProfile {
    private String markerprofileDbId;
    private String germplasmDbId;
    private String uniqueDisplayName;
    private String extractDbId;
    private String analysisMethod;
    private int resultCount;
    private String sampleDbId;

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

    public String getUniqueDisplayName() {
        return this.uniqueDisplayName;
    }

    public void setUniqueDisplayName(String uniqueDisplayName) {
        this.uniqueDisplayName = uniqueDisplayName;
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

    public int getResultCount() {
        return this.resultCount;
    }

    public void setResultCount(int resultCount) {
        this.resultCount = resultCount;
    }

    public String getSampleDbId() {
        return this.sampleDbId;
    }

    public void setSampleDbId(String sampleDbId) {
        this.sampleDbId = sampleDbId;
    }

    public String toString() {
        return "MarkerProfile{markerprofileDbId=" + this.markerprofileDbId + ", germplasmDbId=" + this.germplasmDbId + ", extractDbId=" + this.extractDbId + ", analysisMethod='" + this.analysisMethod + '\'' + ", resultCount='" + this.resultCount + '\'' + '}';
    }
}

