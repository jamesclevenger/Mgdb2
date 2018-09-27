package jhi.brapi.api.germplasm;

import java.util.List;

public class BrapiGermplasmMarkerProfiles {
    private String germplasmDbId;
    private List<String> markerProfileDbIds;

    public String getGermplasmDbId() {
        return this.germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

    public List<String> getMarkerProfileDbIds() {
        return this.markerProfileDbIds;
    }

    public void setMarkerProfileDbIds(List<String> markerProfileIds) {
        this.markerProfileDbIds = markerProfileIds;
    }

    public String toString() {
        return "GermplasmMarkerProfileList{germplasmDbId=" + this.germplasmDbId + ", markerProfileIds=" + this.markerProfileDbIds + '}';
    }
}

