package jhi.brapi.api.genomemaps;

public class BrapiLinkageGroupMarker {
    private String markerDbId;
    private String markerName;
    private String location;

    public String getMarkerDbId() {
        return this.markerDbId;
    }

    public void setMarkerDbId(String markerDbId) {
        this.markerDbId = markerDbId;
    }

    public String getMarkerName() {
        return this.markerName;
    }

    public void setMarkerName(String markerName) {
        this.markerName = markerName;
    }

    public String getLocation() {
        return this.location;
    }

    public void setLocation(String location) {
        this.location = location;
    }
}

