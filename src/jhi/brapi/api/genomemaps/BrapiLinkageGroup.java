package jhi.brapi.api.genomemaps;

public class BrapiLinkageGroup {
    private String linkageGroupId;
    private int markerCount;
    private double maxPosition;

    public String getLinkageGroupId() {
        return this.linkageGroupId;
    }

    public void setLinkageGroupId(String linkageGroupId) {
        this.linkageGroupId = linkageGroupId;
    }

    public int getMarkerCount() {
        return this.markerCount;
    }

    public void setMarkerCount(int markerCount) {
        this.markerCount = markerCount;
    }

    public double getMaxPosition() {
        return this.maxPosition;
    }

    public void setMaxPosition(double maxPosition) {
        this.maxPosition = maxPosition;
    }
}

