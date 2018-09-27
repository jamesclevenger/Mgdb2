package jhi.brapi.api.genomemaps;

import java.sql.Date;

public class BrapiGenomeMap {
    private String mapDbId;
    private String name;
    private String species;
    private String type;
    private String unit;
    private Date publishedDate;
    private int markerCount;
    private int linkageGroupCount;
    private String comments;

    public String getMapDbId() {
        return this.mapDbId;
    }

    public void setMapDbId(String mapDbId) {
        this.mapDbId = mapDbId;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSpecies() {
        return this.species;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUnit() {
        return this.unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Date getPublishedDate() {
        return this.publishedDate;
    }

    public void setPublishedDate(Date publishedDate) {
        this.publishedDate = publishedDate;
    }

    public int getMarkerCount() {
        return this.markerCount;
    }

    public void setMarkerCount(int markerCount) {
        this.markerCount = markerCount;
    }

    public int getLinkageGroupCount() {
        return this.linkageGroupCount;
    }

    public void setLinkageGroupCount(int linkageGroupCount) {
        this.linkageGroupCount = linkageGroupCount;
    }

    public String getComments() {
        return this.comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String toString() {
        return "Map{mapDbId=" + this.mapDbId + ", name='" + this.name + '\'' + ", species='" + this.species + '\'' + ", type='" + this.type + '\'' + ", unit='" + this.unit + '\'' + ", publishedDate=" + this.publishedDate + ", markerCount=" + this.markerCount + ", linkageGroupCount=" + this.linkageGroupCount + ", comments=" + this.comments + '}';
    }
}

