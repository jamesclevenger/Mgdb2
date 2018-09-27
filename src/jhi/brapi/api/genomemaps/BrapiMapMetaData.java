package jhi.brapi.api.genomemaps;

import java.util.List;
import jhi.brapi.api.genomemaps.BrapiLinkageGroup;

public class BrapiMapMetaData {
    private String mapDbId;
    private String name;
    private String type;
    private String unit;
    private List<BrapiLinkageGroup> linkageGroups;

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

    public List<BrapiLinkageGroup> getLinkageGroups() {
        return this.linkageGroups;
    }

    public void setLinkageGroups(List<BrapiLinkageGroup> linkageGroups) {
        this.linkageGroups = linkageGroups;
    }

    public String toString() {
        return "MapDetail{name='" + this.name + '\'' + ", type='" + this.type + '\'' + ", unit='" + this.unit + '\'' + '}';
    }
}

