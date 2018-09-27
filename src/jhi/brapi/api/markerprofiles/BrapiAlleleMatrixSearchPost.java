package jhi.brapi.api.markerprofiles;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import jhi.brapi.api.BasicPost;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiAlleleMatrixSearchPost
extends BasicPost {
    private List<String> markerprofileDbId;
    private List<String> markerDbId;
    private List<String> matrixDbId;
    private String format;
    private boolean expandHomozygotes;
    private String unknownString;
    private String sepPhased;
    private String sepUnphased;

    public List<String> getMarkerprofileDbId() {
        return this.markerprofileDbId;
    }

    public void setMarkerprofileDbId(List<String> markerprofileDbId) {
        this.markerprofileDbId = markerprofileDbId;
    }

    public List<String> getMarkerDbId() {
        return this.markerDbId;
    }

    public void setMarkerDbId(List<String> markerDbId) {
        this.markerDbId = markerDbId;
    }

    public List<String> getMatrixDbId() {
        return this.matrixDbId;
    }

    public void setMatrixDbId(List<String> matrixDbId) {
        this.matrixDbId = matrixDbId;
    }

    public String getFormat() {
        return this.format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public boolean isExpandHomozygotes() {
        return this.expandHomozygotes;
    }

    public void setExpandHomozygotes(boolean expandHomozygotes) {
        this.expandHomozygotes = expandHomozygotes;
    }

    public String getUnknownString() {
        return this.unknownString;
    }

    public void setUnknownString(String unknownString) {
        this.unknownString = unknownString;
    }

    public String getSepPhased() {
        return this.sepPhased;
    }

    public void setSepPhased(String sepPhased) {
        this.sepPhased = sepPhased;
    }

    public String getSepUnphased() {
        return this.sepUnphased;
    }

    public void setSepUnphased(String sepUnphased) {
        this.sepUnphased = sepUnphased;
    }

    public String toString() {
        return "BrapiAlleleMatrixSearchPost{markerprofileDbId=" + this.markerprofileDbId + ", markerDbId=" + this.markerDbId + ", matrixDbId=" + this.matrixDbId + ", format='" + this.format + '\'' + ", expandHomozygotes=" + this.expandHomozygotes + ", unknownString='" + this.unknownString + '\'' + ", sepPhased='" + this.sepPhased + '\'' + ", sepUnphased='" + this.sepUnphased + '\'' + ", pageSize=" + this.pageSize + ", page=" + this.page + '}';
    }
}

