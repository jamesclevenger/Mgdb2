package jhi.brapi.api.studies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import jhi.brapi.api.BasicPost;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiStudiesPost
extends BasicPost {
    private String studyType;
    private List<String> studyNames;
    private List<String> studyLocations;
    private List<String> programNames;
    private List<String> germplasmDbIds;
    private List<String> observationVariableDbIds;
    private boolean active;
    private String sortBy;
    private String sortOrder;

    public String getStudyType() {
        return this.studyType;
    }

    public void setStudyType(String studyType) {
        this.studyType = studyType;
    }

    public List<String> getStudyNames() {
        return this.studyNames;
    }

    public void setStudyNames(List<String> studyNames) {
        this.studyNames = studyNames;
    }

    public List<String> getStudyLocations() {
        return this.studyLocations;
    }

    public void setStudyLocations(List<String> studyLocations) {
        this.studyLocations = studyLocations;
    }

    public List<String> getProgramNames() {
        return this.programNames;
    }

    public void setProgramNames(List<String> programNames) {
        this.programNames = programNames;
    }

    public List<String> getGermplasmDbIds() {
        return this.germplasmDbIds;
    }

    public void setGermplasmDbIds(List<String> germplasmDbIds) {
        this.germplasmDbIds = germplasmDbIds;
    }

    public List<String> getObservationVariableDbIds() {
        return this.observationVariableDbIds;
    }

    public void setObservationVariableDbIds(List<String> observationVariableDbIds) {
        this.observationVariableDbIds = observationVariableDbIds;
    }

    public String getSortBy() {
        return this.sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public String getSortOrder() {
        return this.sortOrder;
    }

    public void setSortOrder(String sortOrder) {
        this.sortOrder = sortOrder;
    }

    public boolean isActive() {
        return this.active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}

