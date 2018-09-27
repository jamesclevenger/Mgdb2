package jhi.brapi.api.studies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.sql.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiStudies {
    private String studyDbId;
    private String name;
    private String trialDbId;
    private String trialName;
    private List<String> seasons;
    private String locationDbId;
    private String locationName;
    private String programDbId;
    private String programName;
    private Date startDate;
    private Date endDate;
    private String studyType;
    private boolean active;
    private Object additionalInfo;

    public String getStudyDbId() {
        return this.studyDbId;
    }

    public void setStudyDbId(String studyDbId) {
        this.studyDbId = studyDbId;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStudyType() {
        return this.studyType;
    }

    public void setStudyType(String studyType) {
        this.studyType = studyType;
    }

    public List<String> getSeasons() {
        return this.seasons;
    }

    public void setSeasons(List<String> seasons) {
        this.seasons = seasons;
    }

    public String getTrialDbId() {
        return this.trialDbId;
    }

    public void setTrialDbId(String trialDbId) {
        this.trialDbId = trialDbId;
    }

    public String getTrialName() {
        return this.trialName;
    }

    public void setTrialName(String trialName) {
        this.trialName = trialName;
    }

    public Date getStartDate() {
        return this.startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return this.endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public boolean isActive() {
        return this.active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Object getAdditionalInfo() {
        return this.additionalInfo;
    }

    public void setAdditionalInfo(Object additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String getLocationDbId() {
        return this.locationDbId;
    }

    public void setLocationDbId(String locationDbId) {
        this.locationDbId = locationDbId;
    }

    public String getLocationName() {
        return this.locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public String getProgramDbId() {
        return this.programDbId;
    }

    public void setProgramDbId(String programDbId) {
        this.programDbId = programDbId;
    }

    public String getProgramName() {
        return this.programName;
    }

    public void setProgramName(String programName) {
        this.programName = programName;
    }
}

