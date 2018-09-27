package jhi.brapi.api.studies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.sql.Date;
import java.util.List;
import jhi.brapi.api.locations.BrapiLocation;
import jhi.brapi.api.studies.BrapiContact;
import jhi.brapi.api.studies.BrapiDataLinks;
import jhi.brapi.api.studies.BrapiLastUpdate;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiStudiesDetail {
    private String studyDbId;
    private String studyName;
    private String studyType;
    private String studyDescription;
    private List<String> seasons;
    private String trialDbId;
    private String trialName;
    private Date startDate;
    private Date endDate;
    private boolean active;
    private String license;
    private BrapiLocation location;
    private List<BrapiContact> contacts;
    private List<BrapiDataLinks> dataLinks;
    private BrapiLastUpdate lastUpdate;
    private Object additionalInfo;

    public String getStudyDbId() {
        return this.studyDbId;
    }

    public void setStudyDbId(String studyDbId) {
        this.studyDbId = studyDbId;
    }

    public String getStudyName() {
        return this.studyName;
    }

    public void setStudyName(String studyName) {
        this.studyName = studyName;
    }

    public String getStudyDescription() {
        return this.studyDescription;
    }

    public void setStudyDescription(String studyDescription) {
        this.studyDescription = studyDescription;
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

    public BrapiLocation getLocation() {
        return this.location;
    }

    public void setLocation(BrapiLocation location) {
        this.location = location;
    }

    public List<BrapiContact> getContacts() {
        return this.contacts;
    }

    public void setContacts(List<BrapiContact> contacts) {
        this.contacts = contacts;
    }

    public Object getAdditionalInfo() {
        return this.additionalInfo;
    }

    public void setAdditionalInfo(Object additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public boolean isActive() {
        return this.active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getLicense() {
        return this.license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public BrapiLastUpdate getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate(BrapiLastUpdate lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public List<BrapiDataLinks> getDataLinks() {
        return this.dataLinks;
    }

    public void setDataLinks(List<BrapiDataLinks> dataLinks) {
        this.dataLinks = dataLinks;
    }
}

