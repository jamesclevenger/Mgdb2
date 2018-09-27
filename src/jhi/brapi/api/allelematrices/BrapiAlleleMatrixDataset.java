package jhi.brapi.api.allelematrices;

import java.sql.Date;

public class BrapiAlleleMatrixDataset {
    private String name;
    private String matrixDbId;
    private String description;
    private Date lastUpdated;
    private String studyDbId;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMatrixDbId() {
        return this.matrixDbId;
    }

    public void setMatrixDbId(String matrixDbId) {
        this.matrixDbId = matrixDbId;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Date getLastUpdated() {
        return this.lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public String getStudyDbId() {
        return this.studyDbId;
    }

    public void setStudyDbId(String studyDbId) {
        this.studyDbId = studyDbId;
    }
}

