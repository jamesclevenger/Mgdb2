package jhi.brapi.api.studies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiContact {
    private String contactDbId;
    private String name;
    private String instituteName;
    private String email;
    private String type;
    private String orcid;

    public String getContactDbId() {
        return this.contactDbId;
    }

    public void setContactDbId(String contactDbId) {
        this.contactDbId = contactDbId;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getInstituteName() {
        return this.instituteName;
    }

    public void setInstituteName(String instituteName) {
        this.instituteName = instituteName;
    }

    public String getEmail() {
        return this.email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getType() {
        return this.type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrcid() {
        return this.orcid;
    }

    public void setOrcid(String orcid) {
        this.orcid = orcid;
    }
}

