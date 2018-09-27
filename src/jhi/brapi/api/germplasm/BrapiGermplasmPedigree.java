package jhi.brapi.api.germplasm;

public class BrapiGermplasmPedigree {
    private String germplasmDbId;
    private String defaultDisplayName;
    private String pedigree;
    private String parent1Id;
    private String parent2Id;

    public String getGermplasmDbId() {
        return this.germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

    public String getDefaultDisplayName() {
        return this.defaultDisplayName;
    }

    public void setDefaultDisplayName(String defaultDisplayName) {
        this.defaultDisplayName = defaultDisplayName;
    }

    public String getPedigree() {
        return this.pedigree;
    }

    public void setPedigree(String pedigree) {
        this.pedigree = pedigree;
    }

    public String getParent1Id() {
        return this.parent1Id;
    }

    public void setParent1Id(String parent1Id) {
        this.parent1Id = parent1Id;
    }

    public String getParent2Id() {
        return this.parent2Id;
    }

    public void setParent2Id(String parent2Id) {
        this.parent2Id = parent2Id;
    }

    public String toString() {
        return "BrapiGermplasmPedigree{germplasmDbId='" + this.germplasmDbId + '\'' + ", defaultDisplayName='" + this.defaultDisplayName + '\'' + ", pedigree='" + this.pedigree + '\'' + ", parent1Id='" + this.parent1Id + '\'' + ", parent2Id='" + this.parent2Id + '\'' + '}';
    }
}

