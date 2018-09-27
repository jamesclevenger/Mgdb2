package jhi.brapi.api.germplasm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import jhi.brapi.api.BasicPost;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiGermplasmPost
extends BasicPost {
    private List<String> germplasmPUIs;
    private List<String> germplasmDbIds;
    private List<String> germplasmSpecies;
    private List<String> germplasmGenus;
    private List<String> germplasmNames;
    private List<String> accessionNumbers;

    public List<String> getGermplasmPUIs() {
        return this.germplasmPUIs;
    }

    public void setGermplasmPUIs(List<String> germplasmPUIs) {
        this.germplasmPUIs = germplasmPUIs;
    }

    public List<String> getGermplasmDbIds() {
        return this.germplasmDbIds;
    }

    public void setGermplasmDbIds(List<String> germplasmDbIds) {
        this.germplasmDbIds = germplasmDbIds;
    }

    public List<String> getGermplasmSpecies() {
        return this.germplasmSpecies;
    }

    public void setGermplasmSpecies(List<String> germplasmSpecies) {
        this.germplasmSpecies = germplasmSpecies;
    }

    public List<String> getGermplasmGenus() {
        return this.germplasmGenus;
    }

    public void setGermplasmGenus(List<String> germplasmGenus) {
        this.germplasmGenus = germplasmGenus;
    }

    public List<String> getGermplasmNames() {
        return this.germplasmNames;
    }

    public void setGermplasmNames(List<String> germplasmNames) {
        this.germplasmNames = germplasmNames;
    }

    public List<String> getAccessionNumbers() {
        return this.accessionNumbers;
    }

    public void setAccessionNumbers(List<String> accessionNumbers) {
        this.accessionNumbers = accessionNumbers;
    }
}

