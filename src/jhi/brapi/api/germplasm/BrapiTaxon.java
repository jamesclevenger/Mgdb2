package jhi.brapi.api.germplasm;

public class BrapiTaxon {
    private String source;
    private String id;

    public String getSource() {
        return this.source;
    }

    public BrapiTaxon setSource(String source) {
        this.source = source;
        return this;
    }

    public String getId() {
        return this.id;
    }

    public BrapiTaxon setId(String id) {
        this.id = id;
        return this;
    }
}

