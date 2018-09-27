package jhi.brapi.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiErrorResource {
    private Metadata metadata = new Metadata();
    private Map<Object, Object> result = new HashMap<Object, Object>();

    public BrapiErrorResource() {
        this.metadata.setPagination(Pagination.empty());
    }

    public Metadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Object getResult() {
        return this.result;
    }

    public void setResult(Map<Object, Object> result) {
        this.result = result;
    }
}

