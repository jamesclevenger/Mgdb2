package jhi.brapi.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;

public class BrapiListResource<T> {
    private Metadata metadata = new Metadata();
    private Map<String, List<T>> result;

    public BrapiListResource() {
        this.metadata.setPagination(Pagination.empty());
    }

    public BrapiListResource(List<T> list, int currentPage, int pageSize, long totalCount) {
        this.result = this.wrapList("data", list);
        this.metadata.setPagination(new Pagination(pageSize, currentPage, totalCount, pageSize));
    }

    private Map<String, List<T>> wrapList(String label, List<T> wrappedObject) {
        HashMap<String, List<T>> map = new HashMap<String, List<T>>();
        map.put(label, wrappedObject);
        return map;
    }

    public Metadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Map<String, List<T>> getResult() {
        return this.result;
    }

    public void setResult(Map<String, List<T>> result) {
        this.result = result;
    }

    public List<T> data() {
        return this.result.get("data");
    }
}

