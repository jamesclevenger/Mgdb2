package jhi.brapi.api;

import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;

public class BrapiBaseResource<T> {
    private Metadata metadata = new Metadata();
    private T result;

    public BrapiBaseResource() {
        this.metadata.setPagination(Pagination.empty());
    }

    public BrapiBaseResource(T result) {
        this.result = result;
        this.metadata.setPagination(Pagination.forSingleResult());
    }

    public BrapiBaseResource(T result, int currentPage, int pageSize, long totalCount) {
        this.result = result;
        this.metadata.setPagination(new Pagination(pageSize, currentPage, totalCount, pageSize));
    }

    public Metadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public T getResult() {
        return this.result;
    }

    public void setResult(T result) {
        this.result = result;
    }
}

