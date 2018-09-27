package jhi.brapi.api;

import java.util.ArrayList;
import java.util.List;
import jhi.brapi.api.Pagination;
import jhi.brapi.api.Status;

public class Metadata {
    private Pagination pagination = new Pagination();
    private List<Status> status = new ArrayList<Status>();
    private List<String> datafiles = new ArrayList<String>();

    public Pagination getPagination() {
        return this.pagination;
    }

    public void setPagination(Pagination pagination) {
        this.pagination = pagination;
    }

    public List<Status> getStatus() {
        return this.status;
    }

    public void setStatus(List<Status> status) {
        this.status = status;
    }

    public List<String> getDatafiles() {
        return this.datafiles;
    }

    public void setDatafiles(List<String> datafiles) {
        this.datafiles = datafiles;
    }
}

