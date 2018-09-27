package jhi.brapi.api.markerprofiles;

import java.util.List;

public class BrapiAlleleMatrix {
    private List<List<String>> data;

    public List<List<String>> getData() {
        return this.data;
    }

    public void setData(List<List<String>> data) {
        this.data = data;
    }

    public String markerId(int line) {
        return this.data.get(line).get(0);
    }

    public String markerProfileId(int line) {
        return this.data.get(line).get(1);
    }

    public String allele(int line) {
        return this.data.get(line).get(2);
    }
}

