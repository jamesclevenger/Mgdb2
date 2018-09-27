package jhi.brapi.api.calls;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class BrapiCall {
    public static final String DATATYPE_JSON = "json";
    public static final String DATATYPE_TSV = "tsv";
    public static final String DATATYPE_FLAPJACK = "flapjack";
    public static final String METHOD_GET = "GET";
    public static final String METHOD_POST = "POST";
    public static final String METHOD_PUT = "PUT";
    public static final String METHOD_DELETE = "DELETE";
    private String call;
    private List<String> datatypes = new ArrayList<String>();
    private List<String> methods = new ArrayList<String>();

    public BrapiCall() {
    }

    public BrapiCall(String call) {
        this.call = call;
    }

    public BrapiCall addMethod(String method) {
        this.methods.add(method);
        return this;
    }

    public BrapiCall addDatatype(String dataType) {
        this.datatypes.add(dataType);
        return this;
    }

    public BrapiCall withDatatypeJson() {
        return this.addDatatype(DATATYPE_JSON);
    }

    public BrapiCall withDatatypeTsv() {
        return this.addDatatype(DATATYPE_TSV);
    }

    public BrapiCall withDatatypeFlapjack() {
        return this.addDatatype(DATATYPE_FLAPJACK);
    }

    public BrapiCall withMethodGet() {
        return this.addMethod(METHOD_GET);
    }

    public BrapiCall withMethodPost() {
        return this.addMethod(METHOD_POST);
    }

    public BrapiCall withMethodPut() {
        return this.addMethod(METHOD_PUT);
    }

    public BrapiCall withMethodDelete() {
        return this.addMethod(METHOD_DELETE);
    }

    public boolean hasDataType(String dataType) {
        return this.datatypes.stream().filter(d -> d.equalsIgnoreCase(dataType)).count() >= 1L;
    }

    public boolean hasMethod(String method) {
        return this.methods.stream().filter(d -> d.equalsIgnoreCase(method)).count() >= 1L;
    }

    public String getCall() {
        return this.call;
    }

    public void setCall(String call) {
        this.call = call;
    }

    public List<String> getDatatypes() {
        return this.datatypes;
    }

    public void setDatatypes(List<String> datatypes) {
        this.datatypes = datatypes;
    }

    public List<String> getMethods() {
        return this.methods;
    }

    public void setMethods(List<String> methods) {
        this.methods = methods;
    }

    public String toString() {
        return "BrapiCall{call='" + this.call + '\'' + '}';
    }
}

