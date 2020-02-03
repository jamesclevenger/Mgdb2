package jhi.brapi.api.calls;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiCall {
    public static final String dataType_JSON = "json";
    public static final String dataType_TSV = "tsv";
    public static final String dataType_FLAPJACK = "flapjack";
    public static final String METHOD_GET = "GET";
    public static final String METHOD_POST = "POST";
    public static final String METHOD_PUT = "PUT";
    public static final String METHOD_DELETE = "DELETE";

    private String call;
    private List<String> dataTypes = new ArrayList<String>();
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

    public BrapiCall adddataType(String dataType) {
        this.dataTypes.add(dataType);
        return this;
    }

    public BrapiCall withDataTypeJson() {
        return this.adddataType(dataType_JSON);
    }

    public BrapiCall withDataTypeTsv() {
        return this.adddataType(dataType_TSV);
    }

    public BrapiCall withDataTypeFlapjack() {
        return this.adddataType(dataType_FLAPJACK);
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
        return this.dataTypes.stream().filter(d -> d.equalsIgnoreCase(dataType)).count() >= 1L;
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

    public List<String> getDataTypes() {
        return this.dataTypes;
    }

    public void setDataTypes(List<String> dataTypes) {
        this.dataTypes = dataTypes;
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

