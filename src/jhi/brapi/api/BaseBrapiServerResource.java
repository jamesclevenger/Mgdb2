package jhi.brapi.api;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;

import com.fasterxml.jackson.databind.SerializationFeature;

public abstract class BaseBrapiServerResource<T>
extends ServerResource {
    protected static final String PARAM_PAGE_SIZE = "pageSize";
    protected static final String PARAM_CURRENT_PAGE = "page";
    protected int pageSize = 2000;
    protected int currentPage = 0;

    public void doInit() {
        String page;
        super.doInit();
        String pageSize = this.getQueryValue(PARAM_PAGE_SIZE);
        if (pageSize != null) {
            try {
                this.pageSize = Integer.parseInt(this.getQueryValue(PARAM_PAGE_SIZE));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        if ((page = this.getQueryValue(PARAM_CURRENT_PAGE)) != null) {
            try {
                this.currentPage = Integer.parseInt(this.getQueryValue(PARAM_CURRENT_PAGE));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected List<String> parseGetParameterList(String input) {
        if ((input = this.getQueryValue(input)) != null) {
            return Arrays.stream(input.split(",")).map(String::trim).collect(Collectors.toList());
        }
        return null;
    }

    protected void setPaginationParameters(BasicPost parameters) {
        if (parameters != null) {
            if (parameters.getPage() != null) {
                this.currentPage = parameters.getPage();
            }
            if (parameters.getPageSize() != null) {
                this.pageSize = parameters.getPageSize();
            }
        }
    }

    @Get(value="json")
    public abstract T getJson();

    @Get(value="html")
    public Representation getHtml() {
        T result = this.getJson();
        if (result != null) {
            JacksonRepresentation rep = new JacksonRepresentation(result);
            rep.getObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            return rep;
        }
        throw new ResourceException(404);
    }
}

