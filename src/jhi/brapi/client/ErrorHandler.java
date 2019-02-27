package jhi.brapi.client;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.stream.Collectors;

import jhi.brapi.api.BrapiErrorResource;
import jhi.brapi.api.Status;
import retrofit2.Converter;
import retrofit2.Response;
import retrofit2.Retrofit;

public class ErrorHandler {
    private static BrapiErrorResource convertResponse(RetrofitServiceGenerator generator, Response<?> response) {
        BrapiErrorResource error;
        Retrofit retrofit = generator.getRetrofit();
        Converter converter = retrofit.responseBodyConverter(BrapiErrorResource.class, new Annotation[0]);
        try {
            error = (BrapiErrorResource)converter.convert((Object)response.errorBody());
        }
        catch (IOException e) {
            return new BrapiErrorResource();
        }
        return error;
    }

    public static String getMessage(RetrofitServiceGenerator generator, Response<?> response) {
        BrapiErrorResource errorResource = ErrorHandler.convertResponse(generator, response);
        List<Status> statuses = errorResource.getMetadata().getStatus();
        String errorMessage = statuses.stream().map(Status::toString).collect(Collectors.joining(", "));
        if (errorMessage.isEmpty()) {
            errorMessage = errorMessage + "No BrAPI status messages found, returning HTTP code and message instead: " + response.code() + " - " + response.message();
        }
        return errorMessage;
    }
}

