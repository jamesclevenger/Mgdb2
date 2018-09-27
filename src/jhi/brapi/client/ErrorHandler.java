/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016, 2018, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 *******************************************************************************/
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

