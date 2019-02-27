/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016 - 2019, <CIRAD> <IRD>
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
package fr.cirad.io.brapi;

import java.util.*;

import jhi.brapi.api.calls.*;

public class CallsUtils
{
	public static final String GET = "GET";
	public static final String POST = "POST";
	public static final String JSON = "json";
	public static final String TSV = "tsv";

	private List<BrapiCall> calls;

	public CallsUtils(List<BrapiCall> calls)
	{
		this.calls = calls;
	}

	boolean validate()
	{
		// First validate the calls that MUST be present
		if (hasCall("studies-search", JSON, GET) == false)
			return false;
		if (hasCall("maps", JSON, GET) == false)
			return false;
		if (hasCall("maps/{mapDbId}/positions", JSON, GET) == false)
			return false;
		if (hasCall("markerprofiles", JSON, GET) == false)
			return false;
		if (hasCall("allelematrix-search", JSON, POST) == false && hasCall("allelematrix-search", TSV, POST) == false)
			return false;

		return true;
	}

	public boolean hasCall(String signature, String datatype, String method)
	{
		for (BrapiCall call : calls)
			if (call.getCall().equals(signature) && call.getDatatypes().contains(datatype) && call.getMethods().contains(method))
				return true;

		return false;
	}

	boolean hasToken()
	{
		return hasCall("token", JSON, GET);
	}

	boolean hasPostMarkersSearch()
	{
		return hasCall("markers-search", JSON, POST);
	}

	boolean hasGetMarkersSearch()
	{
		return hasCall("markers-search", JSON, GET);
	}
	
	boolean hasMarkersDetails()
	{
		return hasCall("markers/{markerDbId}", JSON, GET);
	}

//	boolean hasMapsMapDbId()
//	{
//		return hasCall("maps/id", JSON, GET);
//	}

	boolean hasAlleleMatrixSearchTSV()
	{
		return hasCall("allelematrix-search", TSV, POST);
	}
}