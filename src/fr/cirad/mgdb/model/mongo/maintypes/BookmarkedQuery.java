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
package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.HashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

/**
 * The Class BookmarkedQuery.
 */
@Document(collection = "bookmarkedQueries")
@TypeAlias("bq")
public class BookmarkedQuery {
	
	public final static String FIELDNAME_LABELS_FOR_USERS = "lu";
	
	public final static String FIELDNAME_CACHED_FILTERS = "cf";

	/** The entity id. */
	@Id
	private String id;

	/** Users who keep this query's filters in cache */
	@Field(FIELDNAME_LABELS_FOR_USERS)
	private HashMap<String, String> labelsForUsers = new HashMap<>();
		
	/** The individualFIELDNAME_CACHED_FILTERS */
	@Field(FIELDNAME_CACHED_FILTERS)
	private HashMap<String, Object> savedFilters;

	/**
	 * Instantiates a new BookmarkedQuery.
	 *
	 * @param queryKey the query id
	 */
	public BookmarkedQuery(String queryKey) {
		this.id = queryKey;
	}

    public BookmarkedQuery() {
    }

	public String getId() {
		return id;
	}

	public HashMap<String, String> getLabelsForUsers() {
		return labelsForUsers;
	}

	public void setLabelsForUsers(HashMap<String, String> users) {
		this.labelsForUsers = users;
	}

	public HashMap<String, Object> getSavedFilters() {
		return savedFilters;
	}

	public void setSavedFilters(HashMap<String, Object> savedFilters) {
		this.savedFilters = savedFilters;
	}
}