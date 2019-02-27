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

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
//import org.springframework.data.mongodb.core.index.CompoundIndex;
//import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
//import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;

/**
 * The Class VariantData.
 */
@Document(collection = "variants")
@TypeAlias("VD")
//@CompoundIndexes({
//    @CompoundIndex(def = "{'" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE + "': 1, '" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE + "': 1}")
//})
public class VariantData extends AbstractVariantData
{
	
	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
	/** The id. */
	@Id
	protected String id;

	/**
	 * Instantiates a new variant data.
	 */
	public VariantData() {
		super();
	}
	
	/**
	 * Instantiates a new variant data.
	 *
	 * @param id the id
	 */
	public VariantData(String id) {
		super();
		this.id = id;
	}
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public String getId() {
		return (String) id;
	}

	@Override
	public boolean equals(Object o)
	{
		if (this == o)
			return true;
		
		if (o == null || !(o instanceof VariantData))
			return false;
		
		return getId().equals(((VariantData)o).getId());
	}
}