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
package fr.cirad.mgdb.importing;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BulkWriteResult;

import fr.cirad.io.brapi.BrapiClient;
import fr.cirad.io.brapi.BrapiService;
import fr.cirad.io.brapi.BrapiClient.Pager;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.search.BrapiSearchResult;

/**
 * The Class IndividualMetadataImport.
 */
public class IndividualMetadataImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(IndividualMetadataImport.class);

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 3)
			throw new Exception("You must pass 3 or 4 parameters as arguments: DATASOURCE name, metadata file path (TSV format with header on first line), label of column containing individual names (matching those in the DB), and optionally a CSV list of column labels for fields to import (all will be imported if no such parameter is supplied).");
		importIndividualMetadata(args[0], new File(args[1]).toURI().toURL(), args[2], args.length > 3 ? args[2] : null, null);
	}
	
	public static void importIndividualMetadata(String sModule, URL metadataFileURL, String individualColName, String csvFieldListToImport, String username) throws Exception 
	{
		List<String> fieldsToImport = csvFieldListToImport != null ? Arrays.asList(csvFieldListToImport.toLowerCase().split(",")) : null;
		Scanner scanner = new Scanner(metadataFileURL.openStream());

		GenericXmlApplicationContext ctx = null;
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		if (mongoTemplate == null) { // we are probably being invoked offline
			ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

			MongoTemplateManager.initialize(ctx);
			mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
				throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
		}
  
		try
		{
			HashMap<Integer, String> columnLabels = new HashMap<Integer, String>();
			int idColumn = -1;
			String sLine = null;
			BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
			List<String> passedIndList = new ArrayList<>();
			while (scanner.hasNextLine()) {
				sLine = scanner.nextLine().trim();
				if (sLine.length() == 0)
					continue;

				if (columnLabels.isEmpty() && sLine.startsWith("\uFEFF"))
					sLine = sLine.substring(1);

				List<String> cells = Helper.split(sLine, "\t");

				if (columnLabels.isEmpty()) { // it's the header line
					for (int i=0; i<cells.size(); i++)
					{
						String cell = cells.get(i);
						if (cell.equalsIgnoreCase(individualColName))
							idColumn = i;
						else if (fieldsToImport == null || fieldsToImport.contains(cell.toLowerCase()))
							columnLabels.put(i, cell);
					}
					if (idColumn == -1)
						throw new Exception(cells.size() <= 1 ? "Provided file does not seem to be tab-delimited!" : "Unable to find individual name column \"" + individualColName + "\" in file header!");
//					if (columnLabels.size() == 0)
//						throw new Exception("Unable to find any columns to import in file header!");

					continue;
				}

				// now deal with actual data rows
				HashMap<String, Comparable> additionalInfo = new HashMap<>();
				for (int col : columnLabels.keySet())
					if (col != idColumn)
						additionalInfo.put(columnLabels.get(col), cells.size() > col ? cells.get(col) : "");

				String individualId = cells.get(idColumn);
				passedIndList.add(individualId);
				if (username == null)
					bulkOperations.updateMulti(new Query(Criteria.where("_id").is(individualId)), new Update().set(Individual.SECTION_ADDITIONAL_INFO, additionalInfo));
				else
					bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(individualId, username))), new Update().set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO, additionalInfo));
			}
			
			if (passedIndList.size() == 0) {
				if (username == null)
					bulkOperations.updateMulti(new Query(), new Update().unset(Individual.SECTION_ADDITIONAL_INFO)); // a blank metadata file was submitted: let's delete any existing metadata				
				else
					bulkOperations.remove(new Query(Criteria.where("_id." + CustomIndividualMetadata.CustomIndividualMetadataId.FIELDNAME_USER).is(username)));
			}
			else
			{	// first check if any passed individuals are unknown
				Query verificationQuery = new Query(Criteria.where("_id").in(passedIndList));
				verificationQuery.fields().include("_id");
				List<String> foundIndList = mongoTemplate.find(verificationQuery, Individual.class).stream().map(ind -> ind.getId()).collect(Collectors.toList());
				if (foundIndList.size() < passedIndList.size())
					throw new Exception("The following individuals do not exist in the selected database: " + StringUtils.join(CollectionUtils.disjunction(passedIndList, foundIndList), ", "));
			}
			BulkWriteResult bwr = bulkOperations.execute();
			if (passedIndList.size() == 0)
				LOG.info("Database " + sModule + ": metadata was deleted for " + bwr.getModifiedCount() + " individuals");
			else
				LOG.info("Database " + sModule + ": " + bwr.getModifiedCount() + " individuals updated with metadata, out of " + bwr.getMatchedCount() + " matched documents");
		}
		finally
		{
			scanner.close();
			if (ctx != null)
				ctx.close();
		}
	}

	public void importBrapiMetadata(String sModule, String endpointUrl, HashMap<String, String> germplasmDbIdToIndividualMap) throws Exception
	{
		BrapiClient client = new BrapiClient();
		client.initService(endpointUrl);
		client.getCalls();
		client.ensureGermplasmInfoCanBeImported();
		final BrapiService service = client.getService();

		HashMap<String, Object> reqBody = new HashMap<>();
		reqBody.put("germplasmDbIds", germplasmDbIdToIndividualMap.keySet());
		BrapiSearchResult bsr = service.searchGermplasm(reqBody).execute().body().getResult();
		
		Pager callPager = new Pager();
		List<BrapiGermplasm> germplasmList = new ArrayList<>();
		while (callPager.isPaging())
		{
			BrapiListResource<BrapiGermplasm> br = service.searchGermplasmResult(bsr.getSearchResultDbId()).execute().body();
			germplasmList.addAll(br.data());
			callPager.paginate(br.getMetadata());
		}
		ObjectMapper oMapper = new ObjectMapper();
		for (BrapiGermplasm g : germplasmList)
			System.err.println(oMapper.convertValue(g, Map.class));
	}
}
