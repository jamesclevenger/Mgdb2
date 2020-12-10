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
import java.util.Iterator;
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
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;

import fr.cirad.io.brapi.BrapiClient;
import fr.cirad.io.brapi.BrapiClient.Pager;
import fr.cirad.io.brapi.BrapiService;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.Helper;
import fr.cirad.tools.mongo.MongoTemplateManager;
import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.germplasm.BrapiGermplasmAttributes;
import jhi.brapi.api.search.BrapiSearchResult;

import retrofit2.Response;

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
	
	public static int importIndividualMetadata(String sModule, URL metadataFileURL, String individualColName, String csvFieldListToImport, String username) throws Exception 
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
			BulkWriteResult wr = bulkOperations.execute();
			if (passedIndList.size() == 0)
				LOG.info("Database " + sModule + ": metadata was deleted for " + wr.getModifiedCount() + " individuals");
			else
				LOG.info("Database " + sModule + ": " + wr.getModifiedCount() + " individuals updated with metadata, out of " + wr.getMatchedCount() + " matched documents");
			return wr.getModifiedCount() + wr.getUpserts().size();
		}
		finally
		{
			scanner.close();
			if (ctx != null)
				ctx.close();
		}
	}

	public static int importBrapiMetadata(String sModule, String endpointUrl, HashMap<String, String> germplasmDbIdToIndividualMap, String username, String authToken) throws Exception
	{
		if (germplasmDbIdToIndividualMap == null || germplasmDbIdToIndividualMap.isEmpty())
			return 0;	// we must know which individuals to update
		
		BrapiClient client = new BrapiClient();

		// hack to try and make it work with current BMS version
		client.initService(endpointUrl.replace("Ricegigwa/", ""), authToken);
		client.getCalls();
		client.ensureGermplasmInfoCanBeImported();
		client.initService(endpointUrl, authToken);
		
		final BrapiService service = client.getService();

		HashMap<String, Object> reqBody = new HashMap<>();
		reqBody.put("germplasmDbIds", germplasmDbIdToIndividualMap.keySet());

		List<BrapiGermplasm> germplasmList = new ArrayList<>();
		ObjectMapper oMapper = new ObjectMapper();
		
		if (client.hasCallSearchGermplasm()) {
			try {
				Response<BrapiBaseResource<BrapiSearchResult>> response =  service.searchGermplasm(reqBody).execute();
				handleErrorCode(response.code());
				BrapiSearchResult bsr = response.body().getResult();
				
				Pager callPager = new Pager();
				while (callPager.isPaging())
				{
					BrapiListResource<BrapiGermplasm> br = service.searchGermplasmResult(bsr.getSearchResultDbId()).execute().body();
					germplasmList.addAll(br.data());
					callPager.paginate(br.getMetadata());
				}
			} catch (Exception e) {	// we did not get a searchResultDbId: see if we actually got results straight away
				try {
					Response<BrapiListResource<BrapiGermplasm>> response = service.searchGermplasmDirectResult(reqBody).execute();
					handleErrorCode(response.code());
					BrapiListResource<BrapiGermplasm> br = response.body();
					
					Pager callPager = new Pager();
					while (callPager.isPaging()) {
						germplasmList.addAll(br.data());
						callPager.paginate(br.getMetadata());
					}
				}
				catch(Exception f) {
		            LOG.debug(e);
		            LOG.debug(f);
				}
			}
		}

		GenericXmlApplicationContext ctx = null;
		MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
		if (mongoTemplate == null) { // we are probably being invoked offline
			ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

			MongoTemplateManager.initialize(ctx);
			mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
				throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
		}
		
		boolean fCanQueryAttributes = client.hasCallGetAttributes();
		BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
		for (BrapiGermplasm g : germplasmList) {
			Map<Object, Object> aiMap = oMapper.convertValue(g, Map.class);

			if (fCanQueryAttributes) {
				Response<BrapiBaseResource<BrapiGermplasmAttributes>> response = service.getAttributes(aiMap.get("germplasmDbId").toString()).execute();
				if (response.code() != 404) {	// TODO: remove this hack when this call is correctly implemented in BMS
					handleErrorCode(response.code());
					BrapiBaseResource<BrapiGermplasmAttributes> moreAttributes = response.body();
					moreAttributes.getResult().getData().forEach(k -> aiMap.put(k.getAttributeDbId(), k.getValue()));
				}
			}

			// only keep fields whose values are a single-string
			Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
			while(itr.hasNext()) {
			   Map.Entry<Object, Object> entry = itr.next();
			   if (entry.getValue() == null)
				   itr.remove();
			   else if (entry.getValue() instanceof ArrayList) {
				   if (((ArrayList)entry.getValue()).size() == 1)
					   entry.setValue(((ArrayList)entry.getValue()).get(0));
				   else
					   itr.remove();
			   }
			}
	
			if (aiMap.isEmpty()) {
//				throw new Exception("Cannot import an empty list of attributes");
				LOG.warn("Found no metadata to import for germplasm " + g.getGermplasmDbId());
				continue;
			}

			Update update = new Update();			
			if (username == null) {
		        aiMap.forEach((k,v)->update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
		        bulkOperations.updateMulti(new Query(Criteria.where("_id").is(germplasmDbIdToIndividualMap.get(aiMap.get("germplasmDbId")))), update);
			}
			else {
		        aiMap.forEach((k,v)->update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));		        
				bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(germplasmDbIdToIndividualMap.get(aiMap.get("germplasmDbId")), username))), update);
			}
		}
		BulkWriteResult wr = bulkOperations.execute();
		return wr.getModifiedCount() + wr.getUpserts().size();
	}

	private static void handleErrorCode(int code) {
		if(code==400)
		{
			throw new Error("HTTP request returned code 400 - Bad Request");
		}
		if(code==401)//most probably authToken is wrong
		{
			throw new Error("HTTP request returned code 401 - Unauthorized");
		}
		if(code==403)
		{
			throw new Error("HTTP request returned code 403 - Forbidden");
		}
		if(code==404)
		{
			throw new Error("HTTP request returned code 404 - Not Found");
		}
		if(code==500)
		{
			throw new Error("HTTP request returned code 500 - Internal Server Error");
		}
	}
}
