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
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.bson.BasicBSONObject;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBObject;
import com.mongodb.client.model.UpdateOptions;

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
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

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

	public void importBrapiMetadata(String sModule, String endpointUrl, HashMap<String, String> germplasmDbIdToIndividualMap, String username) throws Exception
	{
		BrapiClient client = new BrapiClient();
//		client.getCalls();
		
/*		Interceptor interceptor = new Interceptor() {

			@Override
			public Response intercept(Chain chain) throws IOException {
				String authToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhdXRoMCIsImlhdCI6MTU4MDcyOTA2Nn0.oO9sAabdK1_7uKuECvexMRqYdYmc2TLLkwARcsdIQUw";
				Request originalRequest = chain.request();
				Request newRequest = originalRequest.newBuilder().addHeader("Authorization", "Bearer " + authToken).build();
				
				Response response = chain.proceed(newRequest);
				return response;
			}
			
		};*/
		String authToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhdXRoMCIsImlhdCI6MTU4MDczNDUyOX0.PPJgAq7LSeyHRFifba6RB6p3V-PGiVB4ppFbsEeFWfE";
//		String bearer = "Bearer %s";
/*		Interceptor interceptor = chain ->
		{
			Request original = chain.request();

			// If we already have an authorization token in the header, or we
			// don't have a valid token to add, return the original request
//			if (original.header("Authorization") != null || authToken == null || authToken.isEmpty())
//				return chain.proceed(original);

			// Otherwise add the header and return the tweaked request
			Request next = original.newBuilder()
					.header("Authorization", String.format(bearer, authToken))
					.build();
			System.out.println("hit");

			return chain.proceed(next);
		};*/
		
		client.initService(endpointUrl, authToken);
//		client.getHttpClient().newBuilder().addNetworkInterceptor(interceptor).build();
//		client.setHttpClient(client.getHttpClient().newBuilder().addNetworkInterceptor(interceptor).build());

//		client.setHttpClient(client.getHttpClient().newBuilder().addInterceptor(interceptor).build());
		LOG.info(client.getHttpClient().interceptors());
		
		final BrapiService service = client.getService();
//		client.getUnsafeOkHttpClient();
		

		HashMap<String, Object> reqBody = new HashMap<>();
		reqBody.put("germplasmDbIds", germplasmDbIdToIndividualMap.keySet());

		List<BrapiGermplasm> germplasmList = new ArrayList<>();
		ObjectMapper oMapper = new ObjectMapper();
		
//		searchGermplasmDirectResult
		
		try {
			LOG.info("searchGermplasm");
			BrapiSearchResult bsr = service.searchGermplasm(reqBody).execute().body().getResult();
			Pager callPager = new Pager();
			while (callPager.isPaging())
			{
				BrapiListResource<BrapiGermplasm> br = service.searchGermplasmResult(bsr.getSearchResultDbId()).execute().body();
				germplasmList.addAll(br.data());
				callPager.paginate(br.getMetadata());
			}
			
		} catch (Exception e) {
			try {
				LOG.info("searchGermplasmDirectResult");
				
				
//				LOG.info(service.searchGermplasmDirectResult(reqBody).request().newBuilder().addHeader("lol" , "lol" ).build().headers().toString());
//				Request request = service.searchGermplasmDirectResult(reqBody).request().newBuilder().addHeader("lol" , "lol" ).build();
//				client.getUnsafeOkHttpClient().newCall(request);
				LOG.info(service.searchGermplasmDirectResult(reqBody).request().headers().toString());
				LOG.info(service.searchGermplasmDirectResult(reqBody).execute().headers().toString());
				LOG.info(service.searchGermplasmDirectResult(reqBody).execute().code());
				
				
				BrapiListResource<BrapiGermplasm> br = service.searchGermplasmDirectResult(reqBody).execute().body();
				Pager callPager = new Pager();
				while (callPager.isPaging()) {
					germplasmList.addAll(br.data());
					callPager.paginate(br.getMetadata());
				}
			}catch(Exception f) {
				e.printStackTrace();
				f.printStackTrace();
	            LOG.debug(e);
	            LOG.debug(f);
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
		
		if(username == null) {
			BulkWriteOperation bulkWriteOperation= mongoTemplate.getCollection("individuals").initializeUnorderedBulkOperation();
			LOG.info("Database " + sModule + ": individuals");
		for (BrapiGermplasm g : germplasmList) {
			Map<Object, Object> aiMap = oMapper.convertValue(g, Map.class);
//			LOG.info(aiMap);
//			service.getAttributes(aiMap.get("germplasmDbId").toString()).request().newBuilder().addHeader("name", "value");
//			service.getAttributes(aiMap.get("germplasmDbId").toString()).request().headers().newBuilder().add("bearer", "token");
//			service.getAttributes(aiMap.get("germplasmDbId").toString()).cancel();
			
			System.out.println("HIT");
			LOG.info(service.getAttributes(aiMap.get("germplasmDbId").toString()).request().headers().toString());
//			service.getAttributes(aiMap.get("germplasmDbId").toString()).execute().body();
//			-------- MUST BE PROTECTED --------
			BrapiListResource<Object> moreAttributes = service.getAttributes(aiMap.get("germplasmDbId").toString()).execute().body();
//			LOG.info(service.getAttributes(aiMap.get("germplasmDbId").toString()));
//			LOG.info(moreAttributes.data().toString());
			Update update = new Update();
			
			moreAttributes.data().forEach(
//					(k)->LOG.info(((LinkedHashMap<String,String>)k).get("attributeDbId").toString())
					(k)->aiMap.put(((LinkedHashMap<String,String>)k).get("attributeDbId").toString(), ((LinkedHashMap<String,String>)k).get("value").toString()));
//					(k)->update.set(Individual.SECTION_ADDITIONAL_INFO + "." + ((LinkedHashMap<String,String>)k).get("attributeDbId").toString(), ((LinkedHashMap<String,String>)k).get("value").toString()));
//			-------- MUST BE PROTECTED --------
			
			
			
			// remove ArrayList and null entry from ai fields
			Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
			while(itr.hasNext())
			{
			   Map.Entry<Object, Object> entry = itr.next();
			   if(entry.getValue() instanceof ArrayList || entry.getValue()==null)
			   {
				   itr.remove();
			   }
			}
			
			
//			LOG.info(aiMap);
			
	
			

	        
			
	        aiMap.forEach((k,v)->update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
			bulkWriteOperation.find(new BasicDBObject("_id", germplasmDbIdToIndividualMap.get(aiMap.get("germplasmDbId")))).upsert().updateOne(update.getUpdateObject());
			
			
			
			
		}bulkWriteOperation.execute();
		}else {
			BulkWriteOperation bulkWriteOperation= mongoTemplate.getCollection("customIndividualMetadata").initializeUnorderedBulkOperation();
			LOG.info("Database " + sModule + ": customIndividualMetadata");

			for (BrapiGermplasm g : germplasmList) {
				Map<Object, Object> aiMap = oMapper.convertValue(g, Map.class);
				
				
				
//				-------- MUST BE PROTECTED --------
				BrapiListResource<Object> moreAttributes = service.getAttributes(aiMap.get("germplasmDbId").toString()).execute().body();
//				LOG.info(service.getAttributes(aiMap.get("germplasmDbId").toString()));
//				LOG.info(moreAttributes.data().toString());
				Update update = new Update();
				
				moreAttributes.data().forEach(
//						(k)->LOG.info(((LinkedHashMap<String,String>)k).get("attributeDbId").toString())
						(k)->aiMap.put(((LinkedHashMap<String,String>)k).get("attributeDbId").toString(), ((LinkedHashMap<String,String>)k).get("value").toString()));
//						(k)->update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + ((LinkedHashMap<String,String>)k).get("attributeDbId").toString(), ((LinkedHashMap<String,String>)k).get("value").toString()));
//				-------- MUST BE PROTECTED --------
				
				
				
				// remove ArrayList and null entry from ai fields
				Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
				while(itr.hasNext())
				{
				   Map.Entry<Object, Object> entry = itr.next();
				   if(entry.getValue() instanceof ArrayList || entry.getValue()==null)
				   {
					   itr.remove();
				   }
				}
				
				
				
				
				
				
				


				
				
		        aiMap.forEach((k,v)->update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));
		        
				Map<Object, Object> idMap = new HashMap<Object,Object>();
				idMap.put(CustomIndividualMetadata.CustomIndividualMetadataId.FIELDNAME_INDIVIDUAL_ID, germplasmDbIdToIndividualMap.get(aiMap.get("germplasmDbId")));
				idMap.put(CustomIndividualMetadata.CustomIndividualMetadataId.FIELDNAME_USER, username);
				bulkWriteOperation.find(new BasicDBObject("_id", idMap)).upsert().updateOne(update.getUpdateObject());
				
				
				
			}bulkWriteOperation.execute();}

		
		
		

	}
}
