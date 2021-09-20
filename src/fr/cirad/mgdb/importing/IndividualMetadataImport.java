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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

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
import com.mongodb.bulk.BulkWriteResult;

import fr.cirad.io.brapi.BrapiClient;
import fr.cirad.io.brapi.BrapiClient.Pager;
import fr.cirad.io.brapi.BrapiService;
import fr.cirad.io.brapi.BrapiV2Client;
import fr.cirad.io.brapi.BrapiV2Service;
import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import jhi.brapi.api.BrapiBaseResource;
import jhi.brapi.api.BrapiListResource;
import jhi.brapi.api.germplasm.BrapiGermplasm;
import jhi.brapi.api.germplasm.BrapiGermplasmAttributes;
import jhi.brapi.api.search.BrapiSearchResult;
import org.brapi.v2.model.Germplasm;
import org.brapi.v2.model.GermplasmAttributeValue;
import org.brapi.v2.model.GermplasmAttributeValueListResponse;
import org.brapi.v2.model.GermplasmListResponse;
import org.brapi.v2.model.Sample;
import org.brapi.v2.model.SampleListResponse;
import org.brapi.v2.model.SuccessfulSearchResponse;
import org.brapi.v2.model.SuccessfulSearchResponseResult;

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
		importIndividualMetadata(args[0], null, new File(args[1]).toURI().toURL(), args[2], args.length > 3 ? args[2] : null, null);
	}
	
	public static int importIndividualMetadata(String sModule, HttpSession session, URL metadataFileURL, String individualColName, String csvFieldListToImport, String username) throws Exception 
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
		
		boolean fIsAnonymous = "anonymousUser".equals(username);
		LinkedHashMap<String /*individual*/, LinkedHashMap<String, Comparable>> sessionObject = null;	// start with empty metadata (we only aggregate when we import BrAPI stuff over manually-provided values)
		if (fIsAnonymous) {
			sessionObject = new LinkedHashMap<>();
			session.setAttribute("individuals_metadata_" + sModule, sessionObject);
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

					continue;
				}

				// now deal with actual data rows
				LinkedHashMap<String, Comparable> additionalInfo = new LinkedHashMap<>();
				for (int col : columnLabels.keySet())
					if (col != idColumn)
						additionalInfo.put(columnLabels.get(col), cells.size() > col ? cells.get(col) : "");

				String individualId = cells.get(idColumn);
				passedIndList.add(individualId);
				if (username == null)
					bulkOperations.updateMulti(new Query(Criteria.where("_id").is(individualId)), new Update().set(Individual.SECTION_ADDITIONAL_INFO, additionalInfo));
				else if (!fIsAnonymous)
					bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(individualId, username))), new Update().set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO, additionalInfo));
				else if (session != null)
					sessionObject.put(individualId, additionalInfo);
				else
					LOG.warn("Unable to save metadata for anonymous user (passed HttpSession was null)");
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
			if (!fIsAnonymous) {
				BulkWriteResult wr = bulkOperations.execute();
				if (passedIndList.size() == 0)
					LOG.info("Database " + sModule + ": metadata was deleted for " + wr.getModifiedCount() + " individuals");
				else
					LOG.info("Database " + sModule + ": " + wr.getModifiedCount() + " individuals updated with metadata, out of " + wr.getMatchedCount() + " matched documents");
				return wr.getModifiedCount() + wr.getUpserts().size() + wr.getDeletedCount();
			}
			else {
				LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
				return 1;
			}
		}
		finally
		{
			scanner.close();
			if (ctx != null)
				ctx.close();
		}
	}

	public static int importBrapiMetadata(String sModule, HttpSession session, String endpointUrl, HashMap<String, HashMap<String, String>> germplasmDbIdToIndividualMap, String username, String authToken, ProgressIndicator progress) throws Exception
	{
            if (germplasmDbIdToIndividualMap == null || germplasmDbIdToIndividualMap.isEmpty())
                    return 0;    // we must know which individuals to update

            GenericXmlApplicationContext ctx = null;
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) { // we are probably being invoked offline
                    ctx = new GenericXmlApplicationContext("applicationContext-data.xml");

                    MongoTemplateManager.initialize(ctx);
                    mongoTemplate = MongoTemplateManager.get(sModule);
                    if (mongoTemplate == null)
                            throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
            }      
            
            int modifiedCount = 0;
            
            if (endpointUrl.contains("v1")) {
                
                modifiedCount += importBrapiV1Germplasm(
                    sModule,
                    session,
                    endpointUrl,                    
                    germplasmDbIdToIndividualMap.get("germplasm"),
                    username,
                    authToken,
                    progress,
                    mongoTemplate
                );
                
                modifiedCount += importBrapiV1Samples(
                    sModule,
                    session,
                    endpointUrl,                    
                    germplasmDbIdToIndividualMap.get("sample"),
                    username,
                    authToken,
                    progress,
                    mongoTemplate
                );
                
            } else if (endpointUrl.contains("v2")) {
                
                if (germplasmDbIdToIndividualMap.get("germplasm") != null) {
                    modifiedCount += importBrapiV2Germplasm(
                        sModule,
                        session,
                        endpointUrl,
                        germplasmDbIdToIndividualMap.get("germplasm"),
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                    );
                }
                
                if (germplasmDbIdToIndividualMap.get("sample") != null) {
                    modifiedCount += importBrapiV2Samples(
                        sModule,
                        session,
                        endpointUrl,                    
                        germplasmDbIdToIndividualMap.get("sample"),
                        username,
                        authToken,
                        progress,
                        mongoTemplate
                    );
                }
            } 
            
            return modifiedCount;
        }
        
        
        private static int importBrapiV1Germplasm(
            String sModule, 
            HttpSession session, 
            String endpointUrl, 
            HashMap<String, String> germplasmDbIdToIndividualMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {
        
            HashMap<String, Object> reqBody = new HashMap<>();
            reqBody.put("germplasmDbIds", germplasmDbIdToIndividualMap.keySet());
            
        
            BrapiClient client = new BrapiClient();

            // hack to try and make it work with current BMS version
            client.initService(endpointUrl/*.replace("Ricegigwa/", "")*/, authToken);
            client.getCalls();
            client.ensureGermplasmInfoCanBeImported();
            client.initService(endpointUrl, authToken);

            final BrapiService service = client.getService();

            List<BrapiGermplasm> germplasmList = new ArrayList<>();
            ObjectMapper oMapper = new ObjectMapper();

            if (client.hasCallSearchGermplasm()) {
                    progress.addStep("Getting germplasm list from " + endpointUrl);
                    progress.moveToNextStep();

                    try {
                            Response<BrapiBaseResource<BrapiSearchResult>> response =  service.searchGermplasm(reqBody).execute();
                            handleErrorCode(response.code());
                            BrapiSearchResult bsr = response.body().getResult();

                            Pager germplasmPager = new Pager();
                            while (germplasmPager.isPaging())
                            {
                                    BrapiListResource<BrapiGermplasm> br = service.searchGermplasmResult(bsr.getSearchResultDbId(), germplasmPager.getPageSize(), germplasmPager.getPage()).execute().body();
                                    germplasmList.addAll(br.data());
                                    germplasmPager.paginate(br.getMetadata());
                            }
                    } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
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
                                    progress.setError("Error invoking BrAPI /search/germplasm call (no searchResultDbId returned and yet unable to directly obtain results)");
                                    LOG.error(e);
                                    LOG.error(progress.getError(), f);
                                    return 0;
                            }
                    }
            }



            boolean fCanQueryAttributes = client.hasCallGetAttributes();
            progress.addStep("Getting germplasm information from " + endpointUrl);
            progress.moveToNextStep();

            if (germplasmList.isEmpty())
                    return 0;

            boolean fIsAnonymous = "anonymousUser".equals(username);
            LinkedHashMap<String /*individual*/, LinkedHashMap<String, Comparable>> sessionObject = (LinkedHashMap<String, LinkedHashMap<String, Comparable>>) session.getAttribute("individuals_metadata_" + sModule);
            if (fIsAnonymous  && sessionObject == null) {
                    sessionObject = new LinkedHashMap<>();
                    session.setAttribute("individuals_metadata_" + sModule, sessionObject);
            }

            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
            for (int i=0; i<germplasmList.size(); i++) {
                    BrapiGermplasm g = germplasmList.get(i);
                    Map<Object, Object> aiMap = oMapper.convertValue(g, Map.class);

                    if (fCanQueryAttributes) {
                            Response<BrapiBaseResource<BrapiGermplasmAttributes>> response = service.getAttributes(aiMap.get(BrapiService.BRAPI_FIELD_germplasmDbId).toString()).execute();
                            if (response.code() != 404) {
                                    handleErrorCode(response.code());
                                    BrapiBaseResource<BrapiGermplasmAttributes> moreAttributes = response.body();
                                    moreAttributes.getResult().getData().forEach(k -> aiMap.put(k.getAttributeDbId(), k.getValue()));
                            }
                    }

                    // only keep fields whose values are a single-string
			LinkedHashMap<String, Comparable> remoteAI = new LinkedHashMap<>();
                    Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
			while (itr.hasNext()) {
                       Map.Entry<Object, Object> entry = itr.next();
			   if (entry.getValue() != null) {
				   Object val = entry.getValue() instanceof ArrayList && ((ArrayList) entry.getValue()).size() == 1 ? ((ArrayList) entry.getValue()).get(0) : entry.getValue();
				   if (val != null)
					   remoteAI.put(entry.getKey().toString(), val.toString());
                       }
                    }

                    progress.setCurrentStepProgress((long) (i * 100f / germplasmList.size()));

			if (remoteAI.isEmpty()) {
                            LOG.warn("Found no metadata to import for germplasm " + g.getGermplasmDbId());
                            continue;
                    }
			remoteAI.remove(BrapiService.BRAPI_FIELD_germplasmDbId); // we don't want to persist this field as it's internal to the remote source but not to the present system

                    Update update = new Update();            
                    if (username == null) {
		        remoteAI.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                    bulkOperations.updateMulti(new Query(Criteria.where("_id").is(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId()))), update);
                    }
			else if (!fIsAnonymous) {
		        remoteAI.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));		        
                            bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId()), username))), update);
                    }
			else if (session != null)
				sessionObject.get(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId())).putAll(remoteAI);
			else
				LOG.warn("Unable to save metadata for anonymous user (passed HttpSession was null)");
            }

            progress.addStep("Persisting metadata found at " + endpointUrl);
            progress.moveToNextStep();
		
            if (!fIsAnonymous) {
                BulkWriteResult wr = bulkOperations.execute();
                return wr.getModifiedCount() + wr.getUpserts().size();
            } else {
                LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
                return 1;
            }
    }
    
        private static int importBrapiV2Germplasm(
                String sModule, 
                HttpSession session, 
                String endpointUrl, 
                HashMap<String, String> germplasmDbIdToIndividualMap,
                String username,
                String authToken,
                ProgressIndicator progress,
                MongoTemplate mongoTemplate) throws Exception {

            HashMap<String, Object> reqBody = new HashMap<>();
            reqBody.put("germplasmDbIds", germplasmDbIdToIndividualMap.keySet());            
        
            BrapiV2Client client = new BrapiV2Client();

            // hack to try and make it work with current BMS version
            client.initService(endpointUrl/*.replace("Ricegigwa/", "")*/, authToken);
            client.getCalls();
            client.ensureGermplasmInfoCanBeImported();
            client.initService(endpointUrl, authToken);

            final BrapiV2Service service = client.getService();

            List<Germplasm> germplasmList = new ArrayList<>();
            ObjectMapper oMapper = new ObjectMapper();

            if (client.hasCallSearchGermplasm()) {
                    progress.addStep("Getting germplasm list from " + endpointUrl);
                    progress.moveToNextStep();

                    try {
                            Response<SuccessfulSearchResponse> response =  service.searchGermplasm(reqBody).execute();
                            handleErrorCode(response.code());
                            SuccessfulSearchResponseResult bsr = response.body().getResult();

                            BrapiV2Client.Pager germplasmPager = new BrapiV2Client.Pager();
                            while (germplasmPager.isPaging())
                            {
                                    GermplasmListResponse br = service.searchGermplasmResult(bsr.getSearchResultsDbId(), germplasmPager.getPageSize(), germplasmPager.getPage()).execute().body();
                                    germplasmList.addAll(br.getResult().getData());
                                    germplasmPager.paginate(br.getMetadata());
                            }
                    } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
                            try {
                                    Response<GermplasmListResponse> response = service.searchGermplasmDirectResult(reqBody).execute();
                                    handleErrorCode(response.code());
                                    GermplasmListResponse br = response.body();

                                    BrapiV2Client.Pager germplasmPager = new BrapiV2Client.Pager();
                                    while (germplasmPager.isPaging()) {
                                        germplasmList.addAll(br.getResult().getData());
                                        germplasmPager.paginate(br.getMetadata());
                                    }
                            }
                            catch(Exception f) {
                                    progress.setError("Error invoking BrAPI /search/germplasm call (no searchResultDbId returned and yet unable to directly obtain results)");
                                    LOG.error(e);
                                    LOG.error(progress.getError(), f);
                                    return 0;
                            }
                    }
            }



            boolean fCanQueryAttributes = client.hasCallSearchAttributes();
            progress.addStep("Getting germplasm information from " + endpointUrl);
            progress.moveToNextStep();

            if (germplasmList.isEmpty())
                    return 0;

            boolean fIsAnonymous = "anonymousUser".equals(username);
            LinkedHashMap<String /*individual*/, LinkedHashMap<String, Comparable>> sessionObject = (LinkedHashMap<String, LinkedHashMap<String, Comparable>>) session.getAttribute("individuals_metadata_" + sModule);
            if (fIsAnonymous  && sessionObject == null) {
                    sessionObject = new LinkedHashMap<>();
                    session.setAttribute("individuals_metadata_" + sModule, sessionObject);
            }

            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
            for (int i=0; i<germplasmList.size(); i++) {
                    Germplasm g = germplasmList.get(i);
                    Map<Object, Object> aiMap = oMapper.convertValue(g, Map.class);

                    if (fCanQueryAttributes) {
                            List<GermplasmAttributeValue> attributesList = new ArrayList<>();
                            HashMap<String, Object> body = new HashMap<>();
                            body.put("germplasmDbIds", Arrays.asList(aiMap.get(BrapiService.BRAPI_FIELD_germplasmDbId).toString()));  
                            try {
                                Response<SuccessfulSearchResponse> response = service.searchAttributes(body).execute();
                                handleErrorCode(response.code());
                                SuccessfulSearchResponseResult resultResp = response.body().getResult();

                                BrapiV2Client.Pager attributesPager = new BrapiV2Client.Pager();
                                while (attributesPager.isPaging())
                                {
                                        GermplasmAttributeValueListResponse attributesResp = service.searchAttributesResult(resultResp.getSearchResultsDbId(), attributesPager.getPageSize(), attributesPager.getPage()).execute().body();
                                        attributesList.addAll(attributesResp.getResult().getData());
                                        attributesPager.paginate(attributesResp.getMetadata());
                                }
                            } catch (Exception e1) {
                                try {
                                    Response<GermplasmAttributeValueListResponse> response = service.searchAttributesDirectResult(body).execute();
                                    handleErrorCode(response.code());
                                    GermplasmAttributeValueListResponse attributesResp = response.body();

                                    BrapiV2Client.Pager attributesPager = new BrapiV2Client.Pager();
                                    while (attributesPager.isPaging()) {
                                        attributesList.addAll(attributesResp.getResult().getData());
                                        attributesPager.paginate(attributesResp.getMetadata());
                                    }
                                } catch (Exception e2) {
                                    System.out.println("error");
                                    
                                } finally {
                                    if (!attributesList.isEmpty()) {
                                        attributesList.forEach(k -> aiMap.put(k.getAttributeDbId(), k.getValue()));
                                    }
                                }   
                            } finally {
                                if (!attributesList.isEmpty()) {
                                    attributesList.forEach(k -> aiMap.put(k.getAttributeDbId(), k.getValue()));
                                }
                            }
                            
                    }

                    // only keep fields whose values are a single-string
			LinkedHashMap<String, Comparable> remoteAI = new LinkedHashMap<>();
                    Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
			while (itr.hasNext()) {
                       Map.Entry<Object, Object> entry = itr.next();
			   if (entry.getValue() != null) {
				   Object val = entry.getValue() instanceof ArrayList && ((ArrayList) entry.getValue()).size() == 1 ? ((ArrayList) entry.getValue()).get(0) : entry.getValue();
				   if (val != null)
					   remoteAI.put(entry.getKey().toString(), val.toString());
                       }
                    }

                    progress.setCurrentStepProgress((long) (i * 100f / germplasmList.size()));

			if (remoteAI.isEmpty()) {
                            LOG.warn("Found no metadata to import for germplasm " + g.getGermplasmDbId());
                            continue;
                    }
			remoteAI.remove(BrapiService.BRAPI_FIELD_germplasmDbId); // we don't want to persist this field as it's internal to the remote source but not to the present system

                    Update update = new Update();            
                    if (username == null) {
		        remoteAI.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                    bulkOperations.updateMulti(new Query(Criteria.where("_id").is(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId()))), update);
                    }
			else if (!fIsAnonymous) {
		        remoteAI.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));		        
                            bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId()), username))), update);
                    }
			else if (session != null)
				sessionObject.get(germplasmDbIdToIndividualMap.get(g.getGermplasmDbId())).putAll(remoteAI);
			else
				LOG.warn("Unable to save metadata for anonymous user (passed HttpSession was null)");
            }

            progress.addStep("Persisting metadata found at " + endpointUrl);
            progress.moveToNextStep();
		
            if (!fIsAnonymous) {
                BulkWriteResult wr = bulkOperations.execute();
                return wr.getModifiedCount() + wr.getUpserts().size();
            } else {
                LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
                return 1;
            }
        }
        
        private static int importBrapiV1Samples(
            String sModule, 
            HttpSession session, 
            String endpointUrl, 
            HashMap<String, String> sampleDbIdToIndividualMap,
            String username,
            String authToken,
            ProgressIndicator progress,
            MongoTemplate mongoTemplate) throws Exception {
            progress.setError("Error invoking BrAPI /search/samples call (don't manage V1 samples call, please use brapi V2 url)");
            return 0;
            
    }
        
        private static int importBrapiV2Samples(
                String sModule, 
                HttpSession session, 
                String endpointUrl, 
                HashMap<String, String> sampleDbIdToIndividualMap,
                String username,
                String authToken,
                ProgressIndicator progress,
                MongoTemplate mongoTemplate) throws Exception {

                HashMap<String, Object> reqBody = new HashMap<>();
                reqBody.put("sampleDbIds", sampleDbIdToIndividualMap.keySet());            

                BrapiV2Client client = new BrapiV2Client();

                // hack to try and make it work with current BMS version
                client.initService(endpointUrl/*.replace("Ricegigwa/", "")*/, authToken);
                client.getCalls();
                client.ensureSampleInfoCanBeImported();
                client.initService(endpointUrl, authToken);

                final BrapiV2Service service = client.getService();

                List<Sample> sampleList = new ArrayList<>();
                ObjectMapper oMapper = new ObjectMapper();

                if (client.hasCallSearchSample()) {
                        progress.addStep("Getting samples list from " + endpointUrl);
                        progress.moveToNextStep();

                        try {
                                Response<SuccessfulSearchResponse> response =  service.searchSamples(reqBody).execute();
                                handleErrorCode(response.code());
                                SuccessfulSearchResponseResult bsr = response.body().getResult();

                                BrapiV2Client.Pager samplePager = new BrapiV2Client.Pager();
                                while (samplePager.isPaging())
                                {
                                        SampleListResponse br = service.searchSamplesResult(bsr.getSearchResultsDbId(), samplePager.getPageSize(), samplePager.getPage()).execute().body();
                                        sampleList.addAll(br.getResult().getData());
                                        samplePager.paginate(br.getMetadata());
                                }
                        } catch (Exception e) {    // we did not get a searchResultDbId: see if we actually got results straight away
                                try {
                                        Response<SampleListResponse> response = service.searchSamplesDirectResult(reqBody).execute();
                                        handleErrorCode(response.code());
                                        SampleListResponse br = response.body();

                                        BrapiV2Client.Pager samplePager = new BrapiV2Client.Pager();
                                        while (samplePager.isPaging()) {
                                            sampleList.addAll(br.getResult().getData());
                                            samplePager.paginate(br.getMetadata());
                                        }
                                }
                                catch(Exception f) {
                                        progress.setError("Error invoking BrAPI /search/samples call (no searchResultDbId returned and yet unable to directly obtain results)");
                                        LOG.error(e);
                                        LOG.error(progress.getError(), f);
                                        return 0;
                                }
                        }
                }



                progress.addStep("Getting samples information from " + endpointUrl);
                progress.moveToNextStep();

                if (sampleList.isEmpty())
                        return 0;

                boolean fIsAnonymous = "anonymousUser".equals(username);
                LinkedHashMap<String /*individual*/, LinkedHashMap<String, Comparable>> sessionObject = (LinkedHashMap<String, LinkedHashMap<String, Comparable>>) session.getAttribute("individuals_metadata_" + sModule);
                if (fIsAnonymous  && sessionObject == null) {
                        sessionObject = new LinkedHashMap<>();
                        session.setAttribute("individuals_metadata_" + sModule, sessionObject);
                }

                BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
                for (int i=0; i<sampleList.size(); i++) {
                        Sample sample = sampleList.get(i);
                        Map<Object, Object> aiMap = oMapper.convertValue(sample, Map.class);

                        // only keep fields whose values are a single-string
                        LinkedHashMap<String, Comparable> remoteAI = new LinkedHashMap<>();
                        Iterator<Map.Entry<Object, Object>> itr = aiMap.entrySet().iterator();
                            while (itr.hasNext()) {
                           Map.Entry<Object, Object> entry = itr.next();
                               if (entry.getValue() != null) {
                                       Object val = entry.getValue() instanceof ArrayList && ((ArrayList) entry.getValue()).size() == 1 ? ((ArrayList) entry.getValue()).get(0) : entry.getValue();
                                       if (val != null)
                                               remoteAI.put(entry.getKey().toString(), val.toString());
                           }
                        }

                        progress.setCurrentStepProgress((long) (i * 100f / sampleList.size()));

                            if (remoteAI.isEmpty()) {
                                LOG.warn("Found no metadata to import for germplasm " + sample.getSampleDbId());
                                continue;
                        }
                            remoteAI.remove(BrapiService.BRAPI_FIELD_germplasmDbId); // we don't want to persist this field as it's internal to the remote source but not to the present system

                        Update update = new Update();            
                        if (username == null) {
                            remoteAI.forEach((k, v) -> update.set(Individual.SECTION_ADDITIONAL_INFO + "." + k, v));
                            bulkOperations.updateMulti(new Query(Criteria.where("_id").is(sampleDbIdToIndividualMap.get(sample.getSampleDbId()))), update);
                        }
                            else if (!fIsAnonymous) {
                            remoteAI.forEach((k, v) -> update.set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO + "." + k, v));		        
                                bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(sampleDbIdToIndividualMap.get(sample.getSampleDbId()), username))), update);
                        }
                            else if (session != null)
                                    sessionObject.get(sampleDbIdToIndividualMap.get(sample.getSampleDbId())).putAll(remoteAI);
                            else
                                    LOG.warn("Unable to save metadata for anonymous user (passed HttpSession was null)");
                }

                progress.addStep("Persisting metadata found at " + endpointUrl);
                progress.moveToNextStep();

                if (!fIsAnonymous) {
                    BulkWriteResult wr = bulkOperations.execute();
                    return wr.getModifiedCount() + wr.getUpserts().size();
                } else {
                    LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
                    return 1;
                }
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
