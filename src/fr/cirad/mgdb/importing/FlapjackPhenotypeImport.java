/** *****************************************************************************
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
 * *****************************************************************************
 */
package fr.cirad.mgdb.importing;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

import com.mongodb.bulk.BulkWriteResult;

import fr.cirad.mgdb.model.mongo.maintypes.CustomIndividualMetadata;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The Class IndividualMetadataImport.
 */
public class FlapjackPhenotypeImport {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(FlapjackPhenotypeImport.class);   
    

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            throw new Exception("You must pass 2 or 3 parameters as arguments: DATASOURCE name, metadata file path (TSV format with header on first line), and optionally a CSV list of column labels for fields to import (all will be imported if no such parameter is supplied).");
        }
        importIndividualMetadata(args[0], null, new File(args[1]).toURI().toURL(), args.length > 2 ? args[2] : null, null);
    }

    public static int importIndividualMetadata(String sModule, HttpSession session, URL metadataFileURL, String csvFieldListToImport, String username) throws Exception {
        List<String> fieldsToImport = csvFieldListToImport != null ? Arrays.asList(csvFieldListToImport.toLowerCase().split(",")) : null;
        Scanner scanner = new Scanner(metadataFileURL.openStream());
        GenericXmlApplicationContext ctx = null;
        
        try {
	        MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
	        if (mongoTemplate == null) { // we are probably being invoked offline
	            ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
	
	            MongoTemplateManager.initialize(ctx);
	            mongoTemplate = MongoTemplateManager.get(sModule);
	            if (mongoTemplate == null) {
	                throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
	            }
	        }
	
	        boolean fIsAnonymous = "anonymousUser".equals(username);
	        LinkedHashMap<String /*individual*/, LinkedHashMap<String, Comparable>> sessionObject = null;	// start with empty metadata (we only aggregate when we import BrAPI stuff over manually-provided values)
	        if (fIsAnonymous) {
	            sessionObject = new LinkedHashMap<>();
	            session.setAttribute("individuals_metadata_" + sModule, sessionObject);
	        }
        
            HashMap<Integer, String> columnLabels = new HashMap<Integer, String>();
            String sLine = null;
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, username == null ? Individual.class : CustomIndividualMetadata.class);
            List<String> passedIndList = new ArrayList<>();
            while (scanner.hasNextLine()) {
                sLine = scanner.nextLine();
                if (sLine.trim().length() == 0 || sLine.startsWith("#")) {
                    continue;
                }

                if (columnLabels.isEmpty() && sLine.startsWith("\uFEFF")) {
                    sLine = sLine.substring(1);
                }

                String[] cells = sLine.split("\\s+");

                // No individual id : header line
                if (cells[0].trim().length() == 0) {
                	// Header line, with field names
                	if (columnLabels.isEmpty()) {
                        for (int i = 1; i < cells.length; i++) {  // Skip the first (empty) cell
                            String cell = cells[i];
                            if (fieldsToImport == null || fieldsToImport.contains(cell.toLowerCase())) {
                                columnLabels.put(i, cell);
                            }
                        }
                        if (cells.length <= 1) {
                            throw new Exception("Provided file does not seem to be tab or space-delimited");
                        }
                    }
                	
                	// Otherwise it's a trait metadata row (like experiment information), skip it. FIXME ?
                	else {
                		LOG.warn("Skipped a trait metadata row");
                	}
                	
                	continue;
                }

                // now deal with actual data rows
                LinkedHashMap<String, Comparable> additionalInfo = new LinkedHashMap<>();
                for (int col : columnLabels.keySet()) {
                    if (col != 0) {
                        additionalInfo.put(columnLabels.get(col), cells.length > col ? cells[col] : "");
                    }
                }

                String individualId = cells[0];
                passedIndList.add(individualId);
                if (username == null) {
                    bulkOperations.updateMulti(new Query(Criteria.where("_id").is(individualId)), new Update().set(Individual.SECTION_ADDITIONAL_INFO, additionalInfo));
                } else if (!fIsAnonymous) {
                    bulkOperations.upsert(new Query(Criteria.where("_id").is(new CustomIndividualMetadata.CustomIndividualMetadataId(individualId, username))), new Update().set(CustomIndividualMetadata.SECTION_ADDITIONAL_INFO, additionalInfo));
                } else
                    sessionObject.put(individualId, additionalInfo);
            }

            if (passedIndList.size() == 0) {
                if (username == null) {
                    bulkOperations.updateMulti(new Query(), new Update().unset(Individual.SECTION_ADDITIONAL_INFO)); // a blank metadata file was submitted: let's delete any existing metadata				
                } else {
                    bulkOperations.remove(new Query(Criteria.where("_id." + CustomIndividualMetadata.CustomIndividualMetadataId.FIELDNAME_USER).is(username)));
                }
            } else {	// first check if any passed individuals are unknown
                Query verificationQuery = new Query(Criteria.where("_id").in(passedIndList));
                verificationQuery.fields().include("_id");
                List<String> foundIndList = mongoTemplate.find(verificationQuery, Individual.class).stream().map(ind -> ind.getId()).collect(Collectors.toList());
                if (foundIndList.size() < passedIndList.size()) {
                    throw new Exception("The following individuals do not exist in the selected database: " + StringUtils.join(CollectionUtils.disjunction(passedIndList, foundIndList), ", "));
                }
            }
            if (!fIsAnonymous) {
                BulkWriteResult wr = bulkOperations.execute();
                if (passedIndList.size() == 0) {
                    LOG.info("Database " + sModule + ": metadata was deleted for " + wr.getModifiedCount() + " individuals");
                } else {
                    LOG.info("Database " + sModule + ": " + wr.getModifiedCount() + " individuals updated with metadata, out of " + wr.getMatchedCount() + " matched documents");
                }
                return wr.getModifiedCount() + wr.getUpserts().size() + wr.getDeletedCount();
            } else {
                LOG.info("Database " + sModule + ": metadata was persisted into session for anonymous user");
                return 1;
            }
        } finally {
            scanner.close();
            if (ctx != null) {
                ctx.close();
            }
        }
    }
}
