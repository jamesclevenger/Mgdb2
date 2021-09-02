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
package fr.cirad.mgdb.exporting.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.IntKeyMapPropertyCodecProvider;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import fr.cirad.mgdb.exporting.AbstractExportWritingThread;
import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.AbstractVariantData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;

/**
 * The class ExportManager.
 */
public class ExportManager
{
	
	/** The Constant LOG. */
	static final Logger LOG = Logger.getLogger(ExportManager.class);
	
	static public final AlphaNumericComparator<String> alphaNumericStringComparator = new AlphaNumericComparator<String>();
	
	static public final Comparator<VariantRunData> vrdComparator = new Comparator<VariantRunData>() {
		@Override
		public int compare(VariantRunData vrd1, VariantRunData vrd2) {
			if (vrd1.getReferencePosition() == null) {
				if (vrd2.getReferencePosition() == null)
					return vrd1.getId().getVariantId().compareTo(vrd2.getId().getVariantId()); 	// none is positioned
				return -1;	// only vrd2 is positioned
			}
			if (vrd2.getReferencePosition() == null)
				return 1;	// only vrd1 is positioned
			
			// both are positioned
			int chrComparison = alphaNumericStringComparator.compare(vrd1.getReferencePosition().getSequence(), vrd2.getReferencePosition().getSequence());
			return chrComparison != 0 ? chrComparison : (int) (vrd1.getReferencePosition().getStartSite() - vrd2.getReferencePosition().getStartSite());
		}
	};
	
	@SuppressWarnings("rawtypes")
	private MongoCursor markerCursor;
	
	private ProgressIndicator progress;
	
	private int nQueryChunkSize;
	
	private boolean fWorkingOnTempColl;
	
	private FileWriter warningFileWriter;
	
	private List<BasicDBObject> pipeline = new ArrayList<>();
	
	private MongoTemplate mongoTemplate;
	
	private AbstractExportWritingThread writingThread;
	
	private Long markerCount;

	public static final CodecRegistry pojoCodecRegistry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), CodecRegistries.fromProviders(PojoCodecProvider.builder().register(new IntKeyMapPropertyCodecProvider()).automatic(true).build()));
	
	public ExportManager(MongoTemplate mongoTemplate, MongoCollection<Document> varColl, Class resultType, Document varQuery, List<GenotypingSample> samplesToExport, boolean fIncludeMetadata, int nQueryChunkSize, AbstractExportWritingThread writingThread, long markerCount, FileWriter warningFileWriter, ProgressIndicator progress) {
		this.progress = progress;
		this.nQueryChunkSize = nQueryChunkSize;
		this.warningFileWriter = warningFileWriter;
		this.mongoTemplate = mongoTemplate;
		this.writingThread = writingThread;
		this.markerCount = markerCount;

		String varCollName = varColl.getNamespace().getCollectionName();
		fWorkingOnTempColl = varCollName.startsWith(MongoTemplateManager.TEMP_COLL_PREFIX);

		if (!varQuery.isEmpty())
			pipeline.add(new BasicDBObject("$match", varQuery));

		String refPosPath = AbstractVariantData.FIELDNAME_REFERENCE_POSITION;
		BasicDBObject sortStage = new BasicDBObject("$sort", new Document(refPosPath  + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1).append(refPosPath + "." + ReferencePosition.FIELDNAME_START_SITE, 1));
		pipeline.add(sortStage);

		Document projection = new Document();
		projection.append(refPosPath, 1);
		projection.append(AbstractVariantData.FIELDNAME_KNOWN_ALLELE_LIST, 1);
		projection.append(AbstractVariantData.FIELDNAME_TYPE, 1);
		projection.append(AbstractVariantData.FIELDNAME_SYNONYMS, 1);
		projection.append(AbstractVariantData.FIELDNAME_ANALYSIS_METHODS, 1);
		if (fIncludeMetadata)
			projection.append(AbstractVariantData.SECTION_ADDITIONAL_INFO, 1);

		if (samplesToExport != null && !samplesToExport.isEmpty()) {	// project sample-level fields
			TreeSet<String> annotationFields = new TreeSet<>();
			if (fIncludeMetadata)
				for (GenotypingSample sample : samplesToExport.stream().filter(Helper.distinctByKey(GenotypingSample::getProjectId)).collect(Collectors.toList()))
					annotationFields.addAll(MgdbDao.getAnnotationFields(mongoTemplate, sample.getProjectId(), false));
	
			for (GenotypingSample sp : samplesToExport)
				projection.append(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + sp.getId(), 1);
		}
		pipeline.add(new BasicDBObject("$project", projection));
		LOG.debug("Export pipeline: " + pipeline);
		
//    	long before = System.currentTimeMillis();
    	markerCursor = varColl.aggregate(fWorkingOnTempColl ? Arrays.asList(sortStage, new BasicDBObject("$project", new BasicDBObject("_id", 1))) : pipeline, fWorkingOnTempColl ? Document.class : resultType).collation(IExportHandler.collationObj).allowDiskUse(true).batchSize(nQueryChunkSize).iterator();	/*FIXME: didn't find a way to set noCursorTimeOut on aggregation cursors*/
//		System.err.println("cursor obtained in " + (System.currentTimeMillis() - before) + "ms");
	}
	
    public void readAndWrite() throws IOException, InterruptedException, ExecutionException {		
		if (fWorkingOnTempColl)
			exportFromTempColl();
		else
			exportDirectlyFromRuns();
    }
    
    /**
     * Exports by $match-ing successive chunks of variant IDs in VariantRunData. Would have thought using $lookup with a single cursor would be faster, but it's much slower
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws ExecutionException
     */
	private void exportFromTempColl() throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Void> future = null;
		List<List<VariantRunData>> tempMarkerRunsToWrite = new ArrayList<>(nQueryChunkSize);
		List<VariantRunData> currentMarkerRuns = new ArrayList<>();
		List<String> currentMarkerIDs = new ArrayList<>();
		String varId = null, previousVarId = null;
		int nWrittenmarkerCount = 0;
		
		MongoCollection<VariantRunData> runColl = mongoTemplate.getDb().withCodecRegistry(ExportManager.pojoCodecRegistry).getCollection(mongoTemplate.getCollectionName(VariantRunData.class), VariantRunData.class);
		
		while (markerCursor.hasNext()) {
            if (progress.isAborted() || progress.getError() != null ) {
            	if (warningFileWriter != null)
            		warningFileWriter.close();
			    return;
            }
            
			currentMarkerIDs.add(((Document) markerCursor.next()).getString("_id"));
			
			if (currentMarkerIDs.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
				BasicDBObject initialMatchStage = new BasicDBObject("$match", new BasicDBObject("_id." + VariantRunDataId.FIELDNAME_VARIANT_ID, new BasicDBObject("$in", currentMarkerIDs)));
				if (varId == null)
					pipeline.add(0, initialMatchStage);	// first time we set this stage, it's an insertion
				else
					pipeline.set(0, initialMatchStage);	// replace existing $match

				ArrayList<VariantRunData> runs = runColl.aggregate(pipeline, VariantRunData.class).allowDiskUse(true).into(new ArrayList<>()); // we don't use collation here because it leads to unexpected behaviour (sometimes fetches some additional variants to those in currentMarkerIDs) => we'll have to sort each chunk by hand
				Collections.sort(runs, vrdComparator);	// make sure variants within this chunk are correctly sorted
				
				for (VariantRunData vrd : runs) {
					varId = vrd.getId().getVariantId();
					
					if (previousVarId != null && !varId.equals(previousVarId)) {
						tempMarkerRunsToWrite.add(currentMarkerRuns);
						currentMarkerRuns = new ArrayList<>();
						nWrittenmarkerCount++;
					}
					currentMarkerRuns.add(vrd);
					previousVarId = varId;
				}
				
				if (!markerCursor.hasNext())
					tempMarkerRunsToWrite.add(currentMarkerRuns);	// special case, when the end of the cursor is being reached
				currentMarkerIDs.clear();

				if (future != null && !future.isDone()) {
//					long b4 = System.currentTimeMillis();
					future.get();
//					long delay = System.currentTimeMillis() - b4;
//					if (delay > 100)
//						LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
				}
				
				if (markerCount != null)
					progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
				future = writingThread.writeRuns(tempMarkerRunsToWrite);
				tempMarkerRunsToWrite.clear();
			}
		}

		if (future != null && !future.isDone())
			future.get();
		if (markerCount != null)
			progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
	}


	private void exportDirectlyFromRuns() throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Void> future = null;
		List<List<VariantRunData>> tempMarkerRunsToWrite = new ArrayList<>(nQueryChunkSize);
		List<VariantRunData> currentMarkerRuns = new ArrayList<>();
		String varId = null, previousVarId = null;
		int nWrittenmarkerCount = 0;
		
		while (markerCursor.hasNext()) {
            if (progress.isAborted() || progress.getError() != null ) {
            	if (warningFileWriter != null)
            		warningFileWriter.close();
			    return;
            }
            
			VariantRunData vrd = (VariantRunData) markerCursor.next();
			varId = vrd.getId().getVariantId();

			if (previousVarId != null && !varId.equals(previousVarId)) {
				tempMarkerRunsToWrite.add(currentMarkerRuns);
				currentMarkerRuns = new ArrayList<>();
				nWrittenmarkerCount++;
			}

			currentMarkerRuns.add(vrd);

			if (!markerCursor.hasNext())
				tempMarkerRunsToWrite.add(currentMarkerRuns);	// special case, when the end of the cursor is being reached

			if (tempMarkerRunsToWrite.size() >= nQueryChunkSize || !markerCursor.hasNext()) {
				if (future != null && !future.isDone()) {
//					long b4 = System.currentTimeMillis();
					future.get();
//					long delay = System.currentTimeMillis() - b4;
//					if (delay > 100) {
//						LOG.debug(progress.getProcessId() + " waited " + delay + "ms before writing variant " + nWrittenmarkerCount/* + ", increasing nQueryChunkSize from " + nQueryChunkSize + " to " + nQueryChunkSize*2*/);
				}

				if (markerCount != null && markerCount > 0)
					progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
				future = writingThread.writeRuns(tempMarkerRunsToWrite);
				tempMarkerRunsToWrite.clear();
			}
			previousVarId = varId;
		}

		if (future != null && !future.isDone())
			future.get();
		if (markerCount != null && markerCount > 0)
			progress.setCurrentStepProgress(nWrittenmarkerCount * 100l / markerCount);
	}
}