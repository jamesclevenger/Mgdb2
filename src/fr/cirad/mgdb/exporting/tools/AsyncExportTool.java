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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import fr.cirad.mgdb.exporting.markeroriented.AbstractMarkerOrientedExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.ProgressIndicator;

public class AsyncExportTool {
	
	private static final Logger LOG = Logger.getLogger(AbstractMarkerOrientedExportHandler.class);
	
    /**
     * number of simultaneous query threads
     */
    public static final int INITIAL_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS = 5;
    static final private int MINIMUM_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS = 2;
    static final private int MAXIMUM_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS = 50;

	private DBCursor markerCursor;
	private MongoTemplate mongoTemplate;
	private int nQueryChunkSize;
	private List<GenotypingSample> samples;
	private ProgressIndicator progress;
	private AbstractDataOutputHandler<Integer, LinkedHashMap<VariantData, Collection<VariantRunData>>> dataOutputHandler;

	private int queryChunkIndex = 0, lastWrittenChunkIndex = -1;
	private AtomicInteger runningThreadCount = new AtomicInteger(0);
	private ConcurrentSkipListMap<Integer, LinkedHashMap<VariantData, Collection<VariantRunData>>> dataWritingQueue;
	private boolean fHasBeenLaunched = false;
	private boolean fReadingFinished = false;
	
	private int nNumberOfChunks;
	
	private int nNConcurrentThreads = INITIAL_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS;

	public AsyncExportTool(DBCursor markerCursor, int nTotalMarkerCount, int nQueryChunkSize, MongoTemplate mongoTemplate, List<GenotypingSample> samples, AbstractDataOutputHandler<Integer, LinkedHashMap<VariantData, Collection<VariantRunData>>> dataOutputHandler, ProgressIndicator progress) throws Exception
	{
		if (!markerCursor.hasNext())
			throw new Exception("markerCursor contains no data!");

		this.mongoTemplate = mongoTemplate;
		this.markerCursor = markerCursor;
		this.nQueryChunkSize = nQueryChunkSize;
		this.samples = samples;
		this.progress = progress;
		this.nNumberOfChunks = (int) Math.ceil((float) nTotalMarkerCount / nQueryChunkSize);
		this.dataWritingQueue = new ConcurrentSkipListMap<>();
		this.dataOutputHandler = dataOutputHandler;
	}

	public void launch() throws Exception
	{
		if (fHasBeenLaunched)
			throw new Exception("This AsyncExportTool has already been launched!");
		
		new Thread()
		{
			public void run()
			{
				launchWritingProcess();
			}
		}.start();

		while (!fReadingFinished)
		{
			Thread.sleep(20);
			if (runningThreadCount.get() < nNConcurrentThreads && dataWritingQueue.size() < nNConcurrentThreads)
			{
				Thread t = new Thread()
				{
					public void run()
					{
						tryAndGetNextChunks();
					}
				};
				if (!fHasBeenLaunched)
					t.run();	// launch first chunk synchronously otherwise it may launch too many ones at first
				else
					t.start();
			}
		}
	}
	
	synchronized protected void tryAndGetNextChunks()
	{
		if ((progress != null && progress.isAborted()) || !markerCursor.hasNext())
		{
			fReadingFinished = true;
			return;
		}

		fHasBeenLaunched = true;
		
		int nLoadedMarkerCountInLoop = 0;
		boolean fStartingNewChunk = true;
		List<Object> currentMarkers = new ArrayList<>();
		while (markerCursor.hasNext() && (fStartingNewChunk || nLoadedMarkerCountInLoop%nQueryChunkSize != 0))
		{
			DBObject exportVariant = markerCursor.next();
			currentMarkers.add((String) exportVariant.get("_id"));
			nLoadedMarkerCountInLoop++;
			fStartingNewChunk = false;
		}

		final int nFinalChunkIndex = queryChunkIndex++;
		Thread t = new Thread()
		{
			public void run()
			{
				try
				{
					runningThreadCount.addAndGet(1);
					dataWritingQueue.put(nFinalChunkIndex, MgdbDao.getSampleGenotypes(mongoTemplate, samples, currentMarkers, true, null /*new Sort(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ChromosomalPosition.FIELDNAME_SEQUENCE).and(new Sort(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ChromosomalPosition.FIELDNAME_START_SITE))*/));
					runningThreadCount.addAndGet(-1);
				}
				catch (Exception e)
				{
					LOG.error("Error reading asynchronously genotypes for export", e);
					progress.setError("Error reading asynchronously genotypes for export: " + e);
				}

		        if (nFinalChunkIndex % nNConcurrentThreads == (nNConcurrentThreads - 1)) {
		            if (runningThreadCount.get() > nNConcurrentThreads * .3)
		            	nNConcurrentThreads = (int) (nNConcurrentThreads / 1.5);
		            else if (runningThreadCount.get() < nNConcurrentThreads * .5)
		            	nNConcurrentThreads *= 2;
		            nNConcurrentThreads = Math.min(MAXIMUM_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS, Math.max(MINIMUM_NUMBER_OF_SIMULTANEOUS_QUERY_THREADS, nNConcurrentThreads));
		        }
				
				if (runningThreadCount.get() < nNConcurrentThreads && dataWritingQueue.size() < nNConcurrentThreads)
					tryAndGetNextChunks();
			}
		};
		t.start();
	}
	
	static public abstract class AbstractDataOutputHandler<T, V> implements Callable<Void> {
	    protected V variantDataChunkMap;
	    
	    public void setVariantDataArray(V variantDataArray) {
	        this.variantDataChunkMap = variantDataArray;
	    }

	    public abstract Void call();
	}
	
	protected void launchWritingProcess()
	{
		try
		{
			while ((progress == null || !progress.isAborted()) && (nNumberOfChunks > lastWrittenChunkIndex + 1))
			{
				while (!dataWritingQueue.containsKey(lastWrittenChunkIndex + 1))
					Thread.sleep(50);

				dataOutputHandler.setVariantDataArray(dataWritingQueue.get(lastWrittenChunkIndex + 1));
				dataOutputHandler.call();	// invoke data writing
				dataWritingQueue.remove(++lastWrittenChunkIndex);
				progress.setCurrentStepProgress((lastWrittenChunkIndex + 1) * 100 / nNumberOfChunks);
			}
		}
		catch (Throwable t)
		{
			LOG.error("Error writing export output", t);
			progress.setError("Error writing export output: " + t.getMessage());
		}
	}
}
