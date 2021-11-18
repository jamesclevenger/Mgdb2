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
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapCodec;
import org.broadinstitute.gatk.utils.codecs.hapmap.RawHapMapFeature;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext.Type;

/**
 * The Class HapMapImport.
 */
public class HapMapImport extends AbstractGenotypeImport {

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(VariantData.class);
	
    public boolean m_fCloseContextOpenAfterImport = false;

	/** The m_process id. */
	private String m_processID;

	private static HashMap<String, String> iupacCodeConversionMap = new HashMap<>();

	static
	{
//		iupacCodeConversionMap.put("A", "AA");
//		iupacCodeConversionMap.put("C", "CC");
//		iupacCodeConversionMap.put("G", "GG");
//		iupacCodeConversionMap.put("T", "TT");
		iupacCodeConversionMap.put("U", "TT");
		iupacCodeConversionMap.put("R", "AG");
		iupacCodeConversionMap.put("Y", "CT");
		iupacCodeConversionMap.put("S", "GC");
		iupacCodeConversionMap.put("W", "AT");
		iupacCodeConversionMap.put("K", "GT");
		iupacCodeConversionMap.put("M", "AC");
		iupacCodeConversionMap.put("N", "NN");
	}
	
	/**
	 * Instantiates a new hap map import.
	 */
	public HapMapImport()
	{
	}

	/**
	 * Instantiates a new hap map import.
	 *
	 * @param processID the process id
	 */
	public HapMapImport(String processID)
	{
		m_processID = processID;
	}
	
	/**
     * Instantiates a new hap map import.
     */
    public HapMapImport(boolean fCloseContextOpenAfterImport) {
        this();
    	m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }

    /**
     * Instantiates a new hap map import.
     */
    public HapMapImport(String processID, boolean fCloseContextOpenAfterImport) {
        this(processID);
    	m_fCloseContextOpenAfterImport = fCloseContextOpenAfterImport;
    }


	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 5)
			throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, and HapMap file! An optional 6th parameter supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list).");

		File mainFile = new File(args[4]);
		if (!mainFile.exists() || mainFile.length() == 0)
			throw new Exception("File " + args[4] + " is missing or empty!");

		int mode = 0;
		try
		{
			mode = Integer.parseInt(args[5]);
		}
		catch (Exception e)
		{
			LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
		}
		new HapMapImport().importToMongo(args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), false, mode);
	}

	/**
	 * Import to mongo.
	 *
	 * @param sModule the module
	 * @param sProject the project
	 * @param sRun the run
	 * @param sTechnology the technology
	 * @param mainFileUrl the main file URL
     * @param fSkipMonomorphic whether or not to skip import of variants that have no polymorphism (where all individuals have the same genotype)
	 * @param importMode the import mode
	 * @return a project ID if it was created by this method, otherwise null
	 * @throws Exception the exception
	 */
	public Integer importToMongo(String sModule, String sProject, String sRun, String sTechnology, URL mainFileUrl, boolean fSkipMonomorphic, int importMode) throws Exception
	{
		long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID) != null ? ProgressIndicator.get(m_processID) : new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
		progress.setPercentageEnabled(false);		

		FeatureReader<RawHapMapFeature> reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), new RawHapMapCodec(), false);
		GenericXmlApplicationContext ctx = null;
		try
		{
			MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
			if (mongoTemplate == null)
			{	// we are probably being invoked offline
				try
				{
					ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
				}
				catch (BeanDefinitionStoreException fnfe)
				{
					LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
					ctx = new GenericXmlApplicationContext("applicationContext.xml");
				}

				MongoTemplateManager.initialize(ctx);
				mongoTemplate = MongoTemplateManager.get(sModule);
				if (mongoTemplate == null)
					throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
			}

			if (m_processID == null)
				m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();

			GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            if (importMode == 0 && project != null && project.getPloidyLevel() != 2)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + 2 + ") data!");

            cleanupBeforeImport(mongoTemplate, sModule, project, importMode, sRun);

			Integer createdProject = null;
			// create project if necessary
			if (project == null || importMode == 2)
			{	// create it
				project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
				project.setName(sProject);
				project.setOrigin(2 /* Sequencing */);
				project.setTechnology(sTechnology);
				createdProject = project.getId();
			}

			HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);		
			if (!project.getVariantTypes().contains(Type.SNP.toString()))
				project.getVariantTypes().add(Type.SNP.toString());

			int count = 0;
			String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
			int nNumberOfVariantsToSaveAtOnce = 1;
			ArrayList<VariantData> unsavedVariants = new ArrayList<VariantData>();
			ArrayList<VariantRunData> unsavedRuns = new ArrayList<VariantRunData>();
			HashMap<String /*individual*/, GenotypingSample> previouslyCreatedSamples = new HashMap<>();
			String[] sampleIds = null;
			progress.addStep("Processing variant lines");
			progress.moveToNextStep();

            final ArrayList<Thread> threadsToWaitFor = new ArrayList<>();
            int chunkIndex = 0, nNConcurrentThreads = Math.max(1, Runtime.getRuntime().availableProcessors());
            LOG.debug("Importing project '" + sProject + "' into " + sModule + " using " + nNConcurrentThreads + " threads");
            
            Iterator<RawHapMapFeature> it = reader.iterator();
            count = 0;
			while (it.hasNext()) {   // loop over each variation
				if (progress.getError() != null || progress.isAborted())
					return null;

				RawHapMapFeature hmFeature = it.next();
				if (sampleIds == null)
				    sampleIds = hmFeature.getSampleIDs();

				try
				{
					String variantId = null;
					for (String variantDescForPos : getIdentificationStrings(Type.SNP.toString(), hmFeature.getChr(), (long) hmFeature.getStart(), hmFeature.getName().length() == 0 ? null : Arrays.asList(new String[] {hmFeature.getName()})))
					{
						variantId = existingVariantIDs.get(variantDescForPos);
						if (variantId != null)
							break;
					}

					if (variantId == null && fSkipMonomorphic && Arrays.asList(hmFeature.getGenotypes()).stream().filter(gt -> !"NA".equals(gt) && !"NN".equals(gt)).distinct().count() < 2)
	                    continue; // skip non-variant positions that are not already known

					VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
					if (variant == null)
						variant = new VariantData(hmFeature.getName() != null && hmFeature.getName().length() > 0 ? ((ObjectId.isValid(hmFeature.getName()) ? "_" : "") + hmFeature.getName()) : (generatedIdBaseString + String.format(String.format("%09x", count))));

					VariantRunData runToSave = addHapMapDataToVariant(mongoTemplate, variant, hmFeature, project, sRun, previouslyCreatedSamples, sampleIds);

					if (!project.getSequences().contains(hmFeature.getChr()))
						project.getSequences().add(hmFeature.getChr());

					project.getAlleleCounts().add(variant.getKnownAlleleList().size());	// it's a TreeSet so it will only be added if it's not already present
					
					if (variant.getKnownAlleleList().size() > 0)
					{	// we only import data related to a variant if we know its alleles
						if (!unsavedVariants.contains(variant))
							unsavedVariants.add(variant);
						if (!unsavedRuns.contains(runToSave))
							unsavedRuns.add(runToSave);
					}

					if (count == 0) {
						nNumberOfVariantsToSaveAtOnce = sampleIds.length == 0 ? nMaxChunkSize : Math.max(1, nMaxChunkSize / sampleIds.length);
						LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
					}
					else if (count % nNumberOfVariantsToSaveAtOnce == 0) {
						saveChunk(unsavedVariants, unsavedRuns, existingVariantIDs, mongoTemplate, progress, nNumberOfVariantsToSaveAtOnce, count, null, threadsToWaitFor, nNConcurrentThreads, chunkIndex++);
				        unsavedVariants = new ArrayList<>();
				        unsavedRuns = new ArrayList<>();
					}

					count++;
				}
				catch (Exception e)
				{
					throw new Exception("Error occured importing variant number " + (count + 1) + " (" + Type.SNP.toString() + ":" + hmFeature.getChr() + ":" + hmFeature.getStart() + ")", e);
				}
			}
			reader.close();

			persistVariantsAndGenotypes(!existingVariantIDs.isEmpty(), mongoTemplate, unsavedVariants, unsavedRuns);
            for (Thread t : threadsToWaitFor) // wait for all threads before moving to next phase
           		t.join();

			// save project data
			if (!project.getRuns().contains(sRun))
				project.getRuns().add(sRun);
			mongoTemplate.save(project);	// always save project before samples otherwise the sample cleaning procedure in MgdbDao.prepareDatabaseForSearches may remove them if called in the meantime
            mongoTemplate.insert(previouslyCreatedSamples.values(), GenotypingSample.class);

			progress.addStep("Preparing database for searches");
			progress.moveToNextStep();
			MgdbDao.prepareDatabaseForSearches(mongoTemplate);

			LOG.info("HapMapImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");
			progress.markAsComplete();
			return createdProject;
		}
		finally
		{
			if (m_fCloseContextOpenAfterImport && ctx != null)
				ctx.close();

			reader.close();
		}
	}

	/**
	 * Adds the hap map data to variant.
	 *
	 * @param mongoTemplate the mongo template
	 * @param variantToFeed the variant to feed
	 * @param hmFeature the hm feature
	 * @param project the project
	 * @param runName the run name
	 * @param usedSamples the used samples
	 * @return the variant run data
	 * @throws Exception the exception
	 */
	static private VariantRunData addHapMapDataToVariant(MongoTemplate mongoTemplate, VariantData variantToFeed, RawHapMapFeature hmFeature, GenotypingProject project, String runName, Map<String /*individual*/, GenotypingSample> usedSamples, String[] individuals) throws Exception
	{
	    // genotype fields
        HashMap<String, Integer> alleleIndexMap = new HashMap<>();  // should be more efficient not to call indexOf too often...
        List<Allele> knownAlleles = new ArrayList<>();
        for (String allele : hmFeature.getAlleles()) {
            int alleleIndexMapSize = alleleIndexMap.size();
            alleleIndexMap.put(allele, alleleIndexMapSize);
            knownAlleles.add(Allele.create(allele, alleleIndexMapSize == 0));
        }
        Type variantType = determineType(knownAlleles);

		// mandatory fields
		if (variantToFeed.getType() == null)
			variantToFeed.setType(variantType.toString());
		else if (!variantToFeed.getType().equals(variantType.toString()))
			throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());

		if (variantToFeed.getReferencePosition() == null)	// otherwise we leave it as it is (had some trouble with overridden end-sites)
			variantToFeed.setReferencePosition(new ReferencePosition(hmFeature.getChr(), hmFeature.getStart(), (long) hmFeature.getEnd()));
		
		// take into account ref and alt alleles (if it's not too late)
		if (variantToFeed.getKnownAlleleList().size() == 0)
			variantToFeed.setKnownAlleleList(Arrays.asList(hmFeature.getAlleles()));

		VariantRunData vrd = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));
		
        if (project.getPloidyLevel() == 0)
            project.setPloidyLevel(findCurrentVariantPloidy(hmFeature, knownAlleles));

		for (int i=0; i<hmFeature.getGenotypes().length; i++)
		{
			String genotype = hmFeature.getGenotypes()[i].toUpperCase(), gtCode = null;
			if ("NA".equals(genotype))
				genotype = "NN";
            else if (genotype.length() == 1) {
                String gtForIupacCode = iupacCodeConversionMap.get(genotype);
                if (gtForIupacCode != null)
                    genotype = gtForIupacCode;    // it's a IUPAC code, let's convert it to a pair of bases
            }
			
			boolean fGenotypeContainsSeparator = genotype.contains("/");

			if (!"NN".equals(genotype)) {
				List<String> alleles = null;
				if (fGenotypeContainsSeparator)
				    alleles = Helper.split(genotype, "/");
				else if (alleleIndexMap.containsKey(genotype)) {    // must be a collapsed homozygous
				    alleles = new ArrayList<>();
				    for (int j=0; j<project.getPloidyLevel(); j++)  // expand it
				        alleles.add(genotype);
				}
                else if (variantType.equals(Type.SNP))
                    alleles = Arrays.asList(genotype.split(""));
				
				if (alleles == null || alleles.isEmpty())
                    LOG.warn("Ignoring invalid genotype \"" + genotype + "\" for variant " + variantToFeed.getId() + " and individual " + individuals[i] + (project.getPloidyLevel() == 0 ? ". No ploidy determined at this stage, unable to expand homozygous genotype" : ""));
				else
				    gtCode = alleles.stream().map(allele -> alleleIndexMap.get(allele)).sorted().map(index -> index.toString()).collect(Collectors.joining("/"));
			}

			if (gtCode == null)
				continue;	// we don't add missing genotypes

			SampleGenotype aGT = new SampleGenotype(gtCode);
			GenotypingSample sample = usedSamples.get(individuals[i]);
			if (sample == null)	// we don't want to persist each sample several times
			{
                Individual ind = mongoTemplate.findById(individuals[i], Individual.class);
                if (ind == null) {	// we don't have any population data so we don't need to update the Individual if it already exists
                    ind = new Individual(individuals[i]);
                    mongoTemplate.save(ind);
                }

                int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                sample = new GenotypingSample(sampleId, project.getId(), vrd.getRunName(), individuals[i]);
                usedSamples.put(individuals[i], sample);	// add a sample for this individual to the project
            }			

			vrd.getSampleGenotypes().put(sample.getId(), aGT);
		}
		
        vrd.setKnownAlleleList(variantToFeed.getKnownAlleleList());
        vrd.setReferencePosition(variantToFeed.getReferencePosition());
        vrd.setType(variantToFeed.getType());
        vrd.setSynonyms(variantToFeed.getSynonyms());
		return vrd;
	}

    private static int findCurrentVariantPloidy(RawHapMapFeature hmFeature, List<Allele> knownAlleles) {
        for (String genotype : hmFeature.getGenotypes()) {
            if (genotype.contains("/"))
                return genotype.split("/").length;
            if (determineType(knownAlleles).equals(Type.SNP))
                return genotype.length();
        }
        return 0;
    }
}