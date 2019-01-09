/*******************************************************************************
 * MGDB - Mongo Genotype DataBase
 * Copyright (C) 2016, 2018, <CIRAD> <IRD>
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
import org.bson.types.ObjectId;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.WriteResult;

import fr.cirad.mgdb.importing.base.AbstractGenotypeImport;
import fr.cirad.mgdb.model.mongo.maintypes.AutoIncrementCounter;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader;
import fr.cirad.mgdb.model.mongo.maintypes.DBVCFHeader.VcfHeaderId;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.FeatureReader;
import htsjdk.variant.bcf2.BCF2Codec;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

/**
 * The Class VcfImport.
 */
public class VcfImport extends AbstractGenotypeImport {

    /**
     * The Constant LOG.
     */
    private static final Logger LOG = Logger.getLogger(VariantData.class);

    public static final String ANNOTATION_FIELDNAME_EFF = "EFF";
    public static final String ANNOTATION_FIELDNAME_ANN = "ANN";
    public static final String ANNOTATION_FIELDNAME_CSQ = "CSQ";
    
    /**
     * The m_process id.
     */
    private String m_processID;

    /**
     * Instantiates a new vcf import.
     */
    public VcfImport() {
        this("random_process_" + System.currentTimeMillis() + "_" + Math.random());
    }

    /**
     * Instantiates a new vcf import.
     *
     * @param processID the process id
     */
    public VcfImport(String processID) {
        m_processID = processID;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            throw new Exception("You must pass 5 parameters as arguments: DATASOURCE name, PROJECT name, RUN name, TECHNOLOGY string, and VCF file! Two optionals parameters: 6th supports values '1' (empty project data before importing) and '2' (empty all variant data before importing, including marker list)");
        }

        File mainFile = new File(args[4]);
        if (!mainFile.exists() || mainFile.length() == 0) {
            throw new Exception("File " + args[4] + " is missing or empty!");
        }

        int mode = 0;
        try {
            mode = Integer.parseInt(args[5]);
        } catch (Exception e) {
            LOG.warn("Unable to parse input mode. Using default (0): overwrite run if exists.");
        }
        new VcfImport().importToMongo(args[4].toLowerCase().endsWith(".bcf"), args[0], args[1], args[2], args[3], new File(args[4]).toURI().toURL(), mode);        
    }

    /**
     * Import to mongo.
     *
     * @param fIsBCF whether or not it is a bcf
     * @param sModule the module
     * @param sProject the project
     * @param sRun the run
     * @param sTechnology the technology
     * @param mainFileUrl the main file URL
     * @param importMode the import mode
     * @return a project ID if it was created by this method, otherwise null
     * @throws Exception the exception
     */
    public Integer importToMongo(boolean fIsBCF, String sModule, String sProject, String sRun, String sTechnology, URL mainFileUrl, int importMode) throws Exception {
        long before = System.currentTimeMillis();
        ProgressIndicator progress = ProgressIndicator.get(m_processID);
        if (progress == null)
            progress = new ProgressIndicator(m_processID, new String[]{"Initializing import"});	// better to add it straight-away so the JSP doesn't get null in return when it checks for it (otherwise it will assume the process has ended)
        progress.setPercentageEnabled(false);

        FeatureReader<VariantContext> reader;

        if (fIsBCF) {
            BCF2Codec bc = new BCF2Codec();
            reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), bc, false);
        } else {
            VCFCodec vc = new VCFCodec();
            reader = AbstractFeatureReader.getFeatureReader(mainFileUrl.toString(), vc, false);
        }
        // not compatible java 1.8 ? 
        // FeatureReader<VariantContext> reader = AbstractFeatureReader.getFeatureReader(mainFilePath, fIsBCF ? new BCF2Codec() : new VCFCodec(), false);
        GenericXmlApplicationContext ctx = null;
        try {
            MongoTemplate mongoTemplate = MongoTemplateManager.get(sModule);
            if (mongoTemplate == null) {	// we are probably being invoked offline
                try {
                    ctx = new GenericXmlApplicationContext("applicationContext-data.xml");
                } catch (BeanDefinitionStoreException fnfe) {
                    LOG.warn("Unable to find applicationContext-data.xml. Now looking for applicationContext.xml", fnfe);
                    ctx = new GenericXmlApplicationContext("applicationContext.xml");
                }

                MongoTemplateManager.initialize(ctx);
                mongoTemplate = MongoTemplateManager.get(sModule);
                if (mongoTemplate == null) {
                    throw new Exception("DATASOURCE '" + sModule + "' is not supported!");
                }
            }

            if (m_processID == null) {
                m_processID = "IMPORT__" + sModule + "__" + sProject + "__" + sRun + "__" + System.currentTimeMillis();
            }

            mongoTemplate.getDb().command(new BasicDBObject("profile", 0));	// disable profiling
            GenotypingProject project = mongoTemplate.findOne(new Query(Criteria.where(GenotypingProject.FIELDNAME_NAME).is(sProject)), GenotypingProject.class);
            
            Iterator<VariantContext> variantIterator = reader.iterator();
            int nPloidy = 0, i = 0;
            while (variantIterator.hasNext() && i++ < 100 && nPloidy == 0)
            {
            	VariantContext vcfEntry = variantIterator.next();
	            if (vcfEntry.getCommonInfo().getAttribute("CNV") == null)
	            {
	            	nPloidy = vcfEntry.getMaxPloidy(0);
	            	LOG.info("Found ploidy level of " + nPloidy + " for " + vcfEntry.getType() + " variant " + vcfEntry.getChr() + ":" + vcfEntry.getStart());
	            	break;
	            }
            }
            if (importMode == 0 && project != null && project.getPloidyLevel() != nPloidy)
            	throw new Exception("Ploidy levels differ between existing (" + project.getPloidyLevel() + ") and provided (" + nPloidy + ") data!");
            
            currentlyImportedProjects.put(sModule, sProject);

            if (importMode == 2)
                mongoTemplate.getDb().dropDatabase(); // drop database before importing
            else if (project != null)
            {
				if (importMode == 1 || (project.getRuns().size() == 1 && project.getRuns().get(0).equals(sRun)))
				{	// empty project data before importing
					WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId())), DBVCFHeader.class);
					LOG.info(wr.getN() + " records removed from vcf_header");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId())), VariantRunData.class);
					LOG.info(wr.getN() + " records removed from variantRunData");
					wr = mongoTemplate.remove(new Query(Criteria.where("_id").is(project.getId())), GenotypingProject.class);
					project.clearEverythingExceptMetaData();
				}
				else
				{	// empty run data before importing
                    WriteResult wr = mongoTemplate.remove(new Query(Criteria.where("_id." + VcfHeaderId.FIELDNAME_PROJECT).is(project.getId()).and("_id." + VcfHeaderId.FIELDNAME_RUN).is(sRun)), DBVCFHeader.class);
                    LOG.info(wr.getN() + " records removed from vcf_header");
                    if (project.getRuns().contains(sRun))
                    {
                    	LOG.info("Cleaning up existing run's data");
	                    List<Criteria> crits = new ArrayList<>();
	                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_PROJECT_ID).is(project.getId()));
	                    crits.add(Criteria.where("_id." + VariantRunData.VariantRunDataId.FIELDNAME_RUNNAME).is(sRun));
	                    crits.add(Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES).exists(true));
	                    wr = mongoTemplate.remove(new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()]))), VariantRunData.class);
	                    LOG.info(wr.getN() + " records removed from variantRunData");
                    }
                }
                if (mongoTemplate.count(null, VariantRunData.class) == 0 && doesDatabaseSupportImportingUnknownVariants(sModule))
                {	// if there is no genotyping data left and we are not working on a fixed list of variants then any other data is irrelevant
                    mongoTemplate.getDb().dropDatabase();
                }
            }

            VCFHeader header = (VCFHeader) reader.getHeader();
            int effectAnnotationPos = -1, geneIdAnnotationPos = -1;
            for (VCFInfoHeaderLine headerLine : header.getInfoHeaderLines()) {
                if (ANNOTATION_FIELDNAME_EFF.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_ANN.equals(headerLine.getID()) || ANNOTATION_FIELDNAME_CSQ.equals(headerLine.getID())) {
                    String desc = headerLine.getDescription().replaceAll("\\(", "").replaceAll("\\)", "");
                    desc = desc.substring(1 + desc.indexOf(":")).replace("'", "");
                    String[] fields = desc.split("\\|");
                    for (i = 0; i<fields.length; i++) {
                        String trimmedField = fields[i].trim();
                        if (/*EFF*/ "Gene_Name".equals(trimmedField) || /*EFF*/ "Gene_ID".equals(trimmedField) || /*CSQ or ANN*/ "Gene".equals(trimmedField)) {
                            geneIdAnnotationPos = i;
                            if (effectAnnotationPos != -1) {
                                break;
                            }
                        } else if (/*EFF*/ "Annotation".equals(trimmedField) || /*CSQ or ANN*/ "Consequence".equals(trimmedField)) {
                            effectAnnotationPos = i;
                            if (geneIdAnnotationPos != -1) {
                                break;
                            }
                        }
                    }
                }
            }

            Integer createdProject = null;
            // create project if necessary
            if (project == null || importMode == 2) {	// create it
                project = new GenotypingProject(AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingProject.class)));
                project.setName(sProject);
                project.setOrigin(2 /* Sequencing */);
                project.setTechnology(sTechnology);
                createdProject = project.getId();
            }
            project.setPloidyLevel(nPloidy);

            mongoTemplate.save(new DBVCFHeader(new VcfHeaderId(project.getId(), sRun), header));

            String info = "Header was written for project " + sProject + " and run " + sRun;
            LOG.info(info);
            progress.addStep(info);
            progress.moveToNextStep();

            HashMap<String, String> existingVariantIDs = buildSynonymToIdMapForExistingVariants(mongoTemplate, false);

            int nNumberOfVariantsToSaveAtOnce = 1;
            HashMap<String /*individual*/, GenotypingSample> previouslyCreatedSamples = new HashMap<String /*individual*/, GenotypingSample>();
            HashMap<String /*individual*/, Comparable> phasingGroups = new HashMap<String /*individual*/, Comparable>();
            variantIterator = reader.iterator();
            progress.addStep("Processing variant lines");
            progress.moveToNextStep();
            
            final MongoTemplate finalMongoTemplate = mongoTemplate;
            
            List<VariantData> unsavedVariants = new ArrayList<>();
            List<VariantRunData> unsavedRuns = new ArrayList<>();
            
            // loop over each variation
            long count = 0;
            String generatedIdBaseString = Long.toHexString(System.currentTimeMillis());
            int nMaxChunkSize = existingVariantIDs.size() == 0 ? 10000 /*saved one by one*/ : 50000 /*inserted at once*/;
            Thread asyncThread = null;
            while (variantIterator.hasNext()) {
                VariantContext vcfEntry = variantIterator.next();
                if (!vcfEntry.isVariant())
                    continue; // skip non-variant positions				

                if (vcfEntry.getCommonInfo().hasAttribute(""))
                	vcfEntry.getCommonInfo().removeAttribute("");	// working around cases where the info field accidentally ends with a semicolon
                
                try
                {
                	String variantId = null;
					for (String variantDescForPos : getIdentificationStrings(vcfEntry.getType().toString(), vcfEntry.getChr(), (long) vcfEntry.getStart(), Arrays.asList(new String[] {vcfEntry.getID()})))
					{
						variantId = existingVariantIDs.get(variantDescForPos);
						if (variantId != null)
							break;
					}
                    VariantData variant = variantId == null ? null : mongoTemplate.findById(variantId, VariantData.class);
                    if (variant == null)
                		variant = new VariantData(vcfEntry.hasID() ? ((ObjectId.isValid(vcfEntry.getID()) ? "_" : "") + vcfEntry.getID()) : (generatedIdBaseString + String.format(String.format("%09x", count))));
                    
                    unsavedVariants.add(variant);
                    VariantRunData runToSave = addVcfDataToVariant(mongoTemplate, header, variant, vcfEntry, project, sRun, phasingGroups, previouslyCreatedSamples, effectAnnotationPos, geneIdAnnotationPos);
                    if (!unsavedRuns.contains(runToSave)) {
                        unsavedRuns.add(runToSave);
                    }

                    if (count == 0) {
                        nNumberOfVariantsToSaveAtOnce = vcfEntry.getSampleNames().isEmpty() ? nMaxChunkSize : Math.max(1, nMaxChunkSize / vcfEntry.getSampleNames().size());
                        LOG.info("Importing by chunks of size " + nNumberOfVariantsToSaveAtOnce);
                    }
                    if (count % nNumberOfVariantsToSaveAtOnce == 0) {
                        List<VariantData> finalUnsavedVariants = unsavedVariants;
                        List<VariantRunData> finalUnsavedRuns = unsavedRuns;
                        
	                    Thread insertionThread = new Thread() {
	                        @Override
	                        public void run() {
	                            if (existingVariantIDs.size() == 0) {	// we benefit from the fact that it's the first variant import into this database to use bulk insert which is much faster
	                            	finalMongoTemplate.insert(finalUnsavedVariants, VariantData.class);
	                                finalMongoTemplate.insert(finalUnsavedRuns, VariantRunData.class);
	                            }
	                            else {
	                                for (VariantData vd : finalUnsavedVariants)
	                                	finalMongoTemplate.save(vd);
	                                for (VariantRunData run : finalUnsavedRuns)
	                                	finalMongoTemplate.save(run);
	                            }    
	                        }
	                    };

	                    if (asyncThread == null)
                        {	// every second insert is run asynchronously for better speed
//                        	System.out.println("async");
                        	asyncThread = insertionThread;
                        	asyncThread.start();
                        }
                        else
                        {
//                        	System.out.println("sync");
                        	insertionThread.run();
                        	asyncThread.join();	// make sure previous thread has executed before going further
                        	asyncThread = null;
                        }
                        
	                    unsavedVariants = new ArrayList<>();
	                    unsavedRuns = new ArrayList<>();
	                    
	                    progress.setCurrentStepProgress(count);
	                    if (count > 0 && count % (nNumberOfVariantsToSaveAtOnce*10) == 0) {
	                        info = count + " lines processed"/*"(" + (System.currentTimeMillis() - before) / 1000 + ")\t"*/;
	                        LOG.debug(info);
	                    }
                    }

                    project.getAlleleCounts().add(vcfEntry.getAlleles().size());	// it's a Set so it will only be added if it's not already present
                    project.getVariantTypes().add(vcfEntry.getType().toString());	// it's a Set so it will only be added if it's not already present 
                    project.getSequences().add(vcfEntry.getChr());	// it's a Set so it will only be added if it's not already present

                    count++;
                }
                catch (Exception e) 
                {
                    throw new Exception("Error occured importing variant number " + (count + 1) + " (" + vcfEntry.getType().toString() + ":" + vcfEntry.getChr() + ":" + vcfEntry.getStart() + ")", e);
                }
            }
            reader.close();

            if (existingVariantIDs.size() == 0) {	// we benefit from the fact that it's the first variant import into this database and use batch insert which is ways faster than looping on save 
                mongoTemplate.insert(unsavedVariants, VariantData.class);
                mongoTemplate.insert(unsavedRuns, VariantRunData.class);
            }
            else {
                for (VariantData vd : unsavedVariants)
                    mongoTemplate.save(vd);
                for (VariantRunData run : unsavedRuns)
                    mongoTemplate.save(run);
            }

            // save project data
            if (!project.getRuns().contains(sRun)) {
                project.getRuns().add(sRun);
            }
            mongoTemplate.save(project);
            mongoTemplate.insert(previouslyCreatedSamples.values(), GenotypingSample.class);

            LOG.info("VariantImport took " + (System.currentTimeMillis() - before) / 1000 + "s for " + count + " records");

            progress.addStep("Preparing database for searches");
            progress.moveToNextStep();
            MgdbDao.prepareDatabaseForSearches(mongoTemplate);
            progress.markAsComplete();
            return createdProject;
        }
        finally
        {
        	currentlyImportedProjects.remove(sModule);

            if (ctx != null)
                ctx.close();

            reader.close();
        }
    }

    /**
     * Adds the vcf data to variant.
     *
     * @param mongoTemplate the mongo template
     * @param header the VCF Header
     * @param variantToFeed the variant to feed
     * @param vc the VariantContext
     * @param project the project
     * @param runName the run name
     * @param phasingGroup the phasing group
     * @param usedSamples the used samples
     * @param effectAnnotationPos the effect annotation pos
     * @param geneIdAnnotationPos the gene name annotation pos
     * @return the variant run data
     * @throws Exception the exception
     */
    static private VariantRunData addVcfDataToVariant(MongoTemplate mongoTemplate, VCFHeader header, VariantData variantToFeed, VariantContext vc, GenotypingProject project, String runName, HashMap<String /*individual*/, Comparable> phasingGroup, Map<String /*individual*/, GenotypingSample> usedSamples, int effectAnnotationPos, int geneIdAnnotationPos) throws Exception
    {
        // mandatory fields
        if (variantToFeed.getType() == null) {
            variantToFeed.setType(vc.getType().toString());
        } else if (!variantToFeed.getType().equals(vc.getType().toString())) {
            throw new Exception("Variant type mismatch between existing data and data to import: " + variantToFeed.getId());
        }

        List<String> knownAlleleList = new ArrayList<String>();
        if (variantToFeed.getKnownAlleleList().size() > 0)
            knownAlleleList.addAll(variantToFeed.getKnownAlleleList());
        ArrayList<String> allelesInVC = new ArrayList<String>();
        allelesInVC.add(vc.getReference().getBaseString());
        for (Allele alt : vc.getAlternateAlleles())
            allelesInVC.add(alt.getBaseString());
        for (String vcAllele : allelesInVC)
            if (!knownAlleleList.contains(vcAllele))
                knownAlleleList.add(vcAllele);
        variantToFeed.setKnownAlleleList(knownAlleleList);

        if (variantToFeed.getReferencePosition() == null) // otherwise we leave it as it is (had some trouble with overridden end-sites)
            variantToFeed.setReferencePosition(new ReferencePosition(vc.getContig(), vc.getStart(), (long) vc.getEnd()));

        VariantRunData run = new VariantRunData(new VariantRunData.VariantRunDataId(project.getId(), runName, variantToFeed.getId()));

        // main VCF fields that are stored as additional info in the DB
        if (vc.isFullyDecoded())
            run.getAdditionalInfo().put(VariantData.FIELD_FULLYDECODED, true);
        if (vc.hasLog10PError())
        	run.getAdditionalInfo().put(VariantData.FIELD_PHREDSCALEDQUAL, vc.getPhredScaledQual());
        if (!VariantData.FIELDVAL_SOURCE_MISSING.equals(vc.getSource()))
            run.getAdditionalInfo().put(VariantData.FIELD_SOURCE, vc.getSource());
        if (vc.filtersWereApplied())
            run.getAdditionalInfo().put(VariantData.FIELD_FILTERS, vc.getFilters().size() > 0 ? Helper.arrayToCsv(",", vc.getFilters()) : VCFConstants.PASSES_FILTERS_v4);

        List<String> aiEffect = new ArrayList<String>(), aiGene = new ArrayList<String>();

        // actual VCF info fields
        Map<String, Object> attributes = vc.getAttributes();
        for (String key : attributes.keySet()) {
            if (geneIdAnnotationPos != -1 && (ANNOTATION_FIELDNAME_EFF.equals(key) || ANNOTATION_FIELDNAME_ANN.equals(key) || ANNOTATION_FIELDNAME_CSQ.equals(key))) {
                Object effectAttr = vc.getAttribute(key);
                List<String> effectList = effectAttr instanceof String ? Arrays.asList((String) effectAttr) : (List<String>) vc.getAttribute(key);
                for (String effect : effectList) {
                    for (String effectDesc : effect.split(",")) {
                        String sEffect = null;
                        int parenthesisPos = !ANNOTATION_FIELDNAME_EFF.equals(key) ? -1 /* parenthesis can also be used in ANN or CSQ, but differently */ : effectDesc.indexOf("(");
                        List<String> fields = Helper.split(effectDesc.substring(parenthesisPos + 1).replaceAll("\\)", ""), "|");
                        if (parenthesisPos > 0)
                            sEffect = effectDesc.substring(0, parenthesisPos);	// snpEff version < 4.1
                        else if (effectAnnotationPos != -1)
                            sEffect = fields.get(effectAnnotationPos);
                        if (sEffect != null)
                            aiEffect.add(sEffect);
                        aiGene.add(fields.get(geneIdAnnotationPos));
                    }
                }
                run.getAdditionalInfo().put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_GENE, aiGene);
                run.getAdditionalInfo().put(VariantRunData.FIELDNAME_ADDITIONAL_INFO_EFFECT_NAME, aiEffect);
                for (String variantEffectAnnotation : aiEffect) {
                    if (variantEffectAnnotation != null && !project.getEffectAnnotations().contains(variantEffectAnnotation)) {
                        project.getEffectAnnotations().add(variantEffectAnnotation);
                    }
                }
            }

            Object attrVal = vc.getAttribute(key);
            if (attrVal instanceof ArrayList) {
                run.getAdditionalInfo().put(key, Helper.arrayToCsv(",", (ArrayList) attrVal));
            } else if (attrVal != null) {
                if (attrVal instanceof Boolean && ((Boolean) attrVal).booleanValue()) {
                    run.getAdditionalInfo().put(key, (Boolean) attrVal);
                } else {
                    try {
                        int intVal = Integer.valueOf(attrVal.toString());
                        run.getAdditionalInfo().put(key, intVal);
                    } catch (NumberFormatException nfe1) {
                        try {
                            double doubleVal = Double.valueOf(attrVal.toString());
                            run.getAdditionalInfo().put(key, doubleVal);
                        } catch (NumberFormatException nfe2) {
                            run.getAdditionalInfo().put(key, attrVal.toString());
                        }
                    }
                }
            }
        }
        
        // genotype fields
        Iterator<Genotype> genotypes = vc.getGenotypesOrderedByName().iterator();
        while (genotypes.hasNext()) {
            Genotype genotype = genotypes.next();

            boolean isPhased = genotype.isPhased();
            String sIndividual = genotype.getSampleName();

            if (!usedSamples.containsKey(sIndividual)) // we don't want to persist each sample several times
            {
                Individual ind = mongoTemplate.findById(sIndividual, Individual.class);
                if (ind == null) {	// we don't have any population data so we don't need to update the Individual if it already exists
                    ind = new Individual(sIndividual);
                    mongoTemplate.save(ind);
                }

            	int sampleId = AutoIncrementCounter.getNextSequence(mongoTemplate, MongoTemplateManager.getMongoCollectionName(GenotypingSample.class));
                usedSamples.put(sIndividual, new GenotypingSample(sampleId, project.getId(), run.getRunName(), sIndividual));	// add a sample for this individual to the project
            }

            Comparable phasedGroup = phasingGroup.get(sIndividual);
            if (phasedGroup == null || (!isPhased && !genotype.isNoCall()))
                phasingGroup.put(sIndividual, variantToFeed.getId());
            
            List<String> gtAllelesAsStrings = genotype.getAlleles().stream().map(allele -> allele.getBaseString()).collect(Collectors.toList());
            
            String gtCode = VariantData.rebuildVcfFormatGenotype(knownAlleleList, gtAllelesAsStrings, isPhased, false);
            if (gtCode == null)
            	continue;

            if ("1/0".equals(gtCode))
            	gtCode = "0/1";	// convert to "0/1" so that MAF queries can work reliably

            SampleGenotype aGT = new SampleGenotype(gtCode);
            if (isPhased) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_GT, VariantData.rebuildVcfFormatGenotype(knownAlleleList, gtAllelesAsStrings, isPhased, true));
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PHASED_ID, phasingGroup.get(sIndividual));
            }
            if (genotype.hasGQ()) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_GQ, genotype.getGQ());
            }
            if (genotype.hasDP()) {
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_DP, genotype.getDP());
            }
            boolean fSkipPlFix = false;	// for performance
            if (genotype.hasAD()) {
            	int[] adArray = genotype.getAD(), originalAdArray = adArray;
            	adArray = VariantData.fixAdFieldValue(adArray, vc.getAlleles(), knownAlleleList);
            	if (originalAdArray == adArray)
            		fSkipPlFix = true;	// if AD was correct then PL is too
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_AD, Helper.arrayToCsv(",", adArray));
            }
            if (genotype.hasPL()) {
            	int[] plArray = genotype.getPL();
            	if (!fSkipPlFix)
            		plArray = VariantData.fixPlFieldValue(plArray, genotype.getPloidy(), vc.getAlleles(), knownAlleleList);
                aGT.getAdditionalInfo().put(VariantData.GT_FIELD_PL, Helper.arrayToCsv(",", plArray));
            }
            Map<String, Object> extendedAttributes = genotype.getExtendedAttributes();
            for (String sAttrName : extendedAttributes.keySet()) {
            	VCFFormatHeaderLine formatHeaderLine = header.getFormatHeaderLine(sAttrName);
            	if (formatHeaderLine != null) {
	            	boolean fConvertToNumber = (formatHeaderLine.getType().equals(VCFHeaderLineType.Integer) || formatHeaderLine.getType().equals(VCFHeaderLineType.Float)) && formatHeaderLine.isFixedCount() && formatHeaderLine.getCount() == 1;
	            	String value = extendedAttributes.get(sAttrName).toString();
	            	Object correctlyTypedValue = fConvertToNumber ? Float.parseFloat(value) : value;
	            	if (fConvertToNumber && !formatHeaderLine.getType().equals(VCFHeaderLineType.Float))
	            		correctlyTypedValue = Math.round((float) correctlyTypedValue);
	                aGT.getAdditionalInfo().put(sAttrName, correctlyTypedValue);
            	}
            }

            if (genotype.isFiltered()) {
                aGT.getAdditionalInfo().put(VariantData.FIELD_FILTERS, genotype.getFilters());
            }

            run.getSampleGenotypes().put(usedSamples.get(sIndividual).getId(), aGT);
        }
        
        run.setKnownAlleleList(variantToFeed.getKnownAlleleList());
        run.setReferencePosition(variantToFeed.getReferencePosition());
        run.setType(variantToFeed.getType());
        return run;
    }

//    public static void printGenotypes(int j, int k, String genotype)
//    {
//       if (genotype.length()==k)
//       {
//           LOG.info("genotype " + genotype + " has length " + k);
//       }
//       else
//       {
//           for (int a=0; a<j; ++a)
//           {
//               String s = "" + (char)(a+65);
//               s += genotype;
//               printGenotypes(a+1, k, s);
//           }
//       }
//    }
}