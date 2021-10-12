/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fr.cirad.mgdb.importing;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.brapi.v2.model.Germplasm;
import org.brapi.v2.model.GermplasmNewRequestSynonyms;
import org.brapi.v2.model.TaxonID;

/**
 *
 * @author boizet
 */
public class GermplasmSerializer extends StdSerializer<Germplasm> {
    
    public GermplasmSerializer() {
        this(null);
    }
  
    public GermplasmSerializer(Class<Germplasm> t) {
        super(t);
    }

    @Override
    public void serialize(Germplasm germplasm, JsonGenerator jgen, SerializerProvider provider) 
      throws IOException, JsonProcessingException {
 
        jgen.writeStartObject();        

        jgen.writeStringField("germplasmDbId", germplasm.getGermplasmDbId());
        
        if (germplasm.getAccessionNumber() != null) {
            jgen.writeStringField("accessionNumber", germplasm.getAccessionNumber());
        }
        
        if (germplasm.getAcquisitionDate() != null) {
            jgen.writeStringField("acquisitionDate", germplasm.getAcquisitionDate());
        }
        
        if (germplasm.getBiologicalStatusOfAccessionCode() != null) {
            jgen.writeStringField("biologicalStatusOfAccessionCode", germplasm.getBiologicalStatusOfAccessionCode().toString());
        }
        
        if (germplasm.getBiologicalStatusOfAccessionDescription() != null) {
            jgen.writeStringField("biologicalStatusOfAccessionDescription", germplasm.getBiologicalStatusOfAccessionDescription());
        }
        
        if (germplasm.getBreedingMethodDbId() != null) {
            jgen.writeStringField("breedingMethodDbId", germplasm.getBreedingMethodDbId());
        }
        
        if (germplasm.getCollection() != null) {
            jgen.writeStringField("collection", germplasm.getCollection());
        }
        
        if (germplasm.getCommonCropName() != null) {
            jgen.writeStringField("commonCropName", germplasm.getCommonCropName());
        }        
        
        if (germplasm.getCountryOfOriginCode() != null) {
            jgen.writeStringField("countryOfOriginCode", germplasm.getCountryOfOriginCode());
        }
        
        if (germplasm.getDefaultDisplayName() != null) {
            jgen.writeStringField("defaultDisplayName", germplasm.getDefaultDisplayName());
        }
        
        if (germplasm.getDocumentationURL()!= null) {
            jgen.writeStringField("documentationURL", germplasm.getDocumentationURL());
        }
        
        if (germplasm.getGenus() != null) {
            jgen.writeStringField("genus", germplasm.getGenus());
        }
        
        if (germplasm.getGermplasmName() != null) {
            jgen.writeStringField("germplasmName", germplasm.getGermplasmName());
        }
        
        if (germplasm.getGermplasmPUI() != null) {
            jgen.writeStringField("germplasmPUI", germplasm.getGermplasmPUI());
        }
        
        if (germplasm.getInstituteCode() != null) {
            jgen.writeStringField("instituteCode", germplasm.getInstituteCode());
        }
        
        if (germplasm.getInstituteName() != null) {
            jgen.writeStringField("instituteName", germplasm.getInstituteName());
        }
        
        if (germplasm.getPedigree() != null) {
            jgen.writeStringField("pedigree", germplasm.getPedigree());
        }
        
        if (germplasm.getSeedSource() != null) {
            jgen.writeStringField("seedSource", germplasm.getSeedSource());
        }
        
        if (germplasm.getSeedSourceDescription() != null) {
            jgen.writeStringField("seedSourceDescription", germplasm.getSeedSourceDescription());
        }
        
        if (germplasm.getSpecies() != null) {
            jgen.writeStringField("species", germplasm.getSpecies());
        }
        
        if (germplasm.getSpeciesAuthority() != null) {
            jgen.writeStringField("speciesAuthority", germplasm.getSpeciesAuthority());
        }
        
        if (germplasm.getSubtaxa()!= null) {
            jgen.writeStringField("subtaxa", germplasm.getSubtaxa());
        }
        
        if (germplasm.getSubtaxaAuthority() != null) {
            jgen.writeStringField("subtaxaAuthority", germplasm.getSubtaxaAuthority());
        }
        
        if (germplasm.getSynonyms() != null) {
            for (GermplasmNewRequestSynonyms syn:germplasm.getSynonyms()) {
                jgen.writeStringField("synonym_" + syn.getType(), syn.getSynonym());
            }
        }
        
        if (germplasm.getTaxonIds() != null) {
            for (TaxonID taxon:germplasm.getTaxonIds()) {
                jgen.writeStringField("taxonId_" + taxon.getSourceName(), taxon.getTaxonId());
            }
        }
        
        if (germplasm.getAdditionalInfo() != null) {
            for (String key:germplasm.getAdditionalInfo().keySet()) {
                jgen.writeStringField(key, germplasm.getAdditionalInfo().get(key));
            }
        }
        
        jgen.writeEndObject();
    }
}