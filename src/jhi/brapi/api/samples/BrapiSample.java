/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jhi.brapi.api.samples;

import java.util.HashMap;

/**
 *
 * @author boizet
 */
public class BrapiSample {
    static public final HashMap<String, String> germplasmFields = new HashMap<>();
    private String germplasmDbId;
    private String notes;
    private String observationUnitDbId;
    private String plantDbId;
    private String plateDbId;
    private String plateIndex;
    private String plotDbId;
    private String sampleDbId;
    private String sampleTimestamp;
    private String sampleType;
    private String studyDbId;
    private String takenBy;
    private String tissueType;
    
    static {
    	germplasmFields.put("germplasmdbid", "germplasmDbId");
    	germplasmFields.put("notes", "notes");
    	germplasmFields.put("observationUnitDbId", "observationUnitDbId");
    	germplasmFields.put("plantDbId", "plantDbId");
    	germplasmFields.put("plateDbId", "plateDbId");
    	germplasmFields.put("plateIndex", "plateIndex");
    	germplasmFields.put("plotDbId", "plotDbId");
    	germplasmFields.put("sampleDbId", "sampleDbId");
    	germplasmFields.put("sampleTimestamp", "sampleTimestamp");
    	germplasmFields.put("sampleType", "sampleType");
    	germplasmFields.put("studyDbId", "studyDbId");
    	germplasmFields.put("takenBy", "takenBy");
    	germplasmFields.put("tissueType", "tissueType");
    }     

    public String getGermplasmDbId() {
        return germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public String getObservationUnitDbId() {
        return observationUnitDbId;
    }

    public void setObservationUnitDbId(String observationUnitDbId) {
        this.observationUnitDbId = observationUnitDbId;
    }

    public String getPlantDbId() {
        return plantDbId;
    }

    public void setPlantDbId(String plantDbId) {
        this.plantDbId = plantDbId;
    }

    public String getPlateDbId() {
        return plateDbId;
    }

    public void setPlateDbId(String plateDbId) {
        this.plateDbId = plateDbId;
    }

    public String getPlateIndex() {
        return plateIndex;
    }

    public void setPlateIndex(String plateIndex) {
        this.plateIndex = plateIndex;
    }

    public String getPlotDbId() {
        return plotDbId;
    }

    public void setPlotDbId(String plotDbId) {
        this.plotDbId = plotDbId;
    }

    public String getSampleDbId() {
        return sampleDbId;
    }

    public void setSampleDbId(String sampleDbId) {
        this.sampleDbId = sampleDbId;
    }

    public String getSampleTimestamp() {
        return sampleTimestamp;
    }

    public void setSampleTimestamp(String sampleTimestamp) {
        this.sampleTimestamp = sampleTimestamp;
    }

    public String getSampleType() {
        return sampleType;
    }

    public void setSampleType(String sampleType) {
        this.sampleType = sampleType;
    }

    public String getStudyDbId() {
        return studyDbId;
    }

    public void setStudyDbId(String studyDbId) {
        this.studyDbId = studyDbId;
    }

    public String getTakenBy() {
        return takenBy;
    }

    public void setTakenBy(String takenBy) {
        this.takenBy = takenBy;
    }

    public String getTissueType() {
        return tissueType;
    }

    public void setTissueType(String tissueType) {
        this.tissueType = tissueType;
    }
    
    
}
