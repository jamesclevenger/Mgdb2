package jhi.brapi.api.germplasm;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class BrapiGermplasmAttributes {
	
	static public class Attribute {
	    private String attributeCode;
	    private String attributeDbId;
	    private String attributeName;
	    private String determinedDate;
	    private String value;
		public String getAttributeCode() {
			return attributeCode;
		}
		public void setAttributeCode(String attributeCode) {
			this.attributeCode = attributeCode;
		}
		public String getAttributeDbId() {
			return attributeDbId;
		}
		public void setAttributeDbId(String attributeDbId) {
			this.attributeDbId = attributeDbId;
		}
		public String getAttributeName() {
			return attributeName;
		}
		public void setAttributeName(String attributeName) {
			this.attributeName = attributeName;
		}
		public String getDeterminedDate() {
			return determinedDate;
		}
		public void setDeterminedDate(String determinedDate) {
			this.determinedDate = determinedDate;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
	}
	
    private String germplasmDbId;
    private List<Attribute> data;
    

    public String getGermplasmDbId() {
        return this.germplasmDbId;
    }

    public void setGermplasmDbId(String germplasmDbId) {
        this.germplasmDbId = germplasmDbId;
    }

	public List<Attribute> getData() {
		return data;
	}

	public void setData(List<Attribute> data) {
		this.data = data;
	}
}

