package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.HashMap;
import java.util.LinkedHashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "customSampleMetadata")
@TypeAlias("SM")
public class CustomSampleMetadata {
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	static public class CustomSampleMetadataId {
                
                /** The Constant FIELDNAME_SAMPLE_NAME. */
		public final static String FIELDNAME_SAMPLE_NAME= "nm";
		
		/** The Constant FIELDNAME_USER. */
		public final static String FIELDNAME_USER = "ur";
                
                /** The sample name. */
		@Field(FIELDNAME_SAMPLE_NAME)
		private String name;

		/** The user name. */
		@Field(FIELDNAME_USER)
		private String user;


		/**
		 * Instantiates a new custom individual metadata id.
		 *
		 * @param individual the individual id
		 * @param user the user's name
		 */
		public CustomSampleMetadataId(String name, String user) {
                        this.name = name;
			this.user = user;
		}

		/**
		 * Gets the run name.
		 *
		 * @return the run name
		 */
		public String getUser() {
			return user;
		}

                public String getName() {
                    return name;
                }

                
                
	}
	
	
	/** The id. */
	@Id
	private CustomSampleMetadataId id;

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private LinkedHashMap<String, Comparable> additionalInfo = null;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public CustomSampleMetadataId getId() {
		return id;
	}
	
	
	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Comparable> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new LinkedHashMap<String, Comparable>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(LinkedHashMap<String, Comparable> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	@Override
	public String toString() {
		return id.getName();
	}

}