package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.HashMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = "customIndividualMetadata")
@TypeAlias("M")
public class CustomIndividualMetadata {
	
	/** The Constant SECTION_ADDITIONAL_INFO. */
	public final static String SECTION_ADDITIONAL_INFO = "ai";
	
	static public class CustomIndividualMetadataId {
		
		/** The Constant FIELDNAME_INDIVIDUAL_ID. */
		public final static String FIELDNAME_INDIVIDUAL_ID = "ii";
		
		/** The Constant FIELDNAME_USER. */
		public final static String FIELDNAME_USER = "ur";

		/** The individual id. */
		@Field(FIELDNAME_INDIVIDUAL_ID)
		private String individualId;

		/** The user name. */
		@Field(FIELDNAME_USER)
		private String user;


		/**
		 * Instantiates a new custom individual metadata id.
		 *
		 * @param individual the individual id
		 * @param user the user's name
		 */
		public CustomIndividualMetadataId(String individualId, String user) {
			this.individualId = individualId;
			this.user = user;
		}

		/**
		 * Gets the individual id.
		 *
		 * @return the individual id
		 */
		public String getIndividualId() {
			return individualId;
		}

		/**
		 * Gets the run name.
		 *
		 * @return the run name
		 */
		public String getUser() {
			return user;
		}
	}
	
	
	/** The id. */
	@Id
	private CustomIndividualMetadataId id;

	/** The additional info. */
	@Field(SECTION_ADDITIONAL_INFO)
	private HashMap<String, Comparable> additionalInfo = null;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public CustomIndividualMetadataId getId() {
		return id;
	}
	
	
	/**
	 * Gets the additional info.
	 *
	 * @return the additional info
	 */
	public HashMap<String, Comparable> getAdditionalInfo() {
		if (additionalInfo == null)
			additionalInfo = new HashMap<String, Comparable>();
		return additionalInfo;
	}

	/**
	 * Sets the additional info.
	 *
	 * @param additionalInfo the additional info
	 */
	public void setAdditionalInfo(HashMap<String, Comparable> additionalInfo) {
		this.additionalInfo = additionalInfo;
	}
	
	@Override
	public String toString() {
		return id.getIndividualId();
	}

}