package fr.cirad.mgdb.model.mongo.maintypes;

import java.util.Date;

public class DatabaseInformation {
	public static final String FIELDNAME_LAST_MODIFICATION = "lastModification";
	public static final String FIELDNAME_IS_RESTORED = "isRestored";

	private Date lastModification = null;
	private boolean isRestored = false;
	
	public Date getLastModification() {
		return lastModification;
	}

	public void setLastModification(Date lastModification) {
		this.lastModification = lastModification;
	}
	
	public boolean isRestored() {
		return isRestored;
	}

	public void setRestored(boolean isRestored) {
		this.isRestored = isRestored;
	}
}
