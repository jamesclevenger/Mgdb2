package jhi.brapi.api.locations;

public class BrapiLocation {
    private String locationDbId;
    private String locationType;
    private String name;
    private String abbreviation;
    private String countryCode;
    private String countryName;
    private double latitude;
    private double longitude;
    private double altitude;
    private String instituteName;
    private String instituteAdress;
    private Object additionalInfo = null;

    public String getLocationDbId() {
        return this.locationDbId;
    }

    public void setLocationDbId(String locationDbId) {
        this.locationDbId = locationDbId;
    }

    public String getLocationType() {
        return this.locationType;
    }

    public void setLocationType(String locationType) {
        this.locationType = locationType;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAbbreviation() {
        return this.abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getCountryCode() {
        return this.countryCode;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public String getCountryName() {
        return this.countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getAltitude() {
        return this.altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public String getInstituteName() {
        return this.instituteName;
    }

    public void setInstituteName(String instituteName) {
        this.instituteName = instituteName;
    }

    public String getInstituteAdress() {
        return this.instituteAdress;
    }

    public void setInstituteAdress(String instituteAdress) {
        this.instituteAdress = instituteAdress;
    }

    public Object getAdditionalInfo() {
        return this.additionalInfo;
    }

    public void setAdditionalInfo(Object additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String toString() {
        return "BrapiLocation{locationDbId=" + this.locationDbId + ", name='" + this.name + '\'' + ", countryCode='" + this.countryCode + '\'' + ", countryName='" + this.countryName + '\'' + ", latitude=" + this.latitude + ", longitude=" + this.longitude + ", altitude=" + this.altitude + ", additionalInfo=" + this.additionalInfo + '}';
    }
}

