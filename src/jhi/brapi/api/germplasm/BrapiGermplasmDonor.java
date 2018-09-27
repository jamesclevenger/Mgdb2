package jhi.brapi.api.germplasm;

public class BrapiGermplasmDonor {
    private String donorInstituteCode;
    private String donorAccessionNumber;
    private String donorGermplasmPUI;

    public String getDonorInstituteCode() {
        return this.donorInstituteCode;
    }

    public void setDonorInstituteCode(String donorInstituteCode) {
        this.donorInstituteCode = donorInstituteCode;
    }

    public String getDonorAccessionNumber() {
        return this.donorAccessionNumber;
    }

    public void setDonorAccessionNumber(String donorAccessionNumber) {
        this.donorAccessionNumber = donorAccessionNumber;
    }

    public String getDonorGermplasmPUI() {
        return this.donorGermplasmPUI;
    }

    public void setDonorGermplasmPUI(String donorGermplasmPUI) {
        this.donorGermplasmPUI = donorGermplasmPUI;
    }

    public String toString() {
        return "BrapiGermplasmDonor{donorInstituteCode='" + this.donorInstituteCode + '\'' + ", donorAccessionNumber='" + this.donorAccessionNumber + '\'' + ", donorGermplasmPUI='" + this.donorGermplasmPUI + '\'' + '}';
    }
}

