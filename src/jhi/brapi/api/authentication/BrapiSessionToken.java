package jhi.brapi.api.authentication;

import jhi.brapi.api.Metadata;

public class BrapiSessionToken {
    private Metadata metadata;
    private String access_token;
    private String userDisplayName;
    private int expires_in = Integer.MAX_VALUE;

    public String getAccess_token() {
        return this.access_token;
    }

    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }

    public Metadata getMetadata() {
        return this.metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public String getUserDisplayName() {
        return this.userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public int getExpires_in() {
        return this.expires_in;
    }

    public void setExpires_in(int expires_in) {
        this.expires_in = expires_in;
    }
}

