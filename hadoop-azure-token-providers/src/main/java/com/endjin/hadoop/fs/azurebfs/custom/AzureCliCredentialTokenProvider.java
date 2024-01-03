package com.endjin.hadoop.fs.azurebfs.custom;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.AzureCliCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import org.apache.hadoop.conf.Configuration;

import java.time.OffsetDateTime;
import java.util.Date;

class AzureCliCredentialTokenProvider implements org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee {
    private volatile AccessToken token;

    @Override
    public void initialize(Configuration configuration, String accountName) {
    }

    @Override
    public String getAccessToken() {
        if (this.token != null && OffsetDateTime.now().isBefore(this.token.getExpiresAt().minusHours(2))) {
            return this.token.getToken();
        } else {
            setToken();
            return this.token.getToken();
        }
    }

    @Override
    public Date getExpiryTime() {
        return new Date(token.getExpiresAt().toInstant().toEpochMilli());
    }

    private void setToken() {
        AzureCliCredential creds = new AzureCliCredentialBuilder().build();
        TokenRequestContext request = new TokenRequestContext();
        request.addScopes("https://storage.azure.com/.default");
        this.token = creds.getToken(request).block();
    }
}