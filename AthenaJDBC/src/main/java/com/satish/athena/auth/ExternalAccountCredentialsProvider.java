package com.satish.athena.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.simba.athena.amazonaws.auth.BasicSessionCredentials;
import com.simba.athena.amazonaws.client.builder.AwsClientBuilder;
import com.simba.athena.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.simba.athena.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClientBuilder;
import com.simba.athena.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.simba.athena.amazonaws.services.securitytoken.model.AssumeRoleResult;

public class ExternalAccountCredentialsProvider implements AWSCredentialsProvider {

    private BasicSessionCredentials m_credentials;

    public ExternalAccountCredentialsProvider(String region, String roleArn, String endpointURL, String roleSessionName) {
        System.out.println("Getting EndpointConfiguration");
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(endpointURL, region);

        System.out.println("Getting SecurityTokenService");
        AWSSecurityTokenService securityTokenService = AWSSecurityTokenServiceAsyncClientBuilder.standard().withEndpointConfiguration(endpointConfiguration).build();

        System.out.println("Getting AssumeRole Details");
        AssumeRoleResult assumeRoleResult = securityTokenService.assumeRole(new AssumeRoleRequest().withRoleArn(roleArn).withRoleSessionName(roleSessionName));

        System.out.println("Getting AccessKeyId, ecretAccessKey and SessionToken");
        String awsAccessKey = assumeRoleResult.getCredentials().getAccessKeyId();
        String awsSecretKey = assumeRoleResult.getCredentials().getSecretAccessKey();
        String sessionToken = assumeRoleResult.getCredentials().getSessionToken();

        m_credentials = new BasicSessionCredentials(awsAccessKey, awsSecretKey, sessionToken);
    }


    @Override
    public AWSCredentials getCredentials() {
        return m_credentials;
    }

    @Override
    public void refresh() {

    }
}
