package com.satish.athena.executor;

import java.util.Properties;

/**
 *   Holds all the Athena Utility methods.
 */
public class AthenaUtils {

    public static Properties getPropertiesWithDefaultAWSCredentialsProviderChain() {
        Properties info = new Properties();
        // No need to supply any credential provider arguments because they are taken from one of the locations in the default credentials provider chain
        info.put("AwsCredentialsProviderClass", "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain");
        return info;
    }

    // PropertiesFileCredentialsProvider, which uses only one argument and obtains the required credentials from a file
    public static Properties getPropertiesWithPropertiesFileCredentialsProvider(String authParams) {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.PropertiesFileCredentialsProvider");
        // uses only one argument and obtains the required credentials from a file
        info.put("AwsCredentialsProviderArguments",authParams);
        return info;
    }

    // Using InstanceProfileCredentialsProvider, no need to supply any credential provider arguments because they are provided
    // using the EC2 instance profile for the instance on which you are running your application
    public static Properties getPropertiesWithInstanceProfileCredentialsProvider() {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider");
        return info;
    }

    // Using CustomSessionCredentialsProvider
    public static Properties getPropertiesWithCustomSessionCredentialsProvider(String awsAccessKey, String awsSecretKey, String sessionToken) {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.satish.athena.auth.CustomSessionCredentialsProvider");

        String providerArgs = awsAccessKey+"," + awsSecretKey +"," + sessionToken;
        info.put("AwsCredentialsProviderArguments", providerArgs);
        return info;
    }
    public static Properties getPropertiesWithCustomSessionCredentialsProvider(String authParams) {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.satish.athena.auth.CustomSessionCredentialsProvider");
        info.put("AwsCredentialsProviderArguments", authParams);
        return info;
    }

    public static Properties getPropertiesWithExternalAccountCredentialsProvider(String region, String roleArn, String endpointURL, String roleSessionName) {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.satish.athena.auth.ExternalAccountCredentialsProvider");

        String providerArgs = region+"," + roleArn +"," + endpointURL +"," + roleSessionName ;
        info.put("AwsCredentialsProviderArguments", providerArgs);
        return info;
    }
    public static Properties getPropertiesWithExternalAccountCredentialsProvider(String authParams) {
        Properties info = new Properties();
        info.put("AwsCredentialsProviderClass","com.satish.athena.auth.ExternalAccountCredentialsProvider");
        info.put("AwsCredentialsProviderArguments", authParams);
        return info;
    }
}
