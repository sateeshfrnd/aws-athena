package com.satish.athena.main;

import com.satish.athena.executor.AthenaQueryExecutior;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AthenaApp {

    public static void main(String args[]) {
		//final String PROP_FILE = args[0];
        final String PROP_FILE = "src/main/resources/athena.properties";

        String connectionURL = null;
        String driverClass = null;
        String athenaQueryResultBucket = null;
        String authType= null;
        String authParams = null;
        String athenaQuery = null;

        try (InputStream input = new FileInputStream(PROP_FILE)) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            connectionURL = prop.getProperty("aws.athena.connection.url");
            driverClass = prop.getProperty("aws.athena.driver");
            athenaQueryResultBucket = prop.getProperty("aws.athena.s3.result.bucket");
            authType = prop.getProperty("aws.credentials.provider.auth.type");
            authParams = prop.getProperty("aws.credentials.provider.auth.params");
            athenaQuery = prop.getProperty("aws.athena.query");

            // print it out
            System.out.println("connectionURL = "+connectionURL);
            System.out.println("driverClass = "+driverClass);
            System.out.println("athenaQueryResultBucket = "+athenaQueryResultBucket);
            System.out.println("authType = "+authType);
            System.out.println("authParams = "+authParams);
            System.out.println("athenaQuery = "+athenaQuery);


            AthenaQueryExecutior athenaQueryExecutior = new AthenaQueryExecutior(connectionURL, driverClass, athenaQueryResultBucket, authType, authParams, athenaQuery);
            athenaQueryExecutior.execute();


        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
